use std::sync::Arc;

use bytes::Buf;
use datafusion::arrow::array::*;
use datafusion::arrow::buffer::{Buffer, MutableBuffer};
use datafusion::arrow::datatypes::{DataType, SchemaRef, ToByteSlice};
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc::Sender;
use tokio_postgres::Client;

use crate::error::RdbmsError;

/// PGCOPY header
const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";
const EPOCH_DAYS: i32 = 10957;
const EPOCH_MICROS: i64 = 946684800000000;

// TODO expose these via a struct with a read trait. This would be so we can expose
// the same interface for both a binary and row reader.
pub async fn read_from_query<'a>(
    client: &'a Client,
    query: &str,
    schema: SchemaRef,
    sender: Sender<ArrowResult<RecordBatch>>,
) -> Result<()> {
    let reader = client
        .copy_out(format!("copy ({}) to stdout with (format binary)", query).as_str())
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    futures_util::pin_mut!(reader);
    // TODO: move this to a configuration
    let batch_size = 65536;

    let mut buf = reader.next().await.unwrap().unwrap();
    // read signature
    let mut bytes = [0u8; 11];
    buf.copy_to_slice(&mut bytes);
    if bytes != MAGIC {
        // Return error from stream
        let t: Result<()> = Err(RdbmsError::DatabaseError(format!(
            "Unexpected Postgres binary header {:x?}",
            bytes
        ))
        .into());
        t?
    }
    // read flags
    let _size = buf.get_u32();

    // header extension area length
    let _size = buf.get_u32();

    // Start reading the rest of the data
    let field_len = schema.fields().len();

    let mut buffers: Vec<MutableBuffer> = (0..field_len)
        .map(|_| MutableBuffer::new(batch_size))
        .collect();
    let mut null_buffers: Vec<BooleanBufferBuilder> = (0..field_len)
        .map(|_| BooleanBufferBuilder::new(batch_size))
        .collect();
    let mut offset_buffers: Vec<Vec<i32>> = vec![Vec::with_capacity(batch_size); field_len];

    let default_values: Vec<Vec<u8>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Null => vec![],
            DataType::Boolean => vec![0],
            DataType::Int8 => vec![0],
            DataType::Int16 => vec![0; 2],
            DataType::Int32 => vec![0; 4],
            DataType::Int64 => vec![0; 8],
            DataType::UInt8 => vec![0],
            DataType::UInt16 => vec![0; 2],
            DataType::UInt32 => vec![0; 4],
            DataType::UInt64 => vec![0; 8],
            DataType::Float16 => vec![0; 2],
            DataType::Float32 => vec![0; 4],
            DataType::Float64 => vec![0; 8],
            DataType::Timestamp(_, _) => vec![0; 8],
            DataType::Date32 => vec![0; 4],
            DataType::Date64 => vec![0; 8],
            DataType::Time32(_) => vec![0; 4],
            DataType::Time64(_) => vec![0; 8],
            DataType::Duration(_) => vec![0; 8],
            DataType::Interval(_) => vec![0; 8],
            DataType::Binary => vec![],
            DataType::FixedSizeBinary(len) => vec![0; *len as usize],
            DataType::Utf8 => vec![],
            DataType::List(_) => vec![],
            DataType::FixedSizeList(_, len) => vec![0; *len as usize],
            DataType::Struct(_) => vec![],
            DataType::Union(_, _) => vec![],
            DataType::Dictionary(_, _) => vec![],
            DataType::LargeBinary => vec![],
            DataType::LargeUtf8 => vec![],
            DataType::LargeList(_) => vec![],
            DataType::Decimal(_, _) => vec![0; 16],
            t => panic!("Unsupported type {:?}", t),
        })
        .collect();

    let mut record_num = 0;
    while buf.has_remaining() {
        // tuple length
        let size = buf.get_i16();
        if size == -1 {
            // trailer
            continue;
        }
        let size = size as usize;
        // in almost all cases, the tuple length should equal schema field length
        assert_eq!(size, field_len);

        for i in 0..field_len {
            let col_length = buf.get_i32();
            // populate offsets for types that need them
            match schema.field(i).data_type() {
                DataType::Binary | DataType::Utf8 => {
                    offset_buffers[i].push(col_length);
                }
                DataType::FixedSizeBinary(binary_size) => {
                    offset_buffers[i].push((record_num as i32 + 1) * binary_size);
                }
                DataType::List(_) => {}
                DataType::FixedSizeList(_, _) => {}
                DataType::Struct(_) => {}
                _ => {}
            }
            // populate values
            if col_length == -1 {
                // null value
                null_buffers[i].append(false);
                buffers[i].extend_from_slice(default_values[i].as_slice());
            } else {
                null_buffers[i].append(true);
                // big endian data, needs to be converted to little endian
                let data = read_col(&mut buf, schema.field(i).data_type(), col_length as usize)
                    .expect("Unable to read data");
                buffers[i].extend_from_slice(&data);
            }
        }

        // Increment record count
        record_num += 1;

        // Check if to return batch
        if record_num == batch_size {
            let num_records = record_num as usize;
            let batch = complete_batch(
                buffers.drain(0..),
                &mut null_buffers,
                &mut offset_buffers,
                num_records,
                schema.clone(),
            )?;
            offset_buffers.iter_mut().for_each(|v| v.clear());
            // null_buffers.iter_mut().map(|b| b.finish());
            // Allocate new buffers
            // TODO: we drop the MutableBuffers, is there a better way to do this?
            buffers = (0..field_len)
                .map(|_| MutableBuffer::new(batch_size))
                .collect();
            // println!("Yielding completed batch with {record_num} records");
            // Reset records
            record_num = 0;
            sender.send(Ok(batch)).await.unwrap();
        }

        // Get more data if the buffer is exhausted
        if !buf.has_remaining() {
            match reader.next().await {
                Some(Ok(more)) => {
                    buf = more;
                }
                Some(Err(e)) => {
                    let t: Result<()> =
                        Err(RdbmsError::DatabaseError(format!("Buffer error: {:?}", e)).into());
                    t?
                }
                None => {
                    continue;
                }
            }
        }
    }

    // Last batch
    let num_records = record_num as usize;
    if num_records > 0 {
        // println!("Yielding incomplete batch with {num_records} records");
        let batch = complete_batch(
            buffers.drain(0..),
            &mut null_buffers,
            &mut offset_buffers,
            num_records,
            schema.clone(),
        )?;
        sender.send(Ok(batch)).await.unwrap();
    }

    Ok(())
}

/// Convert allocated data to [RecordBatch], then return empty vecs
fn complete_batch(
    buffers: impl Iterator<Item = MutableBuffer>,
    null_buffers: &mut Vec<BooleanBufferBuilder>,
    offset_buffers: &mut Vec<Vec<i32>>,
    num_records: usize,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut arrays = Vec::with_capacity(null_buffers.len());
    // build record batches
    buffers
        .zip(null_buffers.iter_mut())
        .zip(schema.fields().iter())
        .enumerate()
        .for_each(|(i, ((b, n), f))| {
            // let null_count = n.iter().filter(|v| v == &&false).count();
            let null_buffer = n.finish();
            match f.data_type() {
                DataType::Boolean => {
                    // TODO: verify that this is correct, or if we can create bool array directly from MutableBuffer
                    let bools = b.iter().map(|v| v == &0).collect::<Vec<bool>>();
                    let mut bool_buffer = BooleanBufferBuilder::new(bools.len());
                    bool_buffer.append_slice(&bools[..]);
                    let bool_buffer = bool_buffer.finish();
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        num_records,
                        None,
                        Some(null_buffer),
                        0,
                        vec![bool_buffer],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(Arc::new(BooleanArray::from(data)) as ArrayRef)
                }
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_) => {
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        num_records,
                        None,
                        Some(null_buffer),
                        0,
                        vec![b.into()],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                DataType::FixedSizeBinary(_) => {
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        num_records,
                        None,
                        Some(null_buffer),
                        0,
                        vec![b.into()],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                DataType::Binary | DataType::Utf8 => {
                    // recontruct offsets
                    let mut offset = 0;
                    let mut offsets = vec![0];
                    offset_buffers[i].iter().for_each(|o| {
                        offsets.push(offset + o);
                        offset += o;
                    });
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        num_records,
                        None,
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(offsets.to_byte_slice()), b.into()],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                DataType::LargeBinary | DataType::LargeUtf8 => {
                    todo!()
                }
                DataType::List(_) | DataType::LargeList(_) => {
                    // TODO: this is incorrect, no offsets included
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        num_records,
                        None,
                        Some(null_buffer),
                        0,
                        vec![b.into()],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                DataType::FixedSizeList(_, _) => {
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        num_records,
                        None,
                        Some(null_buffer),
                        0,
                        vec![b.into()],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                DataType::Decimal(_, _) => {
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        num_records,
                        None,
                        Some(null_buffer),
                        0,
                        vec![b.into()],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                t => unreachable!("Encountered type {:?} which is not supported", t),
            }
        });
    Ok(RecordBatch::try_new(schema.clone(), arrays).unwrap())
}

fn read_col<R: Buf>(reader: &mut R, data_type: &DataType, length: usize) -> Result<Vec<u8>> {
    Ok(match data_type {
        DataType::Boolean => read_bool(reader),
        DataType::Int8 => read_i8(reader),
        DataType::Int16 => read_i16(reader),
        DataType::Int32 => read_i32(reader),
        DataType::Int64 => read_i64(reader),
        DataType::UInt8 => read_u8(reader),
        DataType::UInt16 => read_u16(reader),
        DataType::UInt32 => read_u32(reader),
        DataType::UInt64 => read_u64(reader),
        DataType::Float32 => read_f32(reader),
        DataType::Float64 => read_f64(reader),
        DataType::Timestamp(_, _) => read_timestamp64(reader),
        DataType::Date32 => read_date32(reader),
        DataType::Date64 => unreachable!(),
        DataType::Time32(_) => read_i32(reader),
        DataType::Time64(_) => read_time64(reader),
        DataType::Duration(_) => read_i64(reader),
        DataType::Interval(_) => read_i64(reader),
        DataType::Binary => read_string(reader, length), // TODO we'd need the length of the binary
        DataType::FixedSizeBinary(_) => read_string(reader, length),
        DataType::Utf8 => read_string(reader, length),
        DataType::Decimal(_, s) => read_decimal(reader, *s),
        t => {
            return Err(
                RdbmsError::UnsupportedType(format!("The type {:?} is unsupported", t)).into(),
            )
        }
    })
}

fn read_u8<R: Buf>(reader: &mut R) -> Vec<u8> {
    reader.get_u8().to_le_bytes().to_vec()
}

fn read_i8<R: Buf>(reader: &mut R) -> Vec<u8> {
    reader.get_i8().to_le_bytes().to_vec()
}

fn read_u16<R: Buf>(reader: &mut R) -> Vec<u8> {
    reader.get_u16().to_le_bytes().to_vec()
}
fn read_i16<R: Buf>(reader: &mut R) -> Vec<u8> {
    reader.get_i16().to_le_bytes().to_vec()
}
fn read_u32<R: Buf>(reader: &mut R) -> Vec<u8> {
    reader.get_u32().to_le_bytes().to_vec()
}
fn read_i32<R: Buf>(reader: &mut R) -> Vec<u8> {
    reader.get_i32().to_le_bytes().to_vec()
}
fn read_u64<R: Buf>(reader: &mut R) -> Vec<u8> {
    reader.get_u64().to_le_bytes().to_vec()
}
fn read_i64<R: Buf>(reader: &mut R) -> Vec<u8> {
    reader.get_i64().to_le_bytes().to_vec()
}
fn read_bool<R: Buf>(reader: &mut R) -> Vec<u8> {
    reader.get_u8().to_le_bytes().to_vec()
}
fn read_string<R: Buf>(reader: &mut R, len: usize) -> Vec<u8> {
    let mut buf = vec![0; len];
    reader.copy_to_slice(&mut buf);
    buf
}
fn read_f32<R: Buf>(reader: &mut R) -> Vec<u8> {
    reader.get_f32().to_le_bytes().to_vec()
}
fn read_f64<R: Buf>(reader: &mut R) -> Vec<u8> {
    reader.get_f64().to_le_bytes().to_vec()
}

/// Postgres dates are days since epoch of 01-01-2000, so we add 10957 days
fn read_date32<R: Buf>(reader: &mut R) -> Vec<u8> {
    { EPOCH_DAYS + reader.get_i32() }.to_le_bytes().to_vec()
}

fn read_timestamp64<R: Buf>(reader: &mut R) -> Vec<u8> {
    { EPOCH_MICROS + reader.get_i64() }.to_le_bytes().to_vec()
}

/// we do not support time with time zone as it is 48-bit,
/// time without a zone is 32-bit but arrow only supports 64-bit at microsecond resolution
fn read_time64<R: Buf>(reader: &mut R) -> Vec<u8> {
    { reader.get_i32() as i64 }.to_le_bytes().to_vec()
}

fn read_decimal<R: Buf>(reader: &mut R, target_scale: usize) -> Vec<u8> {
    let num_groups = reader.get_u16();
    let weight = reader.get_i16();

    // TODO handle NaN and +/-Inf
    let sign = reader.get_u16();
    let _scale = reader.get_u16();
    let digits = (0..num_groups)
        .map(|_| reader.get_u16())
        .collect::<Vec<_>>();

    // If there are no digits, return 0
    if num_groups == 0 {
        return vec![0; 16];
    }

    if num_groups == 1 && weight == 1 {
        return (digits[0] as i128 * 10i128.pow(4 + target_scale as u32))
            .to_le_bytes()
            .to_vec();
    }

    let integer_count = (weight + 1) as usize;
    let mut result = 0i128;

    // Integers
    let integers = &digits[0..integer_count];
    integers.iter().enumerate().for_each(|(i, integer)| {
        let mul_scale = (integer_count - i - 1) * 4 + target_scale;
        result += (*integer as i128) * 10i128.pow(mul_scale as u32);
    });
    // Fractions
    let fractions = &digits[integer_count..];
    fractions.iter().enumerate().for_each(|(i, fraction)| {
        let mul_scale: i32 = target_scale as i32 - (4 * (i + 1) as i32);
        if mul_scale >= 0 {
            result += (*fraction as i128) * 10i128.pow(mul_scale as u32);
        } else {
            result += (*fraction as i128) / 10i128.pow(-mul_scale as u32);
        }
    });

    // Sign
    if sign == 0x4000 {
        result *= -1;
    }

    result.to_le_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::util::pretty;
    use datafusion::catalog::catalog::CatalogProvider;

    use crate::sqldb::{postgres::PostgresConnection, ConnectionParameters};

    #[tokio::test]
    async fn test_binary_read() {
        let pg_conn = PostgresConnection::new(
            ConnectionParameters::new("postgresql://postgres:password@localhost/bench"),
            "bench",
        );
        let catalog = pg_conn.load_catalog().await.unwrap();
        let table_provider = catalog.schema("public").unwrap().table("customer").unwrap();
        let schema = table_provider.schema();
        let (client, connection) = pg_conn.connect().await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres onnection error: {}", e);
            }
        });

        let query = "select * from bench.public.customer limit 2049 offset 10";

        // let strm = read_from_query(&client, query, reader, schema).await;
        // let batches = strm
        //     .filter_map(|b| async move { b.ok() })
        //     .collect::<Vec<_>>()
        //     .await;
        // pretty::print_batches(&batches).unwrap();
    }
}
