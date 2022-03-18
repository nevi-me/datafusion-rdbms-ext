use std::{io::Write, sync::Arc};

use bytes::Buf;
use datafusion::arrow::array::*;
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{DataType, Schema, ToByteSlice};
use datafusion::arrow::record_batch::RecordBatch;
use futures_util::StreamExt;
use tokio_postgres::Client;
use tokio_postgres::CopyOutStream;

/// PGCOPY header
const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";
const EPOCH_DAYS: i32 = 10957;
const EPOCH_MICROS: i64 = 946684800000000;

pub(super) async fn get_binary_reader<'a>(
    client: &'a mut Client,
    query: &str,
) -> Result<CopyOutStream, ()> {
    dbg!(&query);
    Ok(client
        .copy_out(format!("COPY ({}) TO stdout with (format binary)", query).as_str())
        .await
        .unwrap())
}

pub(super) async fn read_from_binary(
    reader: CopyOutStream,
    schema: &Schema,
) -> Result<RecordBatch, ()> {
    futures_util::pin_mut!(reader);
    let mut buf = reader.next().await.unwrap().unwrap();
    // for values in reader.next().await.unwrap() {}
    // read signature
    let mut bytes = [0u8; 11];
    buf.copy_to_slice(&mut bytes);
    if bytes != MAGIC {
        eprintln!("Unexpected binary format type");
        // return Err(DataFrameError::IoError(
        //     "Unexpected Postgres binary type".to_string(),
        // ));
        return Err(());
    }
    // read flags
    let _size = buf.get_u32();

    // header extension area length
    let _size = buf.get_u32();

    // Start reading the rest of the data
    let field_len = schema.fields().len();

    let mut buffers: Vec<Vec<u8>> = vec![vec![]; field_len];
    let mut null_buffers: Vec<Vec<bool>> = vec![vec![]; field_len];
    let mut offset_buffers: Vec<Vec<i32>> = vec![vec![]; field_len];

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
            t => panic!("unsupported type {t}"),
        })
        .collect();

    let mut record_num = -1;

    while buf.has_remaining() {
        record_num += 1;
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
                    offset_buffers[i].push(record_num * binary_size);
                }
                DataType::List(_) => {}
                DataType::FixedSizeList(_, _) => {}
                DataType::Struct(_) => {}
                _ => {}
            }
            // populate values
            if col_length == -1 {
                // null value
                null_buffers[i].push(false);
                buffers[i].write_all(default_values[i].as_slice()).unwrap();
            } else {
                null_buffers[i].push(true);
                // big endian data, needs to be converted to little endian
                let mut data = read_col(&mut buf, schema.field(i).data_type(), col_length as usize)
                    .expect("Unable to read data");
                buffers[i].append(&mut data);
            }
        }

        // Get more data if the buffer is exhausted
        if !buf.has_remaining() {
            match reader.next().await {
                Some(Ok(more)) => {
                    buf = more;
                }
                Some(Err(e)) => {
                    eprintln!("{e}");
                    return Err(());
                }
                None => {
                    continue;
                }
            }
        }
    }

    let mut arrays = vec![];
    // build record batches
    buffers
        .into_iter()
        .zip(null_buffers.into_iter())
        .zip(schema.fields().iter())
        .enumerate()
        .for_each(|(i, ((b, n), f))| {
            let null_count = n.iter().filter(|v| v == &&false).count();
            let mut null_buffer = BooleanBufferBuilder::new(n.len() / 8 + 1);
            null_buffer.append_slice(&n[..]);
            let null_buffer = null_buffer.finish();
            match f.data_type() {
                DataType::Boolean => {
                    let bools = b.iter().map(|v| v == &0).collect::<Vec<bool>>();
                    let mut bool_buffer = BooleanBufferBuilder::new(bools.len());
                    bool_buffer.append_slice(&bools[..]);
                    let bool_buffer = bool_buffer.finish();
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
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
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                DataType::FixedSizeBinary(_) => {
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                DataType::Binary | DataType::Utf8 | DataType::LargeBinary | DataType::LargeUtf8 => {
                    // recontruct offsets
                    let mut offset = 0;
                    let mut offsets = vec![0];
                    offset_buffers[i].iter().for_each(|o| {
                        offsets.push(offset + o);
                        offset += o;
                    });
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(offsets.to_byte_slice()), Buffer::from(b)],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                DataType::List(_) | DataType::LargeList(_) => {
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                DataType::FixedSizeList(_, _) => {
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                DataType::Float16 => panic!("Float16 not yet implemented"),
                DataType::Struct(_) => panic!("Reading struct arrays not implemented"),
                DataType::Dictionary(_, _) => panic!("Reading dictionary arrays not implemented"),
                DataType::Union(_, _) => panic!("Union not supported"),
                DataType::Null => panic!("Null not supported"),
                DataType::Decimal(_, _) => {
                    let data = ArrayData::try_new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    )
                    .unwrap();
                    arrays.push(make_array(data))
                }
                DataType::Map(_, _) => panic!("Map not supported"),
            }
        });
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), arrays).unwrap())
}

fn read_col<R: Buf>(reader: &mut R, data_type: &DataType, length: usize) -> Result<Vec<u8>, ()> {
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
        DataType::Float16 => return Err(()),
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
        DataType::List(_) => return Err(()),
        DataType::FixedSizeList(_, _) => return Err(()),
        DataType::Struct(_) => return Err(()),
        DataType::Dictionary(_, _) => return Err(()),
        DataType::Union(_, _) => return Err(()),
        DataType::Null => return Err(()),
        DataType::LargeBinary => return Err(()),
        DataType::LargeUtf8 => return Err(()),
        DataType::LargeList(_) => return Err(()),
        DataType::Decimal(_, s) => read_decimal(reader, *s),
        _ => return Err(()),
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
        let (mut client, connection) = pg_conn.connect().await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres onnection error: {}", e);
            }
        });

        let query =
            "select * from bench.public.customer where c_acctbal < -90 order by c_acctbal limit 2";

        let reader = get_binary_reader(&mut client, query).await.unwrap();
        let batch = read_from_binary(reader, &schema).await.unwrap();
        dbg!(batch);
    }
}
