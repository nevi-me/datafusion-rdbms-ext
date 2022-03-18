//! Conversion between Postgres and Arrow datatypes

use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use tokio_postgres::types::*;
use tokio_postgres::Row;

/// Convert Postgres Type to Arrow DataType
///
/// Not all types are covered, but can be easily added
pub fn pg_to_arrow_type(dt: &Type) -> Option<DataType> {
    match dt {
        &Type::BOOL => Some(DataType::Boolean),
        &Type::BYTEA | &Type::CHAR | &Type::NAME | &Type::TEXT | &Type::VARCHAR => {
            Some(DataType::Utf8)
        }
        &Type::INT2 => Some(DataType::Int16),
        &Type::INT4 => Some(DataType::Int32),
        &Type::INT8 => Some(DataType::Int64),
        &Type::NUMERIC => Some(DataType::Float64),
        //        &OID => None,
        //        &JSON => None,
        &Type::FLOAT4 => Some(DataType::Float32),
        &Type::FLOAT8 => Some(DataType::Float64),
        //        &ABSTIME => None,
        //        &RELTIME => None,
        //        &TINTERVAL => None,
        //        &MONEY => None,
        &Type::BOOL_ARRAY => Some(DataType::List(Box::new(Field::new(
            "item",
            DataType::Boolean,
            true,
        )))),
        &Type::BYTEA_ARRAY | &Type::CHAR_ARRAY | &Type::NAME_ARRAY => Some(DataType::List(
            Box::new(Field::new("item", DataType::Utf8, true)),
        )),
        &Type::INT2_ARRAY => Some(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int16,
            true,
        )))),
        //        &INT2_VECTOR => None,
        //        &INT2_VECTOR_ARRAY => None,
        &Type::INT4_ARRAY => Some(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            true,
        )))),
        //        &TEXT_ARRAY => None,
        &Type::INT8_ARRAY => Some(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int64,
            true,
        )))),
        &Type::FLOAT4_ARRAY => Some(DataType::List(Box::new(Field::new(
            "item",
            DataType::Float32,
            true,
        )))),
        &Type::FLOAT8_ARRAY => Some(DataType::List(Box::new(Field::new(
            "item",
            DataType::Float64,
            true,
        )))),
        //        &ABSTIME_ARRAY => None,
        //        &RELTIME_ARRAY => None,
        //        &TINTERVAL_ARRAY => None,
        //        &DATE => None,
        &Type::TIME => Some(DataType::Time64(TimeUnit::Microsecond)),
        &Type::TIMESTAMP => Some(DataType::Timestamp(TimeUnit::Millisecond, None)),
        &Type::TIMESTAMP_ARRAY => Some(DataType::List(Box::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )))),
        //        &DATE_ARRAY => None,
        &Type::TIME_ARRAY => Some(DataType::List(Box::new(Field::new(
            "item",
            DataType::Time64(TimeUnit::Millisecond),
            true,
        )))),
        //        &TIMESTAMPTZ => None,
        //        &TIMESTAMPTZ_ARRAY => None,
        //        &INTERVAL => None,
        //        &INTERVAL_ARRAY => None,
        //        &NUMERIC_ARRAY => None,
        //        &TIMETZ => None,
        //        &BIT => None,
        //        &BIT_ARRAY => None,
        //        &VARBIT => None,
        //        &NUMERIC => None,
        //        &UUID => None,
        t @ _ => panic!("Postgres type {:?} not supported", t),
    }
}

/// Generate Arrow schema from a row
pub fn row_to_schema(row: &Row) -> Result<Schema, ()> {
    let fields = row
        .columns()
        .iter()
        .map(|col: &tokio_postgres::Column| {
            Field::new(col.name(), pg_to_arrow_type(col.type_()).unwrap(), true)
        })
        .collect();
    Ok(Schema::new(fields))
}

/// Convert rows from an information_schema query to a schema
pub fn info_schema_table_to_schema(rows: Vec<Row>) -> Result<Schema, ()> {
    let fields: Result<Vec<_>, ()> = rows
        .iter()
        .map(|row| PgDataType {
            column_name: row.get("column_name"),
            ordinal_position: row.get("ordinal_position"),
            is_nullable: row.get("is_nullable"),
            data_type: row.get("data_type"),
            char_max_length: row.get("character_maximum_length"),
            numeric_precision: row.get("numeric_precision"),
            datetime_precision: row.get("datetime_precision"),
        })
        .map(Field::try_from)
        .collect();
    Ok(Schema::new(fields?))
}

struct PgDataType {
    column_name: String,
    ordinal_position: i32,
    is_nullable: String,
    data_type: String,
    char_max_length: Option<i32>,
    numeric_precision: Option<i32>,
    datetime_precision: Option<i32>,
}

impl TryFrom<PgDataType> for Field {
    type Error = ();
    fn try_from(field: PgDataType) -> Result<Self, Self::Error> {
        let data_type = match field.data_type.as_str() {
            "int" | "integer" => match field.numeric_precision {
                Some(8) => Ok(DataType::Int8),
                Some(16) => Ok(DataType::Int16),
                Some(32) => Ok(DataType::Int32),
                Some(64) => Ok(DataType::Int64),
                _ => Err(()),
            },
            "bigint" => Ok(DataType::Int64),
            "\"char\"" | "character" => Ok(DataType::Utf8),
            // "anyarray" | "ARRAY" => Err(()),
            "boolean" => Ok(DataType::Boolean),
            "bytea" => Ok(DataType::Binary),
            "character varying" => Ok(DataType::Utf8),
            "date" => Ok(DataType::Date32),
            "double precision" => Ok(DataType::Float64),
            // "inet" => Err(()),
            "interval" => Ok(DataType::Interval(IntervalUnit::DayTime)), // TODO: use appropriate unit
            // "name" => Err(()),
            // This is a default that I have set, we can change to something saner
            // Ideally we should get the default precision from the DB if it is set
            "numeric" => Ok(DataType::Decimal(38, 10)),
            // "oid" => Err(()),
            "real" => Ok(DataType::Float32),
            "smallint" => Ok(DataType::Int16),
            "text" => Ok(DataType::Utf8),
            "time" | "time without time zone" => Ok(DataType::Time64(TimeUnit::Microsecond)), // TODO: use datetime_precision to determine correct type
            "timestamp with time zone" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
            "timestamp" | "timestamp without time zone" => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }
            "uuid" => Ok(DataType::Binary), // TODO: use a more specialised data type
            t => {
                eprintln!("Conversion not set for data type: {:?}", t);
                Err(())
            }
        };
        Ok(Field::new(
            &field.column_name,
            data_type?,
            &field.is_nullable == "YES",
        ))
    }
}
