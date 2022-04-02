//! Errors

use std::fmt::Display;

use datafusion::error::DataFusionError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RdbmsError {
    /// There was an error with executing some query on a database
    DatabaseError(String),
    UnsupportedType(String),
}

impl std::error::Error for RdbmsError {}

impl Display for RdbmsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdbmsError::DatabaseError(err) => f.write_str(err),
            RdbmsError::UnsupportedType(err) => f.write_str(err),
        }
    }
}

impl From<RdbmsError> for DataFusionError {
    fn from(val: RdbmsError) -> Self {
        DataFusionError::External(Box::new(val))
    }
}

impl From<tokio_postgres::Error> for RdbmsError {
    fn from(err: tokio_postgres::Error) -> Self {
        RdbmsError::DatabaseError(format!("Postgres error: {:?}", err))
    }
}
