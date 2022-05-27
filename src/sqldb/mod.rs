use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef, error::DataFusionError, physical_plan::SendableRecordBatchStream,
};

use self::postgres::PostgresConnection;

pub mod postgres;

#[derive(Debug, Clone, Copy)]
pub enum DatabaseType {
    Postgres,
    MySql,
    MsSql,
}

#[derive(Clone, Debug)]
// TODO: implement a custom Debug that doesn't leak credentials
pub struct ConnectionParameters {
    /// Let's say that we only take a connection string for now
    connection_string: String,
}

impl ConnectionParameters {
    pub fn new(connection_string: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DatabaseConnector {
    db_type: DatabaseType,
    params: ConnectionParameters,
    db_name: String,
}

impl DatabaseConnector {
    pub fn into_connection(&self) -> Arc<impl DatabaseConnection> {
        match self.db_type {
            DatabaseType::Postgres => Arc::new(PostgresConnection::new(
                self.params.clone(),
                self.db_name.as_str(),
            )),
            DatabaseType::MySql => todo!(),
            DatabaseType::MsSql => todo!(),
        }
    }
}

#[async_trait::async_trait]
pub trait DatabaseConnection: Clone {
    fn fetch_query(
        &self,
        query: &str,
        schema: SchemaRef,
        // sender: Sender<ArrowResult<RecordBatch>>,
    ) -> Result<SendableRecordBatchStream, DataFusionError>;
    // fn fetch_table(
    //     &self,
    //     table_path: &str,
    //     schema: SchemaRef,
    // ) -> Result<SendableRecordBatchStream, DataFusionError>;

    fn database_type(&self) -> DatabaseType;

    /// Count the number of records to be returned, for partition purposes
    async fn count_records(&self, query: &str) -> Result<Option<usize>, DataFusionError>;
}
