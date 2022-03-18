use std::{collections::HashMap, sync::Arc};

use datafusion::{
    arrow::{datatypes::Schema, error::Result as ArrowResult, record_batch::RecordBatch},
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    error::DataFusionError,
    physical_plan::SendableRecordBatchStream,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_postgres::{tls::NoTlsStream, Client, Connection, NoTls, Socket};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    catalog::{DatabaseCatalog, SchemaCatalog},
    physical_plan::DatabaseStream,
};

use self::{
    binary_reader::{get_binary_reader, read_from_binary},
    datatypes::info_schema_table_to_schema,
    table_provider::PostgresTableProvider,
};

use super::{ConnectionParameters, DatabaseConnection, DatabaseConnector, DatabaseType};

pub mod binary_reader;
mod datatypes;
pub mod table_provider;

#[derive(Clone)]
pub struct PostgresConnection {
    params: ConnectionParameters,
    database: String,
}

impl std::fmt::Debug for PostgresConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresConnection")
            .field("database", &self.database)
            .finish()
    }
}

impl PostgresConnection {
    // TODO do we need some validation of the connection here?
    pub fn new(params: ConnectionParameters, database: &str) -> Self {
        Self {
            params,
            database: database.to_string(),
        }
    }

    pub fn database_name(&self) -> &str {
        &self.database
    }

    pub fn to_connector(&self) -> DatabaseConnector {
        DatabaseConnector {
            db_type: DatabaseType::Postgres,
            params: self.params.clone(),
            db_name: self.database.clone(),
        }
    }

    async fn connect(&self) -> Result<(Client, Connection<Socket, NoTlsStream>), ()> {
        Ok(
            tokio_postgres::connect(&self.params.connection_string, NoTls)
                .await
                .unwrap(),
        )
    }

    pub async fn load_catalog(&self) -> Result<DatabaseCatalog, ()> {
        let catalog = DatabaseCatalog::new(&self.database.as_str());
        let (client, connection) = self.connect().await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres onnection error: {}", e);
            }
        });
        let metadata = client
            .query(
                "select table_schema, table_name
            from information_schema.tables
            where table_schema not in ('information_schema', 'pg_catalog')
                and table_type = 'BASE TABLE'
            order by table_schema, table_name",
                &[],
            )
            .await
            .unwrap();

        // Organise into a schema and list of tables
        let mut schemas = HashMap::new();
        for row in metadata {
            let list = schemas
                .entry(row.get::<_, &str>("table_schema").to_string())
                .or_insert(vec![]);
            list.push(row.get::<_, &str>("table_name").to_string());
        }
        // Get the schemas of each table
        for (schema, tables) in schemas {
            let schema_catalog = SchemaCatalog::new(&schema);
            for table in tables {
                let table_schema = client.query(
                    "select column_name, ordinal_position, is_nullable, data_type, character_maximum_length, numeric_precision, datetime_precision
                    from information_schema.columns where table_schema = $1 and table_name = $2",
                    &[&schema, &table]
                ).await.unwrap();
                let arrow_schema = info_schema_table_to_schema(table_schema)?;
                let provider = PostgresTableProvider::new(
                    &schema,
                    &table,
                    datafusion::datasource::TableType::Base,
                    arrow_schema.into(),
                    self.clone(),
                );
                schema_catalog
                    .register_table(table, Arc::new(provider))
                    .unwrap();
            }

            // Register schema in the database catalog
            catalog.register_schema(&schema, Arc::new(schema_catalog));
        }

        Ok(catalog)
    }
}

#[async_trait::async_trait]
impl DatabaseConnection for PostgresConnection {
    fn database_type(&self) -> DatabaseType {
        DatabaseType::Postgres
    }

    async fn fetch_query(
        &self,
        query: &str,
        schema: &Schema,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        // Connect to database
        let (mut client, connection) = self.connect().await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres onnection error: {}", e);
            }
        });

        let (response_tx, response_rx): (
            Sender<ArrowResult<RecordBatch>>,
            Receiver<ArrowResult<RecordBatch>>,
        ) = channel(2);

        let reader = get_binary_reader(&mut client, query).await.unwrap();
        let batches = read_from_binary(reader, schema).await.unwrap();

        tokio::task::spawn(async move {
            response_tx.send(Ok(batches)).await.expect("Unable to send"); // TODO handle this error
        })
        .await
        .expect("Unable to spawn task"); // TODO: handle this error

        Ok(Box::pin(DatabaseStream {
            schema: schema.clone().into(),
            inner: ReceiverStream::new(response_rx),
        }))
    }

    async fn fetch_table(
        &self,
        table_path: &str,
        _schema: &Schema,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let (client, connection) = self.connect().await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres onnection error: {}", e);
            }
        });

        let query = format!("select * from {}", table_path);

        let _result = client.query(&query, &[]).await.map_err(|_| ()).unwrap();
        Err(DataFusionError::NotImplemented(
            "Fetching table not yet implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::catalog::catalog::CatalogProvider;

    use super::*;

    /// Tests that the catalog can be loaded from a DB with TPC data
    #[tokio::test]
    async fn tpc_catalog_test() {
        let connection = PostgresConnection::new(
            ConnectionParameters {
                connection_string: "postgresql://postgres:password@localhost/bench".to_string(),
            },
            "bench",
        );
        let catalog = connection.load_catalog().await.unwrap();
        let schema_public = catalog.schema("public").unwrap();
        let table_part = schema_public.table("part").unwrap();
        dbg!(table_part.schema());
    }
}
