use std::{collections::HashMap, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    error::DataFusionError,
    physical_plan::{stream::RecordBatchReceiverStream, SendableRecordBatchStream},
};
use tokio_postgres::{tls::NoTlsStream, Client, Connection, NoTls, Socket};

use crate::catalog::{DatabaseCatalog, SchemaCatalog};

use self::{
    binary_reader::read_from_query, datatypes::info_schema_table_to_schema,
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

    pub async fn connect(&self) -> Result<(Client, Connection<Socket, NoTlsStream>), ()> {
        Ok(
            tokio_postgres::connect(&self.params.connection_string, NoTls)
                .await
                .unwrap(),
        )
    }

    pub async fn load_catalog(&self) -> Result<DatabaseCatalog, ()> {
        let catalog = DatabaseCatalog::new(self.database.as_str());
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
            catalog
                .register_schema(&schema, Arc::new(schema_catalog))
                .unwrap();
        }

        Ok(catalog)
    }
}

#[async_trait::async_trait]
impl DatabaseConnection for PostgresConnection {
    fn database_type(&self) -> DatabaseType {
        DatabaseType::Postgres
    }

    fn fetch_query(
        &self,
        query: &str,
        schema: SchemaRef,
        // sender: Sender<ArrowResult<RecordBatch>>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        // Connect to database
        let connection = self.clone();
        let query = query.to_string();

        // Use spawn_blocking only if running from a tokio context (#2201)
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let (response_tx, response_rx) = tokio::sync::mpsc::channel(2);
                let schema_copy = schema.clone();
                let join_handle = handle.spawn(async move {
                    let (client, connection) = connection.connect().await.unwrap();
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("Postgres onnection error: {}", e);
                        }
                    });

                    read_from_query(&client, &query, schema_copy, response_tx)
                        .await
                        .unwrap();
                });
                Ok(RecordBatchReceiverStream::create(
                    &schema,
                    response_rx,
                    join_handle,
                ))
            }
            Err(_) => panic!("Not sure of what to do"),
        }
    }

    async fn count_records(&self, query: &str) -> Result<Option<usize>, DataFusionError> {
        let (client, connection) = self.connect().await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres onnection error: {}", e);
            }
        });

        let wrapped_query = format!("SELECT COUNT(*) as records FROM ({query}) a");

        let result = client.query_one(&wrapped_query, &[]).await;
        match result {
            Ok(row) => {
                let count = row.get::<_, i64>(0);
                let count: Option<usize> = count.try_into().ok();
                Ok(count)
            }
            _ => Ok(None),
        }
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
