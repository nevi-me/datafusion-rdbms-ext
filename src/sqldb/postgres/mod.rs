use std::{collections::HashMap, sync::Arc};

use datafusion::{catalog::schema::SchemaProvider, physical_plan::SendableRecordBatchStream};
use tokio_postgres::{tls::NoTlsStream, Client, Connection, NoTls, Socket};

use crate::catalog::{DatabaseCatalog, SchemaCatalog};

use self::{datatypes::info_schema_table_to_schema, table_provider::PostgresTableProvider};

use super::{ConnectionParameters, DatabaseConnection, DatabaseType};

mod datatypes;
mod table_provider;

#[derive(Clone)]
pub struct PostgresConnection {
    params: ConnectionParameters,
    database: String,
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

    async fn connect(&self) -> Result<(Client, Connection<Socket, NoTlsStream>), ()> {
        Ok(
            tokio_postgres::connect(&self.params.connection_string, NoTls)
                .await
                .unwrap(),
        )
    }

    async fn load_catalog(&self) -> Result<DatabaseCatalog, ()> {
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
                    from information_schema.columns where table_schema = '$1' and table_name = '$2'",
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
            catalog.register_schema(schema, Arc::new(schema_catalog));
        }

        Ok(catalog)
    }
}

#[async_trait::async_trait]
impl DatabaseConnection for PostgresConnection {
    fn database_type(&self) -> DatabaseType {
        DatabaseType::Postgres
    }

    async fn fetch_query(&self, query: &str) -> Result<SendableRecordBatchStream, ()> {
        // Connect to database
        let (client, connection) = self.connect().await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres onnection error: {}", e);
            }
        });

        let _result = client.query(query, &[]).await.map_err(|_| ())?;
        Err(())
    }

    async fn fetch_table(&self, table_path: &str) -> Result<SendableRecordBatchStream, ()> {
        let (client, connection) = self.connect().await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres onnection error: {}", e);
            }
        });

        let query = format!("select * from {}", table_path);

        let _result = client.query(&query, &[]).await.map_err(|_| ())?;
        Err(())
    }
}
