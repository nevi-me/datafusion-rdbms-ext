use datafusion::physical_plan::SendableRecordBatchStream;
use tokio_postgres::{tls::NoTlsStream, Client, Connection, NoTls, Socket};

use super::{ConnectionParameters, DatabaseConnection, DatabaseType};

#[derive(Clone)]
pub struct PostgresConnection {
    params: ConnectionParameters,
}

impl PostgresConnection {
    // TODO do we need some validation of the connection here?
    pub fn new(params: ConnectionParameters) -> Self {
        Self { params }
    }

    async fn connect(&self) -> Result<(Client, Connection<Socket, NoTlsStream>), ()> {
        Ok(
            tokio_postgres::connect(&self.params.connection_string, NoTls)
                .await
                .unwrap(),
        )
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
