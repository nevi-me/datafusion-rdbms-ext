//! An implementation of the DataFusion TableProvider for Postgres.
//! Supports filter and projection pushdown.

use std::sync::Arc;

use datafusion::{
    arrow::{datatypes::SchemaRef, error::Result as ArrowResult, record_batch::RecordBatch},
    datasource::{
        datasource::TableProviderFilterPushDown as FPD, datasource::TableSource, TableProvider,
        TableType,
    },
    error::{DataFusionError, Result as DfResult},
    execution::context::TaskContext,
    logical_plan::Expr,
    physical_plan::ExecutionPlan,
};
use futures_util::StreamExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    parser::{expr_to_sql, DatabaseDialect},
    physical_plan::DatabaseStream,
};

use crate::sqldb::DatabaseConnection;

use super::PostgresConnection;

/// A table or view that implements [TableProvider]
pub struct PostgresTableProvider {
    connection: PostgresConnection,
    schema_name: String,
    table_name: String,
    table_type: TableType,
    schema: SchemaRef,
}

impl PostgresTableProvider {
    pub fn new(
        schema_name: &str,
        table_name: &str,
        table_type: TableType,
        schema: SchemaRef,
        connection: PostgresConnection,
    ) -> Self {
        Self {
            connection,
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            table_type,
            schema,
        }
    }

    pub fn connection(&self) -> PostgresConnection {
        self.connection.clone()
    }
}

#[async_trait::async_trait]
impl TableProvider for PostgresTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        self.table_type
    }

    fn table_source(&self) -> TableSource {
        TableSource::Relational {
            server: None, // TODO should parse it from connection string
            database: Some(self.connection.database.to_string()),
            schema: Some(self.schema_name.clone()),
            table: self.table_name.clone(),
        }
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let dialect = DatabaseDialect::Generic;
        let (projection, schema) = match projection {
            Some(projection) => {
                let schema = &self.schema();
                let mut iter = projection.iter().map(|index| schema.field(*index).name());
                let out = iter.next().unwrap().clone();
                (
                    iter.fold(out, |a, b| a + ", " + b),
                    Arc::new(self.schema.project(projection)?),
                )
            }
            None => ("*".to_string(), self.schema()),
        };
        let table = self.table_name.as_str();
        let filter = if !filters.is_empty() {
            let mut sql_filters = filters.iter().map(|expr| expr_to_sql(expr, dialect));
            let mut out = String::from(" WHERE ");
            if let Some(s) = sql_filters.next() { out.push_str(&s) }
            sql_filters.fold(out, |a, b| a + " AND " + &b)
        } else {
            String::new()
        };
        let limit = match limit {
            Some(limit) => format!(" LIMIT {}", limit),
            None => String::new(),
        };
        let query = format!(
            "SELECT {projection} FROM {table}{filter}{limit}",
            projection = projection,
            table = table,
            filter = filter,
            limit = limit
        );

        Ok(Arc::new(PostgresExec {
            partitions: vec![QueryPartition {
                connection: self.connection.clone(),
                query,
            }],
            schema,
        }))
    }

    fn supports_filter_pushdown(&self, filter: &Expr) -> DfResult<FPD> {
        Ok(supports_filter_pushdown(filter))
    }
}

/// Postgres executor
#[derive(Debug, Clone)]
pub struct PostgresExec {
    partitions: Vec<QueryPartition>,
    /// Schema after projection is applied
    schema: SchemaRef,
}

#[derive(Debug, Clone)]
pub struct QueryPartition {
    connection: PostgresConnection,
    query: String,
}

#[async_trait::async_trait]
impl ExecutionPlan for PostgresExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn output_ordering(
        &self,
    ) -> Option<&[datafusion::physical_plan::expressions::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // No children as this is a root node
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<datafusion::physical_plan::SendableRecordBatchStream> {
        let (response_tx, response_rx): (
            Sender<ArrowResult<RecordBatch>>,
            Receiver<ArrowResult<RecordBatch>>,
        ) = channel(2);

        let partition = self.partitions.get(partition).unwrap();

        let connection = partition.connection.clone();
        let query = partition.query.clone();
        let schema = self.schema();

        tokio::task::spawn(async move {
            let mut response = connection.fetch_query(&query, &schema).await.unwrap();
            while let Some(value) = response.next().await {
                response_tx.send(value).await.unwrap();
            }
        });

        Ok(Box::pin(DatabaseStream {
            schema: self.schema(),
            inner: ReceiverStream::new(response_rx),
        }))
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        datafusion::physical_plan::Statistics::default()
    }
}

// TODO: add dialect
fn supports_filter_pushdown(filter: &Expr) -> FPD {
    match filter {
        Expr::Alias(expr, _) => supports_filter_pushdown(expr),
        Expr::Column(_) => FPD::Exact,
        Expr::ScalarVariable(_, _) => FPD::Unsupported,
        Expr::Literal(_) => FPD::Exact,
        Expr::BinaryExpr { .. } => FPD::Exact,
        Expr::Not(expr) => supports_filter_pushdown(expr),
        Expr::IsNotNull(expr) => supports_filter_pushdown(expr),
        Expr::IsNull(expr) => supports_filter_pushdown(expr),
        Expr::Negative(expr) => supports_filter_pushdown(expr),
        Expr::GetIndexedField { .. } => FPD::Unsupported,
        Expr::Between { expr, .. } => supports_filter_pushdown(expr),
        Expr::Case { .. } => FPD::Unsupported,
        Expr::Cast { expr, data_type } => {
            // If the expression can be pushed down, then we should be able to cast it
            let fpd = supports_filter_pushdown(expr);
            match &fpd {
                FPD::Unsupported => FPD::Unsupported,
                FPD::Inexact => FPD::Inexact,
                FPD::Exact => {
                    use datafusion::arrow::datatypes::DataType::*;
                    match data_type {
                        Boolean | Int8 | Int16 | Int32 | Int64 => fpd,
                        UInt8 => fpd,
                        Float32 | Float64 => fpd,
                        Timestamp(_, _) => fpd,
                        Date32 => fpd,
                        Time64(_) => fpd,
                        Binary => fpd,
                        LargeBinary => fpd,
                        Utf8 => fpd,
                        LargeUtf8 => fpd,
                        _ => FPD::Unsupported,
                        // Interval DayTime can be supported too
                    }
                }
            }
        }
        Expr::TryCast { .. } => FPD::Unsupported,
        Expr::Sort { .. } => FPD::Unsupported,
        Expr::ScalarFunction { .. } => FPD::Unsupported,
        Expr::ScalarUDF { .. } => FPD::Unsupported,
        Expr::AggregateFunction { .. } => FPD::Unsupported,
        Expr::WindowFunction { .. } => FPD::Unsupported,
        Expr::AggregateUDF { .. } => FPD::Unsupported,
        // TODO: I think I was still working on this
        Expr::InList { .. } => FPD::Exact,
        Expr::Wildcard => FPD::Unsupported,
        Expr::QualifiedWildcard { .. } => FPD::Unsupported,
    }
}
