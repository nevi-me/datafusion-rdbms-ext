//! An implementation of the DataFusion TableProvider for Postgres.
//! Supports filter and projection pushdown.

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{
        datasource::TableProviderFilterPushDown as FPD, datasource::TableSource, TableProvider,
        TableType,
    },
    error::Result as DfResult,
    logical_plan::Expr,
    physical_plan::ExecutionPlan,
};

use crate::parser::{expr_to_sql, DatabaseDialect};

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
        let projection = match projection {
            Some(projection) => {
                let schema = &self.schema();
                let mut iter = projection.iter().map(|index| schema.field(*index).name());
                let out = iter.next().unwrap().clone();
                iter.fold(out, |a, b| a + ", " + b)
            }
            None => "*".to_string(),
        };
        let table = self.table_name.as_str();
        let filter = if !filters.is_empty() {
            let mut sql_filters = filters.iter().map(|expr| expr_to_sql(expr, dialect));
            let mut out = String::from(" WHERE ");
            sql_filters.next().map(|s| out.push_str(&s));
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
        panic!("{}", query)
    }

    fn supports_filter_pushdown(&self, filter: &Expr) -> DfResult<FPD> {
        Ok(supports_filter_pushdown(filter))
    }
}

// TODO: add dialect
fn supports_filter_pushdown(filter: &Expr) -> FPD {
    dbg!(filter);
    match filter {
        Expr::Alias(expr, _) => supports_filter_pushdown(expr),
        Expr::Column(_) => FPD::Unsupported,
        Expr::ScalarVariable(_) => FPD::Unsupported,
        Expr::Literal(_) => FPD::Exact,
        Expr::BinaryExpr { .. } => FPD::Inexact,
        Expr::Not(expr) => supports_filter_pushdown(expr),
        Expr::IsNotNull(expr) => supports_filter_pushdown(expr),
        Expr::IsNull(expr) => supports_filter_pushdown(expr),
        Expr::Negative(expr) => supports_filter_pushdown(expr),
        Expr::GetIndexedField { .. } => FPD::Unsupported,
        Expr::Between { .. } => FPD::Unsupported,
        Expr::Case { .. } => FPD::Unsupported,
        Expr::Cast { .. } => FPD::Unsupported,
        Expr::TryCast { .. } => FPD::Unsupported,
        Expr::Sort { .. } => FPD::Unsupported,
        Expr::ScalarFunction { .. } => FPD::Unsupported,
        Expr::ScalarUDF { .. } => FPD::Unsupported,
        Expr::AggregateFunction { .. } => FPD::Unsupported,
        Expr::WindowFunction { .. } => FPD::Unsupported,
        Expr::AggregateUDF { .. } => FPD::Unsupported,
        Expr::InList { .. } => FPD::Unsupported,
        Expr::Wildcard => FPD::Unsupported,
    }
}
