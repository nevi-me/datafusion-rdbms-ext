//! Database reader

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::datasource::TableProviderFilterPushDown as FPD;
use datafusion::error::Result as DfResult;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, Statistics};
use datafusion::{arrow::datatypes::SchemaRef, datasource::TableProvider, logical_plan::Expr};

use crate::parser::{expr_to_sql, DatabaseDialect};

pub struct DatabaseProvider {
    // TODO: can add schema, database, etc.
    pub table: String,
    pub schema: SchemaRef,
    // TODO: add connection
    pub connection: (),
}

#[async_trait]
impl TableProvider for DatabaseProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
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
        let table = self.table.as_str();
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
            "select {projection} from {table}{filter}{limit}",
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
        // TODO: change back to unsupported
        Expr::WindowFunction { .. } => FPD::Exact,
        Expr::AggregateUDF { .. } => FPD::Unsupported,
        Expr::InList { .. } => FPD::Unsupported,
        Expr::Wildcard => FPD::Unsupported,
    }
}

#[derive(Debug)]
struct DatabaseExec {}

#[async_trait]
impl ExecutionPlan for DatabaseExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    async fn execute(
        &self,
        _partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> DfResult<SendableRecordBatchStream> {
        todo!()
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        prelude::ExecutionContext,
    };

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_simple_read() -> DfResult<()> {
        let fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
        ];
        let schema = Schema::new(fields).into();
        let reader = Arc::new(DatabaseProvider {
            table: "test_table".into(),
            schema,
            connection: (),
        });

        let mut context = ExecutionContext::new();
        context.register_table("test_table", reader)?;

        let mut df = context
            .sql(
                "select min(a) as min_a, b, c, sum(a) over (order by b) as a_running_sum
            from test_table 
            where a > 2 and b < 5 and (c + 3 = 2 or d is not null)
            group by b, c, a_running_sum
            order by c desc, max(b) asc
            limit 1000",
            )
            .await?;
        df = df.limit(100)?;
        df = df.limit(10)?;
        let plan = df.to_logical_plan();

        dbg!(plan);

        let _ = df.collect().await?;

        Ok(())
    }
}
