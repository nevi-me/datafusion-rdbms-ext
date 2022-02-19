//! Database reader

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result as DfResult;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, Statistics};

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

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
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

    // #[tokio::test]
    // #[ignore]
    // async fn test_simple_read() -> DfResult<()> {
    //     let fields = vec![
    //         Field::new("a", DataType::Int32, true),
    //         Field::new("e", DataType::Int32, true),
    //         Field::new("b", DataType::Int32, true),
    //         Field::new("c", DataType::Int32, true),
    //         Field::new("d", DataType::Int32, true),
    //     ];
    //     let schema = Schema::new(fields).into();
    //     let reader = Arc::new(DatabaseProvider {
    //         table: "test_table".into(),
    //         schema,
    //         connection: (),
    //     });

    //     let mut context = ExecutionContext::new();
    //     context.register_table("test_table", reader)?;

    //     let mut df = context
    //         .sql(
    //             "select min(a) as min_a, b, c, sum(a) over (order by b) as a_running_sum
    //         from test_table
    //         where a > 2 and b < 5 and (c + 3 = 2 or d is not null)
    //         group by b, c, a_running_sum
    //         order by c desc, max(b) asc
    //         limit 1000",
    //         )
    //         .await?;
    //     df = df.limit(100)?;
    //     df = df.limit(10)?;
    //     let plan = df.to_logical_plan();

    //     dbg!(plan);

    //     let _ = df.collect().await?;

    //     Ok(())
    // }
}
