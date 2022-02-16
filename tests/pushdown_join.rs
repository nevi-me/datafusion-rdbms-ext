//! This tests that avarious joins can be detected and pushed
//! to the RDMBS source.

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    error::Result,
    execution::{context::ExecutionProps, runtime_env::{RuntimeEnv, RuntimeConfig}},
    logical_plan::{col, JoinType, LogicalPlanBuilder},
    optimizer::optimizer::OptimizerRule, physical_plan::common,
};
use datafusion_rdbms_ext::planner::*;

fn test_schema_a() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
    ])
}
fn test_schema_b() -> Schema {
    Schema::new(vec![
        Field::new("employee_id", DataType::Int32, false),
        Field::new("payment_date", DataType::Date32, false),
        Field::new("salary", DataType::Int32, false),
    ])
}

#[tokio::test]
async fn test_simple_join_pushdown() -> Result<()> {
    let ctx = make_rdbms_context();
    let plan_b = LogicalPlanBuilder::scan_empty(Some("b"), &test_schema_b(), None)?.build()?;
    let plan_a = LogicalPlanBuilder::scan_empty(Some("a"), &test_schema_a(), None)?
        .join(&plan_b, JoinType::Left, (vec!["id"], vec!["employee_id"]))?
        .project(vec![col("id"), col("first_name"), col("last_name")])?;

    let plan = plan_a.build()?;

    let join_opt = JoinOptimizerRule {};
    let exec_props = ExecutionProps::new();
    let optimized_plan = ctx.optimize(&plan)?;

    let physical_plan = ctx.create_physical_plan(&optimized_plan).await.unwrap();
    let result = physical_plan.execute(1, Arc::new(RuntimeEnv::new(RuntimeConfig::default()).unwrap())).await.unwrap();

    let batches = common::collect(result).await.unwrap();

    dbg!(batches);

    Ok(())
}
