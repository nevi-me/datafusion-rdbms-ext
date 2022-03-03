use std::sync::Arc;

use datafusion::prelude::*;
use optimizer::JoinOptimizerRule;
use physical_plan::SqlDatabaseQueryPlanner;

mod catalog;
pub(crate) mod node;
pub mod optimizer;
mod parser;
#[allow(dead_code, unused_variables)]
pub mod physical_plan;
pub mod sqldb;

pub fn add_rdbms_to_context(config: ExecutionConfig) -> ExecutionConfig {
    config
        .with_query_planner(Arc::new(SqlDatabaseQueryPlanner {}))
        .add_optimizer_rule(Arc::new(JoinOptimizerRule {}))
}

pub fn make_rdbms_context() -> ExecutionContext {
    let config = ExecutionConfig::new()
        .with_query_planner(Arc::new(SqlDatabaseQueryPlanner {}))
        .add_optimizer_rule(Arc::new(JoinOptimizerRule {}));

    ExecutionContext::with_config(config)
}
