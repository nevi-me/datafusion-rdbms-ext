use std::sync::Arc;

use datafusion::prelude::*;
use optimizer::*;
use physical_plan::SqlDatabaseQueryPlanner;

mod catalog;
pub(crate) mod node;
pub mod optimizer;
mod parser;
#[allow(dead_code, unused_variables)]
pub mod physical_plan;
pub mod sqldb;

pub fn add_rdbms_to_context(config: SessionConfig) -> SessionConfig {
    config
        .with_query_planner(Arc::new(SqlDatabaseQueryPlanner {}))
        .add_optimizer_rule(Arc::new(JoinOptimizerRule {}))
        // TODO: I was working on these rules but had to first deal with some bugs
    // .add_optimizer_rule(Arc::new(ProjectionAggregateOptimizerRule {}))
    // .add_optimizer_rule(Arc::new(CrossJoinFilterRule {}))
}

pub fn make_rdbms_context() -> SessionContext {
    let config = SessionConfig::new()
        .with_query_planner(Arc::new(SqlDatabaseQueryPlanner {}))
        .add_optimizer_rule(Arc::new(JoinOptimizerRule {}))
        // .add_optimizer_rule(Arc::new(ProjectionAggregateOptimizerRule {}))
        // .add_optimizer_rule(Arc::new(CrossJoinFilterRule {}))
        ;

    SessionContext::with_config(config)
}
