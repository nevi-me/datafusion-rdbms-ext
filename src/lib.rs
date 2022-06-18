use std::sync::Arc;

use datafusion::{
    execution::{context::SessionState, runtime_env::*},
    prelude::*,
};
use optimizer::*;
use physical_plan::SqlDatabaseQueryPlanner;

mod catalog;
mod error;
pub(crate) mod node;
pub mod optimizer;
mod parser;
#[allow(dead_code, unused_variables)]
pub mod physical_plan;
pub mod sqldb;

/// Create a [SessionContext] with RDBMS planners and optimizers
pub fn make_rdbms_context() -> SessionContext {
    let config = SessionConfig::new().with_information_schema(true);
    let state = SessionState::with_config_rt(
        config,
        Arc::new(RuntimeEnv::new(RuntimeConfig::default()).unwrap()),
    )
    .with_query_planner(Arc::new(SqlDatabaseQueryPlanner {}))
    .add_optimizer_rule(Arc::new(QueryPushdownOptimizerRule {}));

    SessionContext::with_state(state)
}
