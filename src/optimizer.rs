//! Implement optimizer rules

use std::sync::Arc;

use datafusion::{
    logical_plan::{plan::Extension, LogicalPlan},
    optimizer::{optimizer::OptimizerRule, utils::optimize_children, OptimizerConfig},
};

use crate::node::SqlAstPlanNode;

/// A general pushdown optimizer that converts as much of the logical plan
/// to an AST that is pushed to the database.
pub struct QueryPushdownOptimizerRule {}

impl OptimizerRule for QueryPushdownOptimizerRule {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> datafusion::common::Result<LogicalPlan> {
        // Try to create an AST node
        let ast_node = SqlAstPlanNode::try_from_plan(plan);
        match ast_node {
            Ok(node) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            })),
            Err(e) => {
                // write warning
                eprintln!("Error converting to AST node: {e}");
                optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "sql_query_pushdown"
    }
}
