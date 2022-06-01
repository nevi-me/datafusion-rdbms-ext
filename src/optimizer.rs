//! Implement optimizer rules

use std::sync::Arc;

use datafusion::{
    datasource::datasource::Source,
    logical_plan::{plan::Extension, LogicalPlan},
    optimizer::{optimizer::OptimizerRule, utils::optimize_children},
};

use crate::node::{SqlAstPlanNode};
/// A rule that optimizes a sequential Projection + Aggregate
pub struct ProjectionAggregateOptimizerRule {}

impl OptimizerRule for ProjectionAggregateOptimizerRule {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &datafusion::execution::context::ExecutionProps,
    ) -> datafusion::error::Result<LogicalPlan> {
        // Look for Projection > Aggregate > TableScan (or supported extension)
        // Projection + Aggregate can sometimes be a (re)naming of an aggregate calculation
        if let LogicalPlan::Projection(ref projection) = plan {
            if let LogicalPlan::Aggregate(aggregate) = &*projection.input {
                if let LogicalPlan::TableScan(scan) = &*aggregate.input {
                    // Check that the table scan is a RDBMS scan
                    if let Source::Relational { .. } = scan.source.source() {
                        // Try to create an AST node
                        let ast_node = SqlAstPlanNode::try_from_plan(plan);
                        match ast_node {
                            Ok(node) => {
                                return Ok(LogicalPlan::Extension(Extension {
                                    node: Arc::new(node),
                                }))
                            }
                            Err(e) => {
                                // write warning
                                eprintln!("Error converting to AST node: {e}");
                                return optimize_children(self, plan, execution_props);
                            }
                        }
                    }
                }
            }
        }

        // Look for Projection > Aggregate > Projection > TableScan
        // This can apply in the where there is an aggregate of a computed column
        // The inner projection can create columns with long names, which are then renamed
        // by the outer projection. It is useful to identify this, and write the inner projection
        // more consicely.
        if let LogicalPlan::Projection(ref projection) = plan {
            if let LogicalPlan::Aggregate(aggregate) = &*projection.input {
                if let LogicalPlan::Projection(ref inner_projection) = &*aggregate.input {
                    if let LogicalPlan::TableScan(scan) = &*inner_projection.input {
                        // Check that the table scan is a RDBMS scan
                        if let Source::Relational { .. } = scan.source.source() {
                            // Try to create an AST node
                            let ast_node = SqlAstPlanNode::try_from_plan(plan);
                            match ast_node {
                                Ok(node) => {
                                    return Ok(LogicalPlan::Extension(Extension {
                                        node: Arc::new(node),
                                    }))
                                }
                                Err(e) => {
                                    // write warning
                                    eprintln!("Error converting to AST node: {e}");
                                    return optimize_children(self, plan, execution_props);
                                }
                            }
                        }
                    }
                }
            }
        }

        // If we didn't find the Limit/Sort combination, recurse as
        // normal and build the result.
        optimize_children(self, plan, execution_props)
    }

    fn name(&self) -> &str {
        "sql_project_aggregate"
    }
}

/// A rule to rewrtie a cross-join + filter if that crossjoin can be rewritten
/// as an inner join
pub struct CrossJoinFilterRule {}
impl OptimizerRule for CrossJoinFilterRule {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &datafusion::execution::context::ExecutionProps,
    ) -> datafusion::error::Result<LogicalPlan> {

        // If we didn't find the Limit/Sort combination, recurse as
        // normal and build the result.
        optimize_children(self, plan, execution_props)
    }

    fn name(&self) -> &str {
        "sql_project_aggregate"
    }
}

/// A general pushdown optimizer that converts as much of the logical plan
/// to an AST that is pushed to the database.
pub struct QueryPushdownOptimizerRule {}

impl OptimizerRule for QueryPushdownOptimizerRule {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &datafusion::execution::context::ExecutionProps,
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
                optimize_children(self, plan, execution_props)
            }
        }
    }

    fn name(&self) -> &str {
        "sql_query_pushdown"
    }
}
