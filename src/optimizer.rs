//! Implement optimizer rules

use std::sync::Arc;

use datafusion::{
    datasource::datasource::TableSource,
    logical_plan::{plan::Extension, LogicalPlan},
    optimizer::{optimizer::OptimizerRule, utils::optimize_children},
};

use crate::node::SqlJoinPlanNode;

pub struct JoinOptimizerRule {}
impl OptimizerRule for JoinOptimizerRule {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &datafusion::execution::context::ExecutionProps,
    ) -> datafusion::error::Result<datafusion::logical_plan::LogicalPlan> {
        // Searches for a Join logical plan, and tries to merge the joins into a single
        // query if:
        // - the joins are from table scans from the same catalog and schema
        // - the other plans between the joins are supported by the source
        //
        // e.g. select a.*, b.col as b_col from a inner join b on a.col = b.col
        //
        // The expected plan for the above is a Join { TableScan, TableScan, InnerJoin }
        // and would be written as JoinedTableScan { a, b, InnerJoin }
        if let LogicalPlan::Join(join) = plan {
            if let (LogicalPlan::TableScan(left_scan), LogicalPlan::TableScan(right_scan)) =
                (join.left.as_ref(), join.right.as_ref())
            {
                // Check if scans are from the same server, database and schema
                // Some relational stores allow joining across databases, this is not yet supported
                if let (
                    TableSource::Relational {
                        server: ref server_left,
                        database: Some(db_left),
                        schema: ref schema_left,
                        ..
                    },
                    TableSource::Relational {
                        server: ref server_right,
                        database: Some(db_right),
                        schema: ref schema_right,
                        ..
                    },
                ) = (
                    left_scan.source.table_source(),
                    right_scan.source.table_source(),
                ) {
                    if server_left.eq(&server_right)
                        && db_left.eq(&db_right)
                        && schema_left.eq(schema_right)
                    {
                        let left_input = self.optimize(&join.left, execution_props)?;
                        let right_input = self.optimize(&join.right, execution_props)?;
                        dbg!(left_input.schema());
                        dbg!(right_input.schema());
                        return Ok(LogicalPlan::Extension(Extension {
                            node: Arc::new(SqlJoinPlanNode {
                                left_input,
                                right_input,
                                join_type: join.join_type.clone(),
                                on: join.on.clone(),
                                schema: join.schema.clone(),
                            }),
                        }));
                    }
                }
            }
        }

        // If we didn't find the Limit/Sort combination, recurse as
        // normal and build the result.
        optimize_children(self, plan, execution_props)
    }

    fn name(&self) -> &str {
        "sql_join"
    }
}
