//! Implement optimizer rules

use std::sync::Arc;

use datafusion::{
    datasource::datasource::TableSource,
    logical_plan::{
        plan::{Aggregate, Extension, Projection},
        LogicalPlan,
    },
    optimizer::{optimizer::OptimizerRule, utils::optimize_children},
};

use crate::node::{SqlJoinPlanNode, SqlProjectAggregateNode};

/// A rule that optimizes joins from the same RDBMS source.
/// Converts joins into a SQL query, so they can be computed at the source DB.
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
        if let LogicalPlan::Projection(project) = plan {
            if let LogicalPlan::Join(join) = &*project.input {
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
                            // dbg!(plan.schema().fields().len(), project.schema.fields().len(), left_input.schema().fields().len(), right_input.schema().fields().len());
                            return Ok(LogicalPlan::Extension(Extension {
                                node: Arc::new(SqlJoinPlanNode {
                                    left_input,
                                    right_input,
                                    join_type: join.join_type.clone(),
                                    on: join.on.clone(),
                                    schema: plan.schema().clone(),
                                }),
                            }));
                        }
                    }
                }
            }
        }

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

                        // NOTE there is currently an issue here with some projections.
                        // If a SQL query projects with a join, the upstream optimizer reinserts
                        // the columns that have been removed by the projection optimizer, causing
                        // the query to fail due to mismatched schemas.
                        // I've opened a vague issue about it so we can track it there.
                        // https://github.com/nevi-me/datafusion-rdbms-ext/issues/5
                        return Ok(LogicalPlan::Extension(Extension {
                            node: Arc::new(SqlJoinPlanNode {
                                left_input,
                                right_input,
                                join_type: join.join_type.clone(),
                                on: join.on.clone(),
                                schema: plan.schema().clone(),
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

/// A rule that optimizes a sequential Projection + Aggregate
pub struct ProjectionAggregateOptimizerRule {}

impl ProjectionAggregateOptimizerRule {
    fn simplify(
        &self,
        proj1: &Projection,
        proj2: &Projection,
        agg: &Aggregate,
    ) -> (Projection, Aggregate) {
        panic!()
    }
}

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
                    if let TableSource::Relational { .. } = scan.source.table_source() {
                        return Ok(LogicalPlan::Extension(Extension {
                            node: Arc::new(SqlProjectAggregateNode {
                                input: aggregate.input.as_ref().clone(),
                                group_expr: aggregate.group_expr.clone(),
                                aggr_expr: aggregate.aggr_expr.clone(),
                                proj_expr: projection.expr.clone(),
                                proj_alias: projection.alias.clone(),
                                schema: plan.schema().clone(),
                            }),
                        }));
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
                        if let TableSource::Relational { .. } = scan.source.table_source() {
                            // TODO: replace with an extension plan
                            // dbg!(&projection.expr, &inner_projection.expr, &inner_projection.alias);
                            return optimize_children(self, plan, execution_props);
                            return Ok(LogicalPlan::Extension(Extension {
                                node: Arc::new(SqlProjectAggregateNode {
                                    input: aggregate.input.as_ref().clone(),
                                    group_expr: aggregate.group_expr.clone(),
                                    aggr_expr: aggregate.aggr_expr.clone(),
                                    proj_expr: projection.expr.clone(),
                                    proj_alias: projection.alias.clone(),
                                    schema: plan.schema().clone(),
                                }),
                            }));
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
        // Look for Aggregate > Projection > TableScan (or supported extension)
        // Projection + Aggregate can sometimes be a (re)naming of an aggregate calculation
        // if let LogicalPlan::Filter(ref filter) = plan {
        //     if let LogicalPlan::CrossJoin(join) = &*filter.input {
        //         if let (LogicalPlan::TableScan(left_scan), LogicalPlan::TableScan(right_scan)) =
        //             (join.left.as_ref(), join.right.as_ref())
        //         {
        //             // Check if scans are from the same server, database and schema
        //             // Some relational stores allow joining across databases, this is not yet supported
        //             if let (
        //                 TableSource::Relational {
        //                     server: ref server_left,
        //                     database: Some(db_left),
        //                     schema: ref schema_left,
        //                     ..
        //                 },
        //                 TableSource::Relational {
        //                     server: ref server_right,
        //                     database: Some(db_right),
        //                     schema: ref schema_right,
        //                     ..
        //                 },
        //             ) = (
        //                 left_scan.source.table_source(),
        //                 right_scan.source.table_source(),
        //             ) {
        //                 if server_left.eq(&server_right)
        //                     && db_left.eq(&db_right)
        //                     && schema_left.eq(schema_right)
        //                 {
        //                     // check the expressions
        //                     dbg!(&filter.predicate);
        //                     let left_input = self.optimize(&join.left, execution_props)?;
        //                     let right_input = self.optimize(&join.right, execution_props)?;
        //                     return Ok(LogicalPlan::Extension(Extension {
        //                         node: Arc::new(SqlJoinPlanNode {
        //                             left_input,
        //                             right_input,
        //                             join_type: datafusion::logical_plan::JoinType::Inner,
        //                             on: vec![],
        //                             schema: join.schema.clone(),
        //                         }),
        //                     }));
        //                 }
        //             }
        //         }
        //     }
        // }

        // If we didn't find the Limit/Sort combination, recurse as
        // normal and build the result.
        optimize_children(self, plan, execution_props)
    }

    fn name(&self) -> &str {
        "sql_project_aggregate"
    }
}
