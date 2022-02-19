use datafusion::physical_plan::expressions::PhysicalSortExpr;
use fmt::Debug;
use std::task::{Context, Poll};
use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    arrow::{
        datatypes::SchemaRef,
        error::{ArrowError, Result as ArrowResult},
        record_batch::RecordBatch,
    },
    datasource::datasource::TableSource,
    execution::runtime_env::RuntimeEnv,
    logical_plan::{Column, JoinType},
};
use datafusion::{
    error::{DataFusionError, Result},
    execution::context::ExecutionContextState,
    execution::context::QueryPlanner,
    logical_plan::{Expr, LogicalPlan, UserDefinedLogicalNode},
    optimizer::{optimizer::OptimizerRule, utils::optimize_children},
    physical_plan::{
        planner::{DefaultPhysicalPlanner, ExtensionPlanner},
        DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalPlanner,
        RecordBatchStream, SendableRecordBatchStream, Statistics,
    },
    prelude::{ExecutionConfig, ExecutionContext},
};
use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

use async_trait::async_trait;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan::plan::Extension;
use datafusion::logical_plan::DFSchemaRef;

use crate::sqldb::{DatabaseConnection, DatabaseConnector};

// /// Execute the specified sql and return the resulting record batches
// /// pretty printed as a String.
// async fn exec_sql(ctx: &mut ExecutionContext, sql: &str) -> Result<String> {
//     let df = ctx.sql(sql).await?;
//     let batches = df.collect().await?;
//     pretty_format_batches(&batches).map_err(DataFusionError::ArrowError)
// }

// /// Create a test table.
// async fn setup_table(mut ctx: ExecutionContext) -> Result<ExecutionContext> {
//     let sql = "CREATE EXTERNAL TABLE sales(customer_id VARCHAR, revenue BIGINT) STORED AS CSV location 'tests/customer.csv'";

//     let expected = vec!["++", "++"];

//     let s = exec_sql(&mut ctx, sql).await?;
//     let actual = s.lines().collect::<Vec<_>>();

//     assert_eq!(expected, actual, "Creating table");
//     Ok(ctx)
// }

// async fn setup_table_without_schemas(mut ctx: ExecutionContext) -> Result<ExecutionContext> {
//     let sql = "CREATE EXTERNAL TABLE sales STORED AS CSV location 'tests/customer.csv'";

//     let expected = vec!["++", "++"];

//     let s = exec_sql(&mut ctx, sql).await?;
//     let actual = s.lines().collect::<Vec<_>>();

//     assert_eq!(expected, actual, "Creating table");
//     Ok(ctx)
// }

// const QUERY1: &str = "SELECT * FROM sales limit 3";

// const QUERY: &str = "SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3";

// // Run the query using the specified execution context and compare it
// // to the known result
// async fn run_and_compare_query(mut ctx: ExecutionContext, description: &str) -> Result<()> {
//     let expected = vec![
//         "+-------------+---------+",
//         "| customer_id | revenue |",
//         "+-------------+---------+",
//         "| paul        | 300     |",
//         "| jorge       | 200     |",
//         "| andy        | 150     |",
//         "+-------------+---------+",
//     ];

//     let s = exec_sql(&mut ctx, QUERY).await?;
//     let actual = s.lines().collect::<Vec<_>>();

//     assert_eq!(
//         expected,
//         actual,
//         "output mismatch for {}. Expectedn\n{}Actual:\n{}",
//         description,
//         expected.join("\n"),
//         s
//     );
//     Ok(())
// }

// // Run the query using the specified execution context and compare it
// // to the known result
// async fn run_and_compare_query_with_auto_schemas(
//     mut ctx: ExecutionContext,
//     description: &str,
// ) -> Result<()> {
//     let expected = vec![
//         "+----------+----------+",
//         "| column_1 | column_2 |",
//         "+----------+----------+",
//         "| andrew   | 100      |",
//         "| jorge    | 200      |",
//         "| andy     | 150      |",
//         "+----------+----------+",
//     ];

//     let s = exec_sql(&mut ctx, QUERY1).await?;
//     let actual = s.lines().collect::<Vec<_>>();

//     assert_eq!(
//         expected,
//         actual,
//         "output mismatch for {}. Expectedn\n{}Actual:\n{}",
//         description,
//         expected.join("\n"),
//         s
//     );
//     Ok(())
// }

pub fn add_rdbms_to_context(config: ExecutionConfig) -> ExecutionConfig {
    config
        .with_query_planner(Arc::new(SqlDatabaseQueryPlanner {}))
        .add_optimizer_rule(Arc::new(JoinOptimizerRule {}))
}

pub fn make_rdbms_context() -> ExecutionContext {
    let config = ExecutionConfig::new()
        .with_query_planner(Arc::new(SqlDatabaseQueryPlanner {}))
        .with_target_partitions(48)
        .add_optimizer_rule(Arc::new(JoinOptimizerRule {}));

    ExecutionContext::with_config(config)
}

// ------ The implementation of the TopK code follows -----

pub struct SqlDatabaseQueryPlanner {}

#[async_trait]
impl QueryPlanner for SqlDatabaseQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan TopK nodes.
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            SqlDatabaseQueryPlanner {},
        )]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, ctx_state)
            .await
    }
}

impl ExtensionPlanner for SqlDatabaseQueryPlanner {
    fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        ctx_state: &ExecutionContextState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        todo!()
    }
}

/// Rule that detects joins from the same RDBMS source that supports joins
pub struct JoinOptimizerRule {}
impl OptimizerRule for JoinOptimizerRule {
    // Example rewrite pass to insert a user defined LogicalPlanNode
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
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
                        server: Some(server_left),
                        database: Some(db_left),
                        schema: ref schema_left,
                        ..
                    },
                    TableSource::Relational {
                        server: Some(server_right),
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
                        return Ok(LogicalPlan::Extension(Extension {
                            node: Arc::new(SqlJoinPlanNode {
                                left_input: self.optimize(&join.left, execution_props)?,
                                right_input: self.optimize(&join.right, execution_props)?,
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

#[derive(Debug)]
struct SqlJoinPlanNode {
    left_input: LogicalPlan,
    right_input: LogicalPlan,
    /// The join type
    join_type: JoinType,
    /// Join fields
    on: Vec<(Column, Column)>,
    /// The output schema
    schema: DFSchemaRef,
    // TODO: join constraints, other params
}

impl UserDefinedLogicalNode for SqlJoinPlanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.left_input, &self.right_input]
    }

    /// Schema for TopK is the same as the input
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // TODO
        write!(f, "SqlJoin: on={}", "TODO join columns")
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        todo!("from_template for join node not yet supported")
        // assert_eq!(inputs.len(), 1, "input size inconsistent");
        // assert_eq!(exprs.len(), 1, "expression size inconsistent");
        // Arc::new(TopKPlanNode {
        //     k: self.k,
        //     input: inputs[0].clone(),
        //     expr: exprs[0].clone(),
        // })
    }

    fn prevent_predicate_push_down_columns(&self) -> std::collections::HashSet<String> {
        // default (safe) is all columns in the schema.
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }
}

// struct TopKPlanNode {
//     k: usize,
//     input: LogicalPlan,
//     /// The sort expression (this example only supports a single sort
//     /// expr)
//     expr: Expr,
// }

// impl Debug for TopKPlanNode {
//     /// For TopK, use explain format for the Debug format. Other types
//     /// of nodes may
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         self.fmt_for_explain(f)
//     }
// }

// impl UserDefinedLogicalNode for TopKPlanNode {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn inputs(&self) -> Vec<&LogicalPlan> {
//         vec![&self.input]
//     }

//     /// Schema for TopK is the same as the input
//     fn schema(&self) -> &DFSchemaRef {
//         self.input.schema()
//     }

//     fn expressions(&self) -> Vec<Expr> {
//         vec![self.expr.clone()]
//     }

//     /// For example: `TopK: k=10`
//     fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "TopK: k={}", self.k)
//     }

//     fn from_template(
//         &self,
//         exprs: &[Expr],
//         inputs: &[LogicalPlan],
//     ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
//         assert_eq!(inputs.len(), 1, "input size inconsistent");
//         assert_eq!(exprs.len(), 1, "expression size inconsistent");
//         Arc::new(TopKPlanNode {
//             k: self.k,
//             input: inputs[0].clone(),
//             expr: exprs[0].clone(),
//         })
//     }
// }

/// Physical planner for TopK nodes
// struct TopKPlanner {}

// impl ExtensionPlanner for TopKPlanner {
//     /// Create a physical plan for an extension node
//     fn plan_extension(
//         &self,
//         _planner: &dyn PhysicalPlanner,
//         node: &dyn UserDefinedLogicalNode,
//         logical_inputs: &[&LogicalPlan],
//         physical_inputs: &[Arc<dyn ExecutionPlan>],
//         _ctx_state: &ExecutionContextState,
//     ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
//         Ok(
//             if let Some(topk_node) = node.as_any().downcast_ref::<TopKPlanNode>() {
//                 assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
//                 assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
//                 // figure out input name
//                 Some(Arc::new(TopKExec {
//                     input: physical_inputs[0].clone(),
//                     k: topk_node.k,
//                 }))
//             } else {
//                 None
//             },
//         )
//     }
// }

/// Physical operator that executes a database query
#[derive(Clone)]
struct DatabaseExec {
    connection: DatabaseConnector,
    query: String,
    /// The schema after the query is read
    schema: SchemaRef,
}

impl Debug for DatabaseExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DatabaseExec")
    }
}

#[async_trait]
impl ExecutionPlan for DatabaseExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::SinglePartition
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // No children as this is a root node
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            0 => Ok(Arc::new(self.clone())),
            _ => Err(DataFusionError::Internal(
                "Children cannot be replaced in DatabaseExec".to_string(),
            )),
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    /// Execute one partition and return an iterator over RecordBatch
    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "DatabaseExec invalid partition {}",
                partition
            )));
        }

        let (response_tx, response_rx): (
            Sender<ArrowResult<RecordBatch>>,
            Receiver<ArrowResult<RecordBatch>>,
        ) = channel(2);

        let connection = self.connection.clone();
        let query = self.query.clone();

        tokio::task::spawn(async move {
            let reader = connection.into_connection();
            let mut response = reader.fetch_query(&query).await.unwrap();
            while let Some(value) = response.next().await {
                response_tx.send(value).await.unwrap();
            }
        });

        Ok(Box::pin(DatabaseStream {
            schema: self.schema(),
            inner: ReceiverStream::new(response_rx),
        }))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "DatabaseExec:")
            }
        }
    }

    fn statistics(&self) -> Statistics {
        // to improve the optimizability of this plan
        // better statistics inference could be provided
        Statistics::default()
    }
}

/// Database reader, that converts the optimized logical plan into
/// a SQL query, and can execute the query against the database.
struct DatabaseStream {
    schema: SchemaRef,
    inner: ReceiverStream<ArrowResult<RecordBatch>>,
}

impl Stream for DatabaseStream {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for DatabaseStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
