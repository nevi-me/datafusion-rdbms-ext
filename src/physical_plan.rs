//! Implememtation of the physical query plan, supporting exection for relations

use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::context::{QueryPlanner, SessionState, TaskContext};
use datafusion::logical_plan::{DFSchema, Expr, LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_plan::planner::{DefaultPhysicalPlanner, ExtensionPlanner};
use datafusion::{
    arrow::{
        datatypes::SchemaRef,
        error::{ArrowError, Result as ArrowResult},
        record_batch::RecordBatch,
    },
    error::DataFusionError as DFError,
    error::Result as DFResult,
    physical_plan::{expressions::PhysicalSortExpr, *},
};
use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

use crate::node::*;
use crate::sqldb::postgres::table_provider::PostgresTableProvider;
use crate::sqldb::{DatabaseConnection, DatabaseConnector};

/// Physical operator that executes a database query
#[derive(Clone)]
struct DatabaseExec {
    connector: DatabaseConnector,
    query: String,
    /// The schema after the query is read
    schema: SchemaRef,
}

impl std::fmt::Debug for DatabaseExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseExec")
            .field("query", &self.query)
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for DatabaseExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn std::any::Any {
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

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    /// Execute one partition and return an iterator over RecordBatch
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let (response_tx, response_rx): (
            Sender<ArrowResult<RecordBatch>>,
            Receiver<ArrowResult<RecordBatch>>,
        ) = channel(2000);

        let connector = self.connector.clone();
        let query = self.query.clone();
        let schema = self.schema();

        let reader = connector.into_connection();
        let stream = reader.fetch_query(&query, schema).unwrap();

        Ok(stream)
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

    fn relies_on_input_order(&self) -> bool {
        true
    }

    fn maintains_input_order(&self) -> bool {
        false
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        !matches!(
            self.required_child_distribution(),
            Distribution::SinglePartition
        )
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        match children.len() {
            0 => Ok(self),
            _ => Err(DFError::Internal(
                "Children cannot be replaced in DatabaseExec".to_string(),
            )),
        }
    }
}

/// Database reader, that converts the optimized logical plan into
/// a SQL query, and can execute the query against the database.
pub struct DatabaseStream {
    pub schema: SchemaRef,
    pub inner: ReceiverStream<ArrowResult<RecordBatch>>,
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

pub struct SqlDatabaseQueryPlanner {}

#[async_trait::async_trait]
impl QueryPlanner for SqlDatabaseQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan RDBMS nodes.
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            SqlDatabaseQueryPlanner {},
        )]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
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
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        // Check the custom nodes
        Ok(
            if let Some(node) = node.as_any().downcast_ref::<SqlAstPlanNode>() {
                let query = node.ast.to_string();
                let query = fix_query(&query);
                dbg!(&query);

                let schema = Schema::new_with_metadata(
                    node.schema()
                        .fields()
                        .iter()
                        .map(|f| f.field().clone())
                        .collect(),
                    node.schema().metadata().clone(),
                );

                Some(Arc::new(DatabaseExec {
                    connector: node.connector.clone(),
                    query,
                    schema: Arc::new(schema),
                }))
            } else {
                None
            },
        )
    }
}

pub struct SqlPhysicalQueryPlanner {}

#[async_trait::async_trait]
impl PhysicalPlanner for SqlPhysicalQueryPlanner {
    // Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        println!("Logical Plan:\n{:?}", logical_plan);
        let state_cloned = session_state.clone();
        match logical_plan {
            LogicalPlan::Extension(extension) => {
                let node = extension.node.as_any();
                if let Some(node) = node.downcast_ref::<SqlAstPlanNode>() {
                    let schema = Schema::new_with_metadata(
                        node.schema()
                            .fields()
                            .iter()
                            .map(|f| f.field().clone())
                            .collect(),
                        node.schema().metadata().clone(),
                    );
                    return Ok(Arc::new(DatabaseExec {
                        connector: node.connector.clone(),
                        query: fix_query(&node.ast.to_string()),
                        schema: Arc::new(schema),
                    }));
                } else {
                    panic!()
                }
            }
            _ => state_cloned.create_physical_plan(logical_plan).await,
        }
    }

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `expr`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `expr`
    ///
    /// `input_schema`: the physical schema for evaluating `expr`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        panic!()
    }
}

/// Utility function to fix SQL queries' expressions that are invalid.
/// Exmaple: `Uint64(x)` -> `x`
fn fix_query(query: &str) -> String {
    // TODO: compile these once, but this is a fallback to not stall development.
    // A more correct solution is to support writing SQL strings from sqlparser-rs.
    let re_int64 = regex::Regex::new(r"Int64\((?P<value>\d+)\)").unwrap();
    let re_uint64 = regex::Regex::new(r"Uint64\((?P<value>\d+)\)").unwrap();
    let re_float64 = regex::Regex::new(r"Float64\((?P<value>[\d.]+)\)").unwrap();
    let re_l_lit = regex::Regex::new(r"(?P<value>\d+)L").unwrap();
    let query = re_int64.replace_all(query, "$value");
    let query = re_uint64.replace_all(&query, "$value");
    let query = re_float64.replace_all(&query, "$value");
    let query = re_l_lit.replace_all(&query, "$value");
    query.to_string()
}

#[test]
fn test_fix_query() {
    let query = r#"select Int64(1), Int64(100), Uint64(1000), 1L"#;
    let fixed = fix_query(query);
    assert_eq!(&fixed, "select 1, 100, 1000, 1")
}
