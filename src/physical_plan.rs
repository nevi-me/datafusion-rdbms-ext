//! Implememtation of the physical query plan, supporting exection for relations

use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::context::{QueryPlanner, SessionState, TaskContext};
use datafusion::logical_plan::{LogicalPlan, TableScan, UserDefinedLogicalNode};
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
        let stream = reader.fetch_query(&query, schema.clone()).unwrap();

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
            0 => Ok(self.clone()),
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
            } else if let Some(join_node) = node.as_any().downcast_ref::<SqlJoinPlanNode>() {
                assert_eq!(logical_inputs.len(), 2, "input size inconsistent");
                assert_eq!(physical_inputs.len(), 2, "input size inconsistent");
                // Extract connectino from any of the logical inputs
                let input1 = logical_inputs[0];
                let connector = if let LogicalPlan::TableScan(TableScan { ref source, .. }) = input1
                {
                    // TODO check which type to cast to
                    let source = source
                        .as_any()
                        .downcast_ref::<PostgresTableProvider>()
                        .unwrap();
                    source.connection().to_connector()
                } else {
                    return Ok(None);
                };
                // let schema = Arc::new(join_node.schema.as_ref().into());
                let left_schema = physical_inputs[0].schema();
                let right_schema = physical_inputs[1].schema();
                let schema = Schema::try_merge(vec![
                    left_schema.as_ref().clone(),
                    right_schema.as_ref().clone(),
                ])?;
                Some(Arc::new(DatabaseExec {
                    connector,
                    query: join_node.to_sql()?,
                    schema: Arc::new(schema),
                }))
            } else if let Some(node) = node.as_any().downcast_ref::<SqlProjectAggregateNode>() {
                assert_eq!(logical_inputs.len(), 1, "input size inconsistent");
                assert_eq!(physical_inputs.len(), 1, "input size inconsistent");
                // Extract connectino from any of the logical inputs
                let input = logical_inputs[0];
                let connector = if let LogicalPlan::TableScan(TableScan { ref source, .. }) = input
                {
                    // TODO check which type to cast to
                    let source = source
                        .as_any()
                        .downcast_ref::<PostgresTableProvider>()
                        .unwrap();
                    source.connection().to_connector()
                } else {
                    return Ok(None);
                };
                // let schema = Arc::new(join_node.schema.as_ref().into());
                let schema = physical_inputs[0].schema();
                Some(Arc::new(DatabaseExec {
                    connector,
                    query: dbg!(node.to_sql()?),
                    schema,
                }))
            } else {
                None
            },
        )
    }
}
