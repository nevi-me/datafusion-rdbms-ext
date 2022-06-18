use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_plan::{DFSchemaRef, Expr, LogicalPlan, UserDefinedLogicalNode};
use sqlparser::ast::Query;

use crate::parser::{logical_plan_to_ast, DatabaseDialect};
use crate::sqldb::DatabaseConnector;

#[derive(Debug)]
pub(crate) struct SqlAstPlanNode {
    pub(crate) input: LogicalPlan,
    pub(crate) schema: DFSchemaRef,
    pub ast: Query,
    pub connector: DatabaseConnector,
}

impl SqlAstPlanNode {
    /// Try to convert a logical plan into a node.
    /// If this function fails, then the optimizer cannot pass the query to
    /// the database.
    pub fn try_from_plan(plan: &LogicalPlan) -> DfResult<Self> {
        let (ast, connector) =
            logical_plan_to_ast(plan, DatabaseDialect::Generic).map_err(DataFusionError::from)?;
        println!("Logical plan: {}", ast);
        Ok(Self {
            input: plan.clone(),
            schema: plan.schema().clone(),
            ast,
            connector,
        })
    }
}

impl UserDefinedLogicalNode for SqlAstPlanNode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // TODO: should this be empty?
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SqlAstPlan:")
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        let (ast, connector) = logical_plan_to_ast(&inputs[0], DatabaseDialect::Generic).unwrap();
        Arc::new(SqlAstPlanNode {
            ast,
            input: inputs[0].clone(),
            schema: self.schema.clone(),
            connector,
        })
    }
}
