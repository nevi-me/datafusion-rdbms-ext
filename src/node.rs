use std::sync::Arc;

use datafusion::error::Result as DfResult;
use datafusion::logical_plan::{
    Column, DFSchemaRef, Expr, JoinType, LogicalPlan, UserDefinedLogicalNode,
};

use crate::parser::{logical_plan_to_sql, DatabaseDialect};

#[derive(Debug)]
pub(crate) struct SqlJoinPlanNode {
    pub(crate) left_input: LogicalPlan,
    pub(crate) right_input: LogicalPlan,
    /// The join type
    pub(crate) join_type: JoinType,
    /// Join fields
    pub(crate) on: Vec<(Column, Column)>,
    /// The output schema
    pub(crate) schema: DFSchemaRef,
    // TODO: join constraints, other params
}

impl SqlJoinPlanNode {
    /// Create a join SQL query
    /// TODO: use database dialect for downstream logical nodes
    pub fn to_sql(&self) -> DfResult<String> {
        let left = logical_plan_to_sql(&self.left_input, DatabaseDialect::Generic)?;
        let right = logical_plan_to_sql(&self.right_input, DatabaseDialect::Generic)?;
        let join_type = self.join_type.to_string().to_uppercase();

        Ok(format!(
            "SELECT a.*, b.* FROM ({left}) a {join_type} JOIN ({right}) b on 1 = 1"
        ))
    }
}

impl UserDefinedLogicalNode for SqlJoinPlanNode {
    fn as_any(&self) -> &dyn std::any::Any {
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

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // TODO
        write!(f, "SqlJoin: on={}", "TODO join columns")
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 2, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Arc::new(SqlJoinPlanNode {
            left_input: inputs[0].clone(),
            right_input: inputs[1].clone(),
            join_type: self.join_type.clone(),
            on: self.on.clone(),
            schema: self.schema.clone(),
        })
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
