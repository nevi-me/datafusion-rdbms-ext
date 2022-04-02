use std::sync::Arc;

use datafusion::error::Result as DfResult;
use datafusion::logical_plan::{
    Column, DFSchemaRef, Expr, JoinType, LogicalPlan, UserDefinedLogicalNode,
};

use crate::parser::{expr_to_sql, logical_plan_to_sql, DatabaseDialect};

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

        // Convert join columns to a query
        let join_columns = if self.on.is_empty() {
            "1 = 1".to_string()
        } else {
            self.on
                .iter()
                .map(|(a, b)| {
                    if a.name != b.name {
                        // No name clash
                        format!("{} = {}", a.name, b.name)
                    } else {
                        format!(
                            "{}.{} = {}.{}",
                            a.relation.as_ref().unwrap(),
                            a.name,
                            b.relation.as_ref().unwrap(),
                            b.name
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(" AND ")
        };

        Ok(format!(
            "SELECT a.*, b.* FROM ({left}) a {join_type} JOIN ({right}) b on {join_columns}"
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
        write!(f, "SqlJoin: on=TODO join columns")
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
            join_type: self.join_type,
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

#[derive(Debug)]
pub(crate) struct SqlProjectAggregateNode {
    /// The input before the tablescan
    pub(crate) input: LogicalPlan,
    pub(crate) group_expr: Vec<Expr>,
    pub(crate) aggr_expr: Vec<Expr>,
    pub(crate) proj_expr: Vec<Expr>,
    pub(crate) proj_alias: Option<String>,
    /// The schema after the aggregate
    pub(crate) schema: DFSchemaRef,
}

impl SqlProjectAggregateNode {
    pub fn to_sql(&self) -> DfResult<String> {
        let input = logical_plan_to_sql(&self.input, DatabaseDialect::Generic)?;
        let projection = self
            .proj_expr
            .iter()
            .map(|expr| expr_to_sql(expr, DatabaseDialect::Generic))
            .collect::<Vec<_>>()
            .join(" AND ");
        let group_by = if self.group_expr.is_empty() {
            String::new()
        } else {
            let exprs = self
                .group_expr
                .iter()
                .map(|expr| expr_to_sql(expr, DatabaseDialect::Generic))
                .collect::<Vec<_>>()
                .join(", ");
            format!("GROUP BY {exprs}")
        };
        dbg!(&projection, &group_by);
        Ok(format!("SELECT {projection} FROM ({input}) a {group_by}"))
    }
}

impl UserDefinedLogicalNode for SqlProjectAggregateNode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
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
        write!(f, "SqlProjectAggregate:")
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Arc::new(SqlProjectAggregateNode {
            input: inputs[0].clone(),
            group_expr: self.group_expr.clone(),
            aggr_expr: self.aggr_expr.clone(),
            proj_expr: self.proj_expr.clone(),
            proj_alias: self.proj_alias.clone(),
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
