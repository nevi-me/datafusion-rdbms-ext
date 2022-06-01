use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_plan::{
    Column, DFSchemaRef, Expr, JoinType, LogicalPlan, UserDefinedLogicalNode,
};
use sqlparser::ast::Query;

use crate::parser::{expr_to_sql, logical_plan_to_ast, logical_plan_to_sql, DatabaseDialect};
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
        // TODO
        write!(f, "AstPlan: TODO")
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
