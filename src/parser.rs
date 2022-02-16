use datafusion::error::Result as DfResult;
use datafusion::logical_plan::*;

pub struct LogicalPlanExt {
    plan: LogicalPlan,
    dialect: DatabaseDialect,
}

#[derive(Copy, Clone, Debug)]
pub enum DatabaseDialect {
    Generic,
    // MsSql,
    // Postgres,
    // MySql,
    // Oracle,
}

/// A trait to convert a logical plan to SQL
pub trait LogicalPlanSqlExt {
    fn to_sql(&self) -> DfResult<String>;
}

impl LogicalPlanSqlExt for LogicalPlanExt {
    fn to_sql(&self) -> DfResult<String> {
        logical_plan_to_sql(&self.plan, self.dialect)
    }
}

/// An inner function that converts a [&LogicalPlan] to a SQL query
/// TODO: should take dialect as an input
fn logical_plan_to_sql(plan: &LogicalPlan, dialect: DatabaseDialect) -> DfResult<String> {
    Ok(match plan {
        LogicalPlan::Projection(_) => todo!(),
        LogicalPlan::Filter(filter) => {
            // A filter can loosely be expressed as "select * from input where {predicate}"
            // While this nests a table select, we ordinarily expect to generate SQL code
            // from an already optimised LogicalPlan, so this should not be a common occurrence
            format!(
                "SELECT * FROM ({}) {} WHERE {}",
                logical_plan_to_sql(&filter.input, dialect)?,
                "tbl_a", // TODO: this shouldn't be hardcoded
                expr_to_sql(&filter.predicate, dialect)
            )
        }
        LogicalPlan::Window(_) => todo!(),
        LogicalPlan::Aggregate(_) => todo!(),
        LogicalPlan::Sort(_) => todo!(),
        LogicalPlan::Join(_) => todo!(),
        LogicalPlan::CrossJoin(_) => todo!(),
        LogicalPlan::Repartition(_) => todo!(),
        LogicalPlan::Union(_) => todo!(),
        LogicalPlan::TableScan(scan) => {
            let projection_sql = match &scan.projection {
                None => String::from("*"),
                Some(projection) => projection
                    .iter()
                    .map(|index| scan.source.schema().field(*index).name().clone())
                    .fold(String::new(), |a, b| a + ", " + &b),
            };
            let filter_sql = if scan.filters.is_empty() {
                String::new()
            } else {
                // TODO: find the most performant solution.
                // See https://stackoverflow.com/questions/36941851/whats-an-idiomatic-way-to-print-an-iterator-separated-by-spaces-in-rust
                scan.filters
                    .iter()
                    .map(|filter| expr_to_sql(filter, dialect))
                    .fold(String::from(" WHERE "), |a, b| a + ", " + &b)
            };
            format!(
                "SELECT {} FROM {}{}{}",
                projection_sql,
                scan.table_name,
                filter_sql,
                scan.limit
                    .map(|l| format!(" LIMIT {}", l))
                    .unwrap_or_default()
            )
        }
        LogicalPlan::EmptyRelation(_) => todo!(),
        LogicalPlan::Limit(_) => todo!(),
        LogicalPlan::CreateExternalTable(_) => todo!(),
        LogicalPlan::CreateMemoryTable(_) => todo!(),
        LogicalPlan::DropTable(_) => todo!(),
        LogicalPlan::Values(_) => todo!(),
        LogicalPlan::Explain(_) => todo!(),
        LogicalPlan::Analyze(_) => todo!(),
        LogicalPlan::Extension(_) => todo!(),
    })
}

pub fn expr_to_sql(expr: &Expr, _dialect: DatabaseDialect) -> String {
    match expr {
        Expr::Alias(expr, alias) => {
            format!("{} AS {}", expr_to_sql(expr, _dialect), alias)
        }
        Expr::Column(col) => col.name.clone(),
        Expr::ScalarVariable(_) => todo!(),
        Expr::Literal(lit) => lit.to_string(),
        Expr::BinaryExpr { left, op, right } => {
            // TODO: expand into a match statement if some ops don't translate to SQL
            match op {
                Operator::And | Operator::Or => {
                    // Wrap in parentheses
                    format!(
                        "({} {} {})",
                        expr_to_sql(left, _dialect),
                        op,
                        expr_to_sql(right, _dialect)
                    )
                }
                _ => {
                    format!(
                        "{} {} {}",
                        expr_to_sql(left, _dialect),
                        op,
                        expr_to_sql(right, _dialect)
                    )
                }
            }
        }
        Expr::Not(expr) => {
            format!("NOT {}", expr_to_sql(expr, _dialect))
        }
        Expr::IsNotNull(expr) => {
            format!("{} IS NOT NULL", expr_to_sql(expr, _dialect))
        }
        Expr::IsNull(expr) => {
            format!("{} IS NULL", expr_to_sql(expr, _dialect))
        }
        Expr::Negative(expr) => {
            format!("-({})", expr)
        }
        Expr::GetIndexedField { .. } => todo!(),
        Expr::Between { .. } => todo!(),
        Expr::Case { .. } => {
            todo!()
        }
        Expr::Cast { expr, .. } => {
            // TODO: complete the cast with datatype_to_sql
            format!("CAST({} AS {})", expr_to_sql(expr, _dialect), "")
        }
        Expr::TryCast { .. } => todo!(),
        Expr::Sort {
            expr,
            asc,
            nulls_first,
        } => {
            format!(
                "ORDER BY {}{}{}",
                expr_to_sql(expr, _dialect),
                if !*asc { " DESC" } else { "" },
                if *nulls_first { " NULLS FIRST" } else { "" }
            )
        }
        Expr::ScalarFunction { .. } => todo!(),
        Expr::ScalarUDF { .. } => todo!(),
        Expr::AggregateFunction { .. } => todo!(),
        Expr::WindowFunction { .. } => todo!(),
        Expr::AggregateUDF { .. } => todo!(),
        Expr::InList { .. } => todo!(),
        Expr::Wildcard => todo!(),
    }
}

// fn datatype_to_sql(data_type: &DataType, dialect: DatabaseDialect) -> String {
//     match dialect {
//         DatabaseDialect::Generic => todo!(),
//         DatabaseDialect::MsSql => todo!(),
//         DatabaseDialect::Postgres => todo!(),
//         DatabaseDialect::MySql => todo!(),
//         DatabaseDialect::Oracle => todo!(),
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::datasource::TableProviderFilterPushDown;
    use datafusion::error::Result as DfResult;
    use datafusion::{
        arrow::datatypes::SchemaRef, datasource::TableProvider, logical_plan::LogicalPlanBuilder,
        physical_plan::ExecutionPlan,
    };

    struct TestTableProvider {
        schema: SchemaRef,
    }

    #[async_trait]
    impl TableProvider for TestTableProvider {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn supports_filter_pushdown(
            &self,
            _filter: &Expr,
        ) -> DfResult<TableProviderFilterPushDown> {
            Ok(TableProviderFilterPushDown::Unsupported)
        }

        async fn scan(
            &self,
            _projection: &Option<Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DfResult<Arc<dyn ExecutionPlan>> {
            unimplemented!("We do not test scans")
        }
    }

    #[test]
    fn test_tablescan_sql() {
        let fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ];
        let schema = Schema::new(fields);
        let plan = LogicalPlanBuilder::scan_empty(Some("test_table"), &schema, None)
            .unwrap()
            .filter(col("a").lt(lit(100i32)).or(col("b").is_not_null()))
            .unwrap()
            .build()
            .unwrap();

        let plan_ext = LogicalPlanExt {
            plan,
            dialect: DatabaseDialect::Generic,
        };

        let sql = plan_ext.to_sql().unwrap();
        assert_eq!(
            "SELECT * FROM (SELECT * FROM test_table) tbl_a WHERE (a < 100 OR b IS NOT NULL)",
            &sql
        );
    }
}