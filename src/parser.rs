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

/// Converts a logical plan to SQL AST
pub fn logical_plan_to_ast(
    plan: &LogicalPlan,
    dialect: DatabaseDialect,
) -> DfResult<sqlparser::ast::Query> {
    use sqlparser::ast::*;
    Ok(match plan {
        LogicalPlan::Projection(projection) => {
            let inner_query = logical_plan_to_ast(&projection.input, dialect)?;
            let body = SetExpr::Query(Box::new(inner_query));
            Query {
                with: None,
                body,
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
                lock: None,
            }
        }
        LogicalPlan::Filter(filter) => {
            // A filter can loosely be expressed as "select * from input where {predicate}"
            // While this nests a table select, we ordinarily expect to generate SQL code
            // from an already optimised LogicalPlan, so this should not be a common occurrence
            todo!()
        }
        LogicalPlan::Window(_) => todo!(),
        LogicalPlan::Aggregate(aggregate) => {
            let proj = aggregate
                .aggr_expr
                .iter()
                .map(|expr| expr_to_sql(expr, dialect))
                .collect::<Vec<_>>()
                .join(", ");
            let aggr = aggregate
                .group_expr
                .iter()
                .map(|expr| expr_to_sql(expr, dialect))
                .collect::<Vec<_>>()
                .join(", ");
            todo!()
        }
        LogicalPlan::Sort(_) => todo!(),
        LogicalPlan::Join(_) => todo!(),
        LogicalPlan::CrossJoin(_) => todo!(),
        LogicalPlan::Repartition(_) => todo!(),
        LogicalPlan::Union(_) => todo!(),
        LogicalPlan::TableScan(scan) => {
            let projection = if scan.projection.is_none() {
                vec![SelectItem::Wildcard]
            } else {
                scan.projected_schema.fields().iter().map(|field| {
                    // TODO handle aliases
                    SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                        value: field.name().clone(),
                        quote_style: None,
                    }))
                }).collect()
            };
            let selection = if scan.filters.is_empty() {
                None
            } else {
                let mut filter_iter = scan.filters.iter();
                Some(scan.filters.iter().map(|filter| {}))
            };
            Query {
                with: None,
                body: SetExpr::Select(Box::new(Select {
                    distinct: false,
                    top: None,
                    projection,
                    from: vec![TableWithJoins {
                        relation: TableFactor::Table {
                            name: ObjectName(
                                scan.table_name
                                    .split(".")
                                    .map(|s| Ident {
                                        value: s.to_string(),
                                        quote_style: None,
                                    })
                                    .collect(),
                            ),
                            alias: None,
                            args: vec![],
                            with_hints: vec![],
                        },
                        joins: vec![],
                    }],
                    lateral_views: vec![],
                    selection: None,
                    group_by: vec![],
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                })),
                order_by: todo!(),
                limit: todo!(),
                offset: todo!(),
                fetch: todo!(),
                lock: todo!(),
            }
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
        LogicalPlan::CreateCatalogSchema(_) => todo!(),
    })
}

/// An inner function that converts a [&LogicalPlan] to a SQL query
pub fn logical_plan_to_sql(plan: &LogicalPlan, dialect: DatabaseDialect) -> DfResult<String> {
    Ok(match plan {
        LogicalPlan::Projection(projection) => {
            let projected = projection
                .expr
                .iter()
                .map(|expr| expr_to_sql(expr, dialect))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT {} FROM {}",
                projected,
                logical_plan_to_sql(&projection.input, dialect)?
            )
        }
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
        LogicalPlan::Aggregate(aggregate) => {
            let proj = aggregate
                .aggr_expr
                .iter()
                .map(|expr| expr_to_sql(expr, dialect))
                .collect::<Vec<_>>()
                .join(", ");
            let aggr = aggregate
                .group_expr
                .iter()
                .map(|expr| expr_to_sql(expr, dialect))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT {} FROM {}{}",
                proj,
                if aggregate.group_expr.is_empty() {
                    ""
                } else {
                    " GROUP BY "
                },
                aggr
            )
        }
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
                    .collect::<Vec<_>>()
                    .join(", "),
            };
            let filter_sql = if scan.filters.is_empty() {
                String::new()
            } else {
                // TODO: find the most performant solution.
                // See https://stackoverflow.com/questions/36941851/whats-an-idiomatic-way-to-print-an-iterator-separated-by-spaces-in-rust
                scan.filters
                    .iter()
                    .map(|filter| expr_to_sql(filter, dialect))
                    .fold(String::from(" WHERE "), |a, b| a + "AND " + &b)
                    .replace("WHERE AND ", "WHERE ")
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
        LogicalPlan::CreateCatalogSchema(_) => todo!(),
    })
}

/// Convert an expression back to AST
fn expr_to_ast(expr: &Expr, _dialect: DatabaseDialect) -> sqlparser::ast::Expr {
    use datafusion::scalar::ScalarValue;
    use sqlparser::ast::Expr as OutExpr;
    use sqlparser::ast::{Ident};
    match expr {
        Expr::Alias(expr, alias) => {
            todo!()
        }
        Expr::Column(col) => OutExpr::Identifier(Ident {
            value: col.name.clone(),
            quote_style: None,
        }),
        Expr::ScalarVariable(_, _) => todo!(),
        // Date32 gets written as the i32 number, which won't work on SQL DBs
        Expr::Literal(lit) => match lit {
            // ScalarValue::Boolean(_) => todo!(),
            // ScalarValue::Float32(_) => todo!(),
            // ScalarValue::Float64(_) => todo!(),
            // ScalarValue::Decimal128(_, _, _) => todo!(),
            // ScalarValue::Int8(_) => todo!(),
            // ScalarValue::Int16(_) => todo!(),
            // ScalarValue::Int32(_) => todo!(),
            // ScalarValue::Int64(_) => todo!(),
            // ScalarValue::UInt8(_) => todo!(),
            // ScalarValue::UInt16(_) => todo!(),
            // ScalarValue::UInt32(_) => todo!(),
            // ScalarValue::UInt64(_) => todo!(),
            ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) => match value {
                Some(string) => format!("'{string}'"),
                None => "NULL".to_string(),
            },
            // ScalarValue::Binary(_) => todo!(),
            // ScalarValue::LargeBinary(_) => todo!(),
            // ScalarValue::List(_, _) => todo!(),
            ScalarValue::Date32(None) => "NULL".to_string(),
            ScalarValue::Date32(Some(value)) => {
                let datetime = datafusion::arrow::temporal_conversions::date32_to_datetime(*value);
                let date = datetime.format("%Y-%m-%d").to_string();
                format!("'{date}'")
            }
            // ScalarValue::Date64(_) => todo!(),
            // ScalarValue::TimestampSecond(_, _) => todo!(),
            // ScalarValue::TimestampMillisecond(_, _) => todo!(),
            // ScalarValue::TimestampMicrosecond(_, _) => todo!(),
            // ScalarValue::TimestampNanosecond(_, _) => todo!(),
            // ScalarValue::IntervalYearMonth(_) => todo!(),
            // ScalarValue::IntervalDayTime(_) => todo!(),
            // ScalarValue::IntervalMonthDayNano(_) => todo!(),
            // ScalarValue::Struct(_, _) => todo!(),
            _ => lit.to_string(),
        },
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
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            format!(
                "{}{}{} AND {}",
                expr_to_sql(expr, _dialect),
                if *negated {
                    " NOT BETWEEN "
                } else {
                    " BETWEEN "
                },
                expr_to_sql(low, _dialect),
                expr_to_sql(high, _dialect)
            )
        }
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
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let list = list
                .iter()
                .map(|expr| expr_to_sql(expr, _dialect))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "{} {}({})",
                expr_to_sql(expr, _dialect),
                if *negated { "NOT IN " } else { "IN" },
                list
            )
        }
        Expr::Wildcard => todo!(),
        Expr::QualifiedWildcard { .. } => todo!(),
    }
}

/// Parses the expression to SQL code
pub fn expr_to_sql(expr: &Expr, _dialect: DatabaseDialect) -> String {
    use datafusion::scalar::ScalarValue;
    match expr {
        Expr::Alias(expr, alias) => {
            format!("{} AS {}", expr_to_sql(expr, _dialect), alias)
        }
        Expr::Column(col) => col.name.clone(),
        Expr::ScalarVariable(_, _) => todo!(),
        // Date32 gets written as the i32 number, which won't work on SQL DBs
        Expr::Literal(lit) => match lit {
            // ScalarValue::Boolean(_) => todo!(),
            // ScalarValue::Float32(_) => todo!(),
            // ScalarValue::Float64(_) => todo!(),
            // ScalarValue::Decimal128(_, _, _) => todo!(),
            // ScalarValue::Int8(_) => todo!(),
            // ScalarValue::Int16(_) => todo!(),
            // ScalarValue::Int32(_) => todo!(),
            // ScalarValue::Int64(_) => todo!(),
            // ScalarValue::UInt8(_) => todo!(),
            // ScalarValue::UInt16(_) => todo!(),
            // ScalarValue::UInt32(_) => todo!(),
            // ScalarValue::UInt64(_) => todo!(),
            ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) => match value {
                Some(string) => format!("'{string}'"),
                None => "NULL".to_string(),
            },
            // ScalarValue::Binary(_) => todo!(),
            // ScalarValue::LargeBinary(_) => todo!(),
            // ScalarValue::List(_, _) => todo!(),
            ScalarValue::Date32(None) => "NULL".to_string(),
            ScalarValue::Date32(Some(value)) => {
                let datetime = datafusion::arrow::temporal_conversions::date32_to_datetime(*value);
                let date = datetime.format("%Y-%m-%d").to_string();
                format!("'{date}'")
            }
            // ScalarValue::Date64(_) => todo!(),
            // ScalarValue::TimestampSecond(_, _) => todo!(),
            // ScalarValue::TimestampMillisecond(_, _) => todo!(),
            // ScalarValue::TimestampMicrosecond(_, _) => todo!(),
            // ScalarValue::TimestampNanosecond(_, _) => todo!(),
            // ScalarValue::IntervalYearMonth(_) => todo!(),
            // ScalarValue::IntervalDayTime(_) => todo!(),
            // ScalarValue::IntervalMonthDayNano(_) => todo!(),
            // ScalarValue::Struct(_, _) => todo!(),
            _ => lit.to_string(),
        },
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
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            format!(
                "{}{}{} AND {}",
                expr_to_sql(expr, _dialect),
                if *negated {
                    " NOT BETWEEN "
                } else {
                    " BETWEEN "
                },
                expr_to_sql(low, _dialect),
                expr_to_sql(high, _dialect)
            )
        }
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
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let list = list
                .iter()
                .map(|expr| expr_to_sql(expr, _dialect))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "{} {}({})",
                expr_to_sql(expr, _dialect),
                if *negated { "NOT IN " } else { "IN" },
                list
            )
        }
        Expr::Wildcard => todo!(),
        Expr::QualifiedWildcard { .. } => todo!(),
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

    #[test]
    fn test_ast() {
        let query = "select * from a where b = 3 and c > d";
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let out = sqlparser::parser::Parser::parse_sql(&dialect, query).unwrap();
        println!("AST: {:#?}", out);
    }
}
