use std::borrow::BorrowMut;
use std::collections::HashMap;

use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::datasource::TableOrigin;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::BuiltinScalarFunction;
use datafusion::logical_plan::plan::DefaultTableSource;
use datafusion::logical_plan::*;
use datafusion::physical_plan::aggregates::AggregateFunction;
use sqlparser::ast::{Ident, ObjectName, SelectItem};

use crate::error::RdbmsError;
use crate::node::SqlAstPlanNode;
use crate::sqldb::postgres::table_provider::PostgresTableProvider;
use crate::sqldb::DatabaseConnector;

#[derive(Copy, Clone, Debug)]
pub enum DatabaseDialect {
    Generic,
    // MsSql,
    // Postgres,
    // MySql,
    // Oracle,
}

/// Converts a logical plan to SQL AST
pub fn logical_plan_to_ast(
    plan: &LogicalPlan,
    dialect: DatabaseDialect,
) -> Result<(sqlparser::ast::Query, DatabaseConnector), RdbmsError> {
    use datafusion::logical_plan::Expr as InExpr;
    use sqlparser::ast::*;
    println!(
        "------------------------\nPlan to AST input:\n{:#?}\n------------------------",
        plan
    );
    Ok(match plan {
        LogicalPlan::Projection(outer_project) => {
            if let LogicalPlan::Aggregate(agg) = &*outer_project.input {
                if let LogicalPlan::Projection(inner_project) = &*agg.input {
                    if let LogicalPlan::TableScan(scan) = &*inner_project.input {
                        // Collect the names from the external projection
                        for e in &inner_project.expr {
                            println!("Inner project: {}", e.to_string().replace('#', ""));
                        }
                        for e in &agg.aggr_expr {
                            if let InExpr::Alias(_, alias) = e {
                                println!("Alias: {}", alias);
                            } else if let InExpr::AggregateFunction {
                                fun,
                                args,
                                distinct,
                            } = e
                            {
                                dbg!(fun, distinct);
                                println!("Aggr: {}", expr_inner_name(&args[0]));
                            }
                            println!("Aggregate: {}", e.to_string().replace('#', ""));
                        }
                        for e in &outer_project.expr {
                            println!("Outer project: {}", e.to_string().replace('#', ""));
                        }
                        // panic!();
                        let inner_agg_expr = agg
                            .aggr_expr
                            .iter()
                            .map(|e| {
                                println!("Expr name: {:?}", e.to_string().replace('#', ""));
                                match e {
                                    InExpr::Alias(ex, alias) => {
                                        let key = alias.replace('#', "");
                                        println!("inner key (a): {},\n\t\tvalue: {}\n", key, ex);
                                        (key, ex.clone())
                                    }
                                    _ => {
                                        // println!("Extracted name: {}", expr_inner_name(e));
                                        let key = e.to_string().replace('#', "");
                                        println!("inner key: {},\n\t\tvalue: {}\n", key, e);
                                        (key, Box::new(e.clone()))
                                    }
                                }
                            })
                            .collect::<HashMap<_, _>>();

                        let mut outer_agg_expr = HashMap::new();
                        for expr in &outer_project.expr {
                            if let InExpr::Alias(e, alias) = expr {
                                let key = e.to_string().replace('#', "");
                                println!("Outer key: {}, value: {}", key, alias);
                                outer_agg_expr.insert(key, (alias.to_owned(), e.clone()));
                            }
                        }

                        // Extract connection
                        let TableScan { ref source, .. } = scan;
                        let connector = {
                            // TODO check which type to cast to
                            let source = source
                                .as_any()
                                .downcast_ref::<DefaultTableSource>()
                                .unwrap();
                            let source = source
                                .table_provider
                                .as_any()
                                .downcast_ref::<PostgresTableProvider>()
                                .unwrap();
                            source.connection().to_connector()
                        };
                        let query = Query {
                            with: None,
                            body: SetExpr::Select(Box::new(Select {
                                distinct: false,
                                top: None,
                                projection: outer_project
                                    .expr
                                    .iter()
                                    .map(|e| {
                                        // If expr is alias, use its aliased expr
                                        match e {
                                            InExpr::Alias(ex, alias) => {
                                                let key = ex.to_string().replace('#', "");
                                                println!(
                                                    "Aliased expr: {key}, e: {}",
                                                    e.to_string().replace('#', "")
                                                );
                                                match inner_agg_expr.get(&key) {
                                                    Some(aliased) => expr_to_select_item(
                                                        &InExpr::Alias(
                                                            aliased.clone(),
                                                            alias.to_owned(),
                                                        ),
                                                        dialect,
                                                    ),
                                                    None => expr_to_select_item(ex, dialect),
                                                }
                                            }
                                            _ => expr_to_select_item(e, dialect),
                                        }
                                    })
                                    .collect(),
                                from: vec![TableWithJoins {
                                    relation: TableFactor::Table {
                                        name: ObjectName(
                                            scan.table_name
                                                .split('.')
                                                .map(|s| Ident {
                                                    value: s.to_string(),
                                                    quote_style: None,
                                                })
                                                .collect(),
                                        ),
                                        alias: None,
                                        args: None,
                                        with_hints: vec![],
                                    },
                                    joins: vec![],
                                }],
                                into: None,
                                lateral_views: vec![],
                                selection: None, //
                                group_by: agg
                                    .group_expr
                                    .iter()
                                    .map(|expr| expr_to_ast(expr, dialect))
                                    .collect::<Vec<_>>(),
                                cluster_by: vec![],
                                distribute_by: vec![],
                                sort_by: vec![],
                                having: None,
                                qualify: None,
                            })),
                            order_by: vec![],
                            limit: None,
                            offset: None,
                            fetch: None,
                            lock: None,
                        };
                        return Ok((query, connector));
                    }
                }
            } else if let LogicalPlan::Join(_) = &*outer_project.input {
                // A join with a select
                let (mut join, connector) = logical_plan_to_ast(&outer_project.input, dialect)?;
                let projection = outer_project
                    .expr
                    .iter()
                    .map(|expr| expr_to_select_item(expr, dialect))
                    .collect::<Vec<_>>();
                // Replace the join's projection
                match join.body.borrow_mut() {
                    SetExpr::Select(select) => {
                        select.projection = projection;
                    }
                    _ => panic!(),
                }
                return Ok((join, connector));
            }
            let mut inner_query = logical_plan_to_ast(&outer_project.input, dialect)?;
            match inner_query.0.body.borrow_mut() {
                SetExpr::Select(select) => {
                    select.projection = outer_project
                        .expr
                        .iter()
                        .map(|e| expr_to_select_item(e, dialect))
                        .collect()
                }
                SetExpr::Query(query) => {
                    // I know that the projection > tablescan only projects because
                    // the tablescan didn't subsume the projection.
                    // So the query here could have some redundant projections.
                    // Some of the projection values might be new columns generated through
                    // calculations, so how would I know?
                    panic!("Query: {:#?}", query.to_string())
                }
                SetExpr::SetOperation { .. } => todo!(),
                SetExpr::Values(_) => todo!(),
                SetExpr::Insert(_) => todo!(),
            }
            let body = SetExpr::Query(Box::new(inner_query.0));
            (
                Query {
                    with: None,
                    body,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    fetch: None,
                    lock: None,
                },
                inner_query.1,
            )
        }
        LogicalPlan::Filter(filter) => {
            // A filter can loosely be expressed as "select * from input where {predicate}"
            // While this nests a table select, we ordinarily expect to generate SQL code
            // from an already optimised LogicalPlan, so this should not be a common occurrence
            let (mut query, connector) = logical_plan_to_ast(&filter.input, dialect)?;
            match query.body.borrow_mut() {
                SetExpr::Select(select) => {
                    select.selection = Some(expr_to_ast(&filter.predicate, dialect))
                }
                _ => {
                    return Err(RdbmsError::UnsupportedQuery(
                        "Filter not fully supported".to_string(),
                    ))
                }
            }
            return Ok((query, connector));
        }
        LogicalPlan::Window(_) => todo!(),
        LogicalPlan::Aggregate(aggregate) => {
            let (mut input, connector) = logical_plan_to_ast(&aggregate.input, dialect)?;
            // Edit the query body
            let projection = aggregate
                .aggr_expr
                .iter()
                .map(|expr| expr_to_select_item(expr, dialect))
                .collect::<Vec<_>>();
            let group_by = aggregate
                .group_expr
                .iter()
                .map(|expr| expr_to_ast(expr, dialect))
                .collect::<Vec<_>>();
            match input.body.borrow_mut() {
                SetExpr::Select(select) => {
                    // TODO: check if there is already a group_by
                    select.group_by = group_by;
                    select.projection = projection;
                }
                SetExpr::Query(query) => {
                    println!("Query: {:#?}", query);
                    println!("Query string: {query}");
                    return Err(RdbmsError::UnsupportedQuery(
                        "Aggregates are not fully supported".to_string(),
                    ));
                }
                SetExpr::SetOperation { .. } => todo!(),
                SetExpr::Values(_) => todo!(),
                SetExpr::Insert(_) => todo!(),
            };
            (input, connector)
        }
        LogicalPlan::Sort(sort) => {
            let (mut input, connector) = logical_plan_to_ast(&sort.input, dialect)?;
            let sort_exprs = sort
                .expr
                .iter()
                .map(|expr| match expr {
                    datafusion::logical_plan::Expr::Sort {
                        expr,
                        asc,
                        nulls_first,
                    } => Ok(OrderByExpr {
                        expr: expr_to_ast(expr, dialect),
                        asc: Some(*asc),
                        nulls_first: Some(*nulls_first),
                    }),
                    _ => Err(crate::error::RdbmsError::UnsupportedType(format!(
                        "Illegal expression in sort: {}",
                        expr
                    ))
                    .into()),
                })
                .collect::<DfResult<Vec<_>>>();
            input.order_by = sort_exprs.unwrap();
            (input, connector)
        }
        LogicalPlan::Join(join) => {
            let join_operator = join_factor_to_ast(join, dialect)?;
            let (mut left, connector) = logical_plan_to_ast(&join.left, dialect)?;
            let (right, _) = logical_plan_to_ast(&join.right, dialect)?;
            let body = match (left.body.borrow_mut(), right.body) {
                (SetExpr::Select(left_select), SetExpr::Select(right_select)) => {
                    let mut from = left_select.from[0].clone();
                    from.joins.push(Join {
                        relation: right_select.from[0].clone().relation,
                        join_operator,
                    });
                    left_select.from[0] = from;
                    // Add columns from right
                    // TODO: this could be simplified to a Wildcard
                    left_select
                        .projection
                        .extend_from_slice(&right_select.projection);
                    left_select.clone()
                }
                // SetExpr::Query(_) => todo!(),
                // SetExpr::SetOperation {
                //     op,
                //     all,
                //     left,
                //     right,
                // } => todo!(),
                // SetExpr::Values(_) => todo!(),
                // SetExpr::Insert(_) => todo!(),
                _ => {
                    todo!("non-select joins not implemented")
                }
            };
            (
                Query {
                    with: None,
                    body: SetExpr::Select(body),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    fetch: None,
                    lock: None,
                },
                connector,
            )
        }
        LogicalPlan::CrossJoin(cross_join) => {
            let (mut left, connector) = logical_plan_to_ast(&cross_join.left, dialect)?;
            let (right, _) = logical_plan_to_ast(&cross_join.right, dialect)?;
            let body = match (left.body.borrow_mut(), right.body) {
                (SetExpr::Select(left_select), SetExpr::Select(right_select)) => {
                    let mut from = left_select.from[0].clone();
                    from.joins.push(Join {
                        relation: right_select.from[0].clone().relation,
                        join_operator: sqlparser::ast::JoinOperator::CrossJoin,
                    });
                    left_select.from[0] = from;
                    // Add columns from right
                    // TODO: this could be simplified to a Wildcard
                    left_select
                        .projection
                        .extend_from_slice(&right_select.projection);
                    left_select.clone()
                }
                // SetExpr::Query(_) => todo!(),
                // SetExpr::SetOperation {
                //     op,
                //     all,
                //     left,
                //     right,
                // } => todo!(),
                // SetExpr::Values(_) => todo!(),
                // SetExpr::Insert(_) => todo!(),
                _ => {
                    todo!("non-select joins not implemented")
                }
            };
            (
                Query {
                    with: None,
                    body: SetExpr::Select(body),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    fetch: None,
                    lock: None,
                },
                connector,
            )
        }
        LogicalPlan::Repartition(_) => todo!(),
        LogicalPlan::Union(_) => todo!(),
        LogicalPlan::TableScan(scan) => {
            // Table scans only make sense when dealing with an expected data source.
            // An expected source would first be a RDBMS, and have the same dialect as
            // the input variable.
            let connector = match scan.source.origin() {
                TableOrigin::Relational { .. } => {
                    // TODO: should we downcast the source to find its dialect?
                    // Trait downcasting would be useful here
                    let source = scan
                        .source
                        .as_any()
                        .downcast_ref::<DefaultTableSource>()
                        .unwrap();
                    let source = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<PostgresTableProvider>()
                        .unwrap();
                    source.connection().to_connector()
                }
                _ => {
                    return Err(RdbmsError::UnsupportedType(
                        "Cannot generate AST for a non-relational table scan".to_owned(),
                    ));
                }
            };
            let projection = if scan.projection.is_none() {
                vec![SelectItem::Wildcard]
            } else {
                scan.projected_schema
                    .fields()
                    .iter()
                    .map(|field| {
                        // TODO handle aliases
                        SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                            value: field.name().clone(),
                            quote_style: None,
                        }))
                    })
                    .collect()
            };
            // TODO: complete the selection
            // let selection: Option<()> = if scan.filters.is_empty() {
            //     None
            // } else {
            //     let mut filter_iter = scan.filters.iter();
            //     Some(scan.filters.iter().map(|filter| {}));
            //     todo!();
            // };
            (
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
                                        .split('.')
                                        .map(|s| Ident {
                                            value: s.to_string(),
                                            quote_style: None,
                                        })
                                        .collect(),
                                ),
                                alias: None,
                                args: None,
                                with_hints: vec![],
                            },
                            joins: vec![],
                        }],
                        into: None,
                        qualify: None,
                        lateral_views: vec![],
                        selection: None,
                        group_by: vec![],
                        cluster_by: vec![],
                        distribute_by: vec![],
                        sort_by: vec![],
                        having: None,
                    })),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    fetch: None,
                    lock: None,
                },
                connector,
            )
        }
        LogicalPlan::EmptyRelation(_) => todo!(),
        LogicalPlan::Limit(limit) => {
            let (mut input, connector) = logical_plan_to_ast(&limit.input, dialect)?;
            input.limit = limit
                .fetch
                .map(|fetch| Expr::Value(Value::Number(fetch.to_string(), false)));
            input.offset = limit.skip.map(|skip| Offset {
                value: Expr::Value(Value::Number(skip.to_string(), false)),
                rows: OffsetRows::None,
            });
            (input, connector)
        }
        LogicalPlan::CreateExternalTable(_) => todo!(),
        LogicalPlan::CreateMemoryTable(_) => todo!(),
        LogicalPlan::DropTable(_) => todo!(),
        LogicalPlan::Values(_) => todo!(),
        LogicalPlan::Explain(_) => todo!(),
        LogicalPlan::Analyze(_) => todo!(),
        LogicalPlan::Extension(extension) => {
            if let Some(node) = extension.node.as_any().downcast_ref::<SqlAstPlanNode>() {
                (node.ast.clone(), node.connector.clone())
            } else {
                panic!()
            }
        }
        LogicalPlan::CreateCatalogSchema(_) => todo!(),
        LogicalPlan::SubqueryAlias(subquery_alias) => {
            // Table with alias
            let (mut query, connector) = logical_plan_to_ast(&subquery_alias.input, dialect)?;
            match query.body.borrow_mut() {
                SetExpr::Select(select) => match select.from.get_mut(0) {
                    Some(from) => match from.relation.borrow_mut() {
                        TableFactor::Table { alias, .. } => {
                            *alias = Some(TableAlias {
                                name: Ident {
                                    value: subquery_alias.alias.clone(),
                                    quote_style: None,
                                },
                                columns: vec![],
                            })
                        }
                        TableFactor::Derived { .. } => todo!(),
                        TableFactor::TableFunction { .. } => todo!(),
                        TableFactor::NestedJoin(_) => todo!(),
                        TableFactor::UNNEST { .. } => todo!(),
                    },
                    None => panic!(),
                },
                _ => panic!(),
            }
            return Ok((query, connector));
        }
        LogicalPlan::CreateCatalog(_) => todo!(),
        LogicalPlan::Subquery(_) => todo!(),
        LogicalPlan::CreateView(_) => todo!(),
    })
}

/// Create a [sqlparser::ast::SelectItem] used for projection
fn expr_to_select_item(expr: &Expr, dialect: DatabaseDialect) -> SelectItem {
    match expr {
        Expr::Alias(expr, name) => SelectItem::ExprWithAlias {
            expr: expr_to_ast(expr, dialect),
            alias: Ident {
                value: name.to_owned(),
                quote_style: None,
            },
        },
        Expr::Wildcard => SelectItem::Wildcard,
        Expr::QualifiedWildcard { qualifier } => SelectItem::QualifiedWildcard(ObjectName(
            qualifier
                .split('.')
                .map(|ident| Ident {
                    value: ident.to_string(),
                    quote_style: None,
                })
                .collect(),
        )),
        _ => SelectItem::UnnamedExpr(expr_to_ast(expr, dialect)),
    }
}

/// Convert an expression back to AST
fn expr_to_ast(expr: &Expr, dialect: DatabaseDialect) -> sqlparser::ast::Expr {
    use datafusion::scalar::ScalarValue;
    use sqlparser::ast::Expr as OutExpr;
    use sqlparser::ast::{
        BinaryOperator, Function, FunctionArg, FunctionArgExpr, UnaryOperator, Value,
    };
    match expr {
        Expr::Alias(expr, _) => {
            // TODO: alias can be function expr (e.g. bench.public.lineitem.l_extendedprice * Int64(1) - bench.public.lineitem.l_discount)
            // We ignore that for now
            expr_to_ast(expr, dialect)
        }
        Expr::Column(col) => OutExpr::Identifier(Ident {
            value: col.name.clone(),
            quote_style: None,
        }),
        Expr::ScalarVariable(_, _) => todo!(),
        // Date32 gets written as the i32 number, which won't work on SQL DBs
        Expr::Literal(lit) => {
            if lit.is_null() {
                return OutExpr::Value(Value::Null);
            }
            match lit {
                ScalarValue::Boolean(Some(value)) => OutExpr::Value(Value::Boolean(*value)),
                ScalarValue::Float32(Some(value)) => {
                    OutExpr::Value(Value::Number(value.to_string(), false))
                }
                ScalarValue::Float64(Some(value)) => {
                    OutExpr::Value(Value::Number(value.to_string(), false))
                }
                ScalarValue::Decimal128(_, _, _) => todo!(),
                ScalarValue::Int8(Some(value)) => {
                    OutExpr::Value(Value::Number(value.to_string(), false))
                }
                ScalarValue::Int16(Some(value)) => {
                    OutExpr::Value(Value::Number(value.to_string(), false))
                }
                ScalarValue::Int32(Some(value)) => {
                    OutExpr::Value(Value::Number(value.to_string(), false))
                }
                ScalarValue::Int64(Some(value)) => {
                    OutExpr::Value(Value::Number(value.to_string(), true))
                }
                ScalarValue::UInt8(Some(value)) => {
                    OutExpr::Value(Value::Number(value.to_string(), false))
                }
                ScalarValue::UInt16(Some(value)) => {
                    OutExpr::Value(Value::Number(value.to_string(), false))
                }
                ScalarValue::UInt32(Some(value)) => {
                    OutExpr::Value(Value::Number(value.to_string(), false))
                }
                ScalarValue::UInt64(Some(value)) => {
                    OutExpr::Value(Value::Number(value.to_string(), true))
                }
                ScalarValue::Utf8(Some(value)) => {
                    OutExpr::Value(Value::SingleQuotedString(value.to_owned()))
                }
                ScalarValue::LargeUtf8(Some(value)) => {
                    OutExpr::Value(Value::SingleQuotedString(value.to_owned()))
                }
                ScalarValue::Binary(_) => todo!(),
                ScalarValue::LargeBinary(_) => todo!(),
                ScalarValue::List(_, _) => todo!(),
                ScalarValue::Date32(_) => todo!(),
                ScalarValue::Date64(_) => todo!(),
                ScalarValue::TimestampSecond(_, _) => todo!(),
                ScalarValue::TimestampMillisecond(_, _) => todo!(),
                ScalarValue::TimestampMicrosecond(_, _) => todo!(),
                ScalarValue::TimestampNanosecond(_, _) => todo!(),
                ScalarValue::IntervalYearMonth(Some(value)) => {
                    OutExpr::Value(Value::SingleQuotedString(value.to_string()))
                }
                ScalarValue::IntervalDayTime(_) => todo!(),
                ScalarValue::IntervalMonthDayNano(_) => todo!(),
                ScalarValue::Struct(_, _) => todo!(),
                _ => todo!(),
            }
        }
        Expr::BinaryExpr { left, op, right } => {
            // TODO: expand into a match statement if some ops don't translate to SQL
            let op = match op {
                Operator::Eq => BinaryOperator::Eq,
                Operator::NotEq => BinaryOperator::NotEq,
                Operator::Lt => BinaryOperator::Lt,
                Operator::LtEq => BinaryOperator::LtEq,
                Operator::Gt => BinaryOperator::Gt,
                Operator::GtEq => BinaryOperator::GtEq,
                Operator::Plus => BinaryOperator::Plus,
                Operator::Minus => BinaryOperator::Minus,
                Operator::Multiply => BinaryOperator::Multiply,
                Operator::Divide => BinaryOperator::Divide,
                Operator::Modulo => BinaryOperator::Modulo,
                Operator::And => BinaryOperator::And,
                Operator::Or => BinaryOperator::Or,
                Operator::Like => BinaryOperator::Like,
                Operator::NotLike => BinaryOperator::NotLike,
                Operator::IsDistinctFrom => todo!(),
                Operator::IsNotDistinctFrom => todo!(),
                // TODO disable based on dialect?
                Operator::RegexMatch => BinaryOperator::PGRegexMatch,
                Operator::RegexIMatch => BinaryOperator::PGRegexIMatch,
                Operator::RegexNotMatch => BinaryOperator::PGRegexNotMatch,
                Operator::RegexNotIMatch => BinaryOperator::PGRegexNotIMatch,
                Operator::BitwiseAnd => BinaryOperator::BitwiseAnd,
                Operator::BitwiseOr => BinaryOperator::BitwiseOr,
                Operator::StringConcat => BinaryOperator::StringConcat,
            };
            OutExpr::BinaryOp {
                left: Box::new(expr_to_ast(left, dialect)),
                op,
                right: Box::new(expr_to_ast(right, dialect)),
            }
        }
        Expr::Not(expr) => OutExpr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(expr_to_ast(expr, dialect)),
        },
        Expr::IsNotNull(expr) => OutExpr::IsNotNull(Box::new(expr_to_ast(expr, dialect))),
        Expr::IsNull(expr) => OutExpr::IsNull(Box::new(expr_to_ast(expr, dialect))),
        Expr::Negative(expr) => {
            todo!("-({})", expr)
        }
        Expr::GetIndexedField { .. } => todo!(),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => OutExpr::Between {
            expr: Box::new(expr_to_ast(expr, dialect)),
            negated: *negated,
            low: Box::new(expr_to_ast(low, dialect)),
            high: Box::new(expr_to_ast(high, dialect)),
        },
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => OutExpr::Case {
            operand: expr
                .as_ref()
                .map(|expr| Box::new(expr_to_ast(expr, dialect))),
            conditions: when_then_expr
                .iter()
                .map(|(expr, _)| expr_to_ast(expr, dialect))
                .collect(),
            results: when_then_expr
                .iter()
                .map(|(_, expr)| expr_to_ast(expr, dialect))
                .collect(),
            else_result: else_expr
                .as_ref()
                .map(|expr| Box::new(expr_to_ast(expr, dialect))),
        },
        Expr::Cast { expr, data_type } => OutExpr::Cast {
            expr: Box::new(expr_to_ast(expr, dialect)),
            data_type: datatype_to_ast(data_type, dialect),
        },
        Expr::TryCast { .. } => todo!(),
        Expr::Sort { .. } => {
            panic!("Sort not supported as expression, handled in logical plan")
        }
        Expr::ScalarFunction { fun, args } => match fun {
            BuiltinScalarFunction::Abs => todo!(),
            BuiltinScalarFunction::Acos => todo!(),
            BuiltinScalarFunction::Asin => todo!(),
            BuiltinScalarFunction::Atan => todo!(),
            BuiltinScalarFunction::Ceil => todo!(),
            BuiltinScalarFunction::Coalesce => todo!(),
            BuiltinScalarFunction::Cos => todo!(),
            BuiltinScalarFunction::Digest => todo!(),
            BuiltinScalarFunction::Exp => todo!(),
            BuiltinScalarFunction::Floor => todo!(),
            BuiltinScalarFunction::Ln => todo!(),
            BuiltinScalarFunction::Log => todo!(),
            BuiltinScalarFunction::Log10 => todo!(),
            BuiltinScalarFunction::Log2 => todo!(),
            BuiltinScalarFunction::Power => todo!(),
            BuiltinScalarFunction::Round => todo!(),
            BuiltinScalarFunction::Signum => todo!(),
            BuiltinScalarFunction::Sin => todo!(),
            BuiltinScalarFunction::Sqrt => todo!(),
            BuiltinScalarFunction::Tan => todo!(),
            BuiltinScalarFunction::Trunc => todo!(),
            BuiltinScalarFunction::Array => todo!(),
            BuiltinScalarFunction::Ascii => todo!(),
            BuiltinScalarFunction::BitLength => todo!(),
            BuiltinScalarFunction::Btrim => todo!(),
            BuiltinScalarFunction::CharacterLength => todo!(),
            BuiltinScalarFunction::Chr => todo!(),
            BuiltinScalarFunction::Concat => todo!(),
            BuiltinScalarFunction::ConcatWithSeparator => todo!(),
            BuiltinScalarFunction::DatePart => OutExpr::Extract {
                field: extract_datetime(&args[0]),
                expr: Box::new(expr_to_ast(&args[1], dialect)),
            },
            BuiltinScalarFunction::DateTrunc => todo!(),
            BuiltinScalarFunction::InitCap => todo!(),
            BuiltinScalarFunction::Left => todo!(),
            BuiltinScalarFunction::Lpad => todo!(),
            BuiltinScalarFunction::Lower => todo!(),
            BuiltinScalarFunction::Ltrim => todo!(),
            BuiltinScalarFunction::MD5 => todo!(),
            BuiltinScalarFunction::NullIf => todo!(),
            BuiltinScalarFunction::OctetLength => todo!(),
            BuiltinScalarFunction::Random => todo!(),
            BuiltinScalarFunction::RegexpReplace => todo!(),
            BuiltinScalarFunction::Repeat => todo!(),
            BuiltinScalarFunction::Replace => todo!(),
            BuiltinScalarFunction::Reverse => todo!(),
            BuiltinScalarFunction::Right => todo!(),
            BuiltinScalarFunction::Rpad => todo!(),
            BuiltinScalarFunction::Rtrim => todo!(),
            BuiltinScalarFunction::SHA224 => todo!(),
            BuiltinScalarFunction::SHA256 => todo!(),
            BuiltinScalarFunction::SHA384 => todo!(),
            BuiltinScalarFunction::SHA512 => todo!(),
            BuiltinScalarFunction::SplitPart => todo!(),
            BuiltinScalarFunction::StartsWith => todo!(),
            BuiltinScalarFunction::Strpos => todo!(),
            BuiltinScalarFunction::Substr => OutExpr::Substring {
                expr: Box::new(expr_to_ast(&args[0], dialect)),
                substring_from: Some(Box::new(expr_to_ast(&args[1], dialect))),
                substring_for: Some(Box::new(expr_to_ast(&args[2], dialect))),
            },
            BuiltinScalarFunction::ToHex => todo!(),
            BuiltinScalarFunction::ToTimestamp => todo!(),
            BuiltinScalarFunction::ToTimestampMillis => todo!(),
            BuiltinScalarFunction::ToTimestampMicros => todo!(),
            BuiltinScalarFunction::ToTimestampSeconds => todo!(),
            BuiltinScalarFunction::Now => todo!(),
            BuiltinScalarFunction::Translate => todo!(),
            BuiltinScalarFunction::Trim => todo!(),
            BuiltinScalarFunction::Upper => todo!(),
            BuiltinScalarFunction::RegexpMatch => todo!(),
            BuiltinScalarFunction::Struct => todo!(),
        },
        Expr::ScalarUDF { .. } => todo!(),
        Expr::AggregateFunction {
            fun,
            args,
            distinct,
        } => match fun {
            AggregateFunction::Count => OutExpr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: "count".to_string(),
                    quote_style: None,
                }]),
                args: args
                    .iter()
                    .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr_to_ast(e, dialect))))
                    .collect(),
                over: None,
                distinct: *distinct,
            }),
            AggregateFunction::Sum => OutExpr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: "sum".to_string(),
                    quote_style: None,
                }]),
                args: args
                    .iter()
                    .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr_to_ast(e, dialect))))
                    .collect(),
                over: None,
                distinct: *distinct,
            }),
            AggregateFunction::Min => OutExpr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: "min".to_string(),
                    quote_style: None,
                }]),
                args: args
                    .iter()
                    .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr_to_ast(e, dialect))))
                    .collect(),
                over: None,
                distinct: *distinct,
            }),
            AggregateFunction::Max => OutExpr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: "max".to_string(),
                    quote_style: None,
                }]),
                args: args
                    .iter()
                    .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr_to_ast(e, dialect))))
                    .collect(),
                over: None,
                distinct: *distinct,
            }),
            AggregateFunction::Avg => OutExpr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: "avg".to_string(),
                    quote_style: None,
                }]),
                args: args
                    .iter()
                    .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr_to_ast(e, dialect))))
                    .collect(),
                over: None,
                distinct: *distinct,
            }),
            AggregateFunction::ApproxDistinct => todo!(),
            AggregateFunction::ArrayAgg => todo!(),
            AggregateFunction::Variance => todo!(),
            AggregateFunction::VariancePop => todo!(),
            AggregateFunction::Stddev => todo!(),
            AggregateFunction::StddevPop => todo!(),
            AggregateFunction::Covariance => todo!(),
            AggregateFunction::CovariancePop => todo!(),
            AggregateFunction::Correlation => todo!(),
            AggregateFunction::ApproxPercentileCont => todo!(),
            AggregateFunction::ApproxPercentileContWithWeight => todo!(),
            AggregateFunction::ApproxMedian => todo!(),
            AggregateFunction::Grouping => todo!(),
        },
        Expr::WindowFunction { .. } => todo!(),
        Expr::AggregateUDF { .. } => todo!(),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let list = list
                .iter()
                .map(|expr| expr_to_ast(expr, dialect))
                .collect::<Vec<_>>();
            OutExpr::InList {
                expr: Box::new(expr_to_ast(expr, dialect)),
                list,
                negated: *negated,
            }
        }
        Expr::Wildcard => todo!(),
        Expr::QualifiedWildcard { .. } => todo!(),
        Expr::Exists { subquery, negated } => {
            let (subquery, _) = logical_plan_to_ast(&subquery.subquery, dialect).unwrap();
            let exists = OutExpr::Exists(Box::new(subquery));
            if *negated {
                OutExpr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(exists),
                }
            } else {
                exists
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let (subquery, _) = logical_plan_to_ast(&subquery.subquery, dialect).unwrap();
            OutExpr::InSubquery {
                expr: Box::new(expr_to_ast(expr, dialect)),
                subquery: Box::new(subquery),
                negated: *negated,
            }
        }
        Expr::ScalarSubquery(subquery) => {
            let (subquery, _) = logical_plan_to_ast(&subquery.subquery, dialect).unwrap();
            OutExpr::Subquery(Box::new(subquery))
        }
        Expr::GroupingSet(_) => todo!(),
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
            ScalarValue::Int64(Some(value)) => value.to_string(),
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
        Expr::ScalarFunction { fun, args } => todo!("Fun: {:?}, args: {:?}", fun, args),
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
        Expr::Exists { .. } => todo!(),
        Expr::InSubquery { .. } => todo!(),
        Expr::ScalarSubquery(..) => {
            todo!()
        }
        Expr::GroupingSet(_) => todo!(),
    }
}

fn expr_inner_name(expr: &Expr) -> String {
    match expr {
        Expr::Alias(expr, _) => expr_inner_name(expr),
        Expr::Column(column) => column.name.clone(),
        Expr::ScalarVariable(_, _) => todo!(),
        Expr::Literal(lit) => lit.to_string(), // TODO
        Expr::BinaryExpr { left, op, right } => {
            dbg!(left);
            dbg!(op);
            dbg!(right);

            panic!();
        }
        Expr::Not(_) => todo!(),
        Expr::IsNotNull(_) => todo!(),
        Expr::IsNull(_) => todo!(),
        Expr::Negative(_) => todo!(),
        Expr::GetIndexedField { .. } => todo!(),
        Expr::Between { .. } => todo!(),
        Expr::Case { .. } => todo!(),
        Expr::Cast { .. } => todo!(),
        Expr::TryCast { .. } => todo!(),
        Expr::Sort { .. } => todo!(),
        Expr::ScalarFunction { .. } => todo!(),
        Expr::ScalarUDF { .. } => todo!(),
        Expr::AggregateFunction {
            fun,
            args,
            distinct,
        } => {
            dbg!(fun, args, distinct);
            panic!()
        }
        Expr::WindowFunction { .. } => todo!(),
        Expr::AggregateUDF { .. } => todo!(),
        Expr::InList { .. } => todo!(),
        Expr::Wildcard => todo!(),
        Expr::QualifiedWildcard { .. } => todo!(),
        Expr::Exists { .. } => todo!(),
        Expr::InSubquery { .. } => todo!(),
        Expr::ScalarSubquery(_) => todo!(),
        Expr::GroupingSet(_) => todo!(),
    }
}

fn datatype_to_ast(data_type: &DataType, _dialect: DatabaseDialect) -> sqlparser::ast::DataType {
    use sqlparser::ast::DataType as DT;
    match data_type {
        DataType::Boolean => DT::Boolean,
        DataType::Int8 => DT::Int(None),
        DataType::Date32 => DT::Date,
        _ => todo!("Casting {:?} is not yet implemented", data_type),
    }
}

fn join_factor_to_ast(
    join: &plan::Join,
    dialect: DatabaseDialect,
) -> Result<sqlparser::ast::JoinOperator, RdbmsError> {
    use sqlparser::ast::{JoinConstraint as Constraint, JoinOperator};

    // TODO: check if join columns aren't empty
    let mut join_expr = {
        let (left, right) = &join.on[0];
        col(left.name.as_str()).eq(col(right.name.as_str()))
    };

    for (left, right) in join.on.iter().skip(1) {
        join_expr = join_expr.and(col(left.name.as_str()).eq(col(right.name.as_str())));
    }

    let join_expr = expr_to_ast(&join_expr, dialect);

    let join_constraint = match join.join_constraint {
        JoinConstraint::On => Constraint::On(join_expr),
        JoinConstraint::Using => todo!("JoinConstraint::Using not yet supported"),
    };

    Ok(match join.join_type {
        JoinType::Inner => JoinOperator::Inner(join_constraint),
        JoinType::Left => JoinOperator::LeftOuter(join_constraint),
        JoinType::Right => JoinOperator::RightOuter(join_constraint),
        JoinType::Full => JoinOperator::FullOuter(join_constraint),
        JoinType::Semi => {
            return Err(RdbmsError::UnsupportedQuery(
                "Semi-join not yet supported".to_string(),
            ))
        }
        JoinType::Anti => {
            return Err(RdbmsError::UnsupportedQuery(
                "Anti-join not yet supported".to_string(),
            ))
        }
    })
}

fn extract_datetime(expr: &Expr) -> sqlparser::ast::DateTimeField {
    use sqlparser::ast::DateTimeField;
    match expr {
        // Expr::Alias(_, _) => todo!(),
        // Expr::Column(_) => todo!(),
        // Expr::ScalarVariable(_, _) => todo!(),
        Expr::Literal(datafusion::scalar::ScalarValue::Utf8(Some(value))) => match value.as_str() {
            "YEAR" => DateTimeField::Year,
            t => panic!("{}", t),
        },
        Expr::Literal(_) => todo!(),
        t => panic!("Cant extract date from {:?}", t),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::datasource::datasource::TableProviderFilterPushDown;
    use datafusion::error::Result as DfResult;
    use datafusion::execution::context::SessionState;
    use datafusion::logical_expr::TableType;
    use datafusion::{
        arrow::datatypes::SchemaRef, datasource::TableProvider, physical_plan::ExecutionPlan,
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

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _ctx: &SessionState,
            _projection: &Option<Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DfResult<Arc<dyn ExecutionPlan>> {
            unimplemented!("We do not test scans")
        }
    }

    #[test]
    fn test_ast() {
        let query = "select * from a where b in (1,2,3)";
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let out = sqlparser::parser::Parser::parse_sql(&dialect, query).unwrap();
        println!("AST: {:#?}", out);
    }

    #[test]
    fn test_join() {
        let query = "
        select a.one, a.two 
        from table_name a
        ";
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let out = sqlparser::parser::Parser::parse_sql(&dialect, query).unwrap();
        println!("AST: {:#?}", out);
    }
}
