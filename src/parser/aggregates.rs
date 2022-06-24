use std::{collections::HashMap, sync::Arc};

use datafusion::logical_plan::{Expr, plan::Aggregate, DFSchema};


///
pub fn collect_aggregates(
    aggregate: &Aggregate,
    inner_project_expr: &HashMap<String, Expr>,
    inner_project_schema: Option<&Arc<DFSchema>>,
) -> HashMap<String, Expr> {
    aggregate.aggr_expr.iter().map(|expr| {
        let name = expr.name(&aggregate.schema).unwrap();
        let agg_expr = if let Expr::AggregateFunction { fun, args, distinct } = expr {
            if args.len() != 1 {
                todo!("Aggregate functions with != expression not yet supported")
            }
            let arg1 = &args[0];
            match arg1 {
                Expr::Alias(_, alias) => {
                    // Get the original expression
                    let inner_expr = inner_project_expr.get(alias).expect("Expr1 not found");
                    // Rewrite the aggregate to be over the expression, not a column
                    Expr::AggregateFunction { fun: fun.clone(), args: vec![inner_expr.clone()], distinct: *distinct }
                },
                Expr::Column(_) | Expr::Literal(_) => {
                    expr.clone()
                },
                Expr::BinaryExpr { left, op, right } => {
                    // Left or Right is an alias (or both)
                    let left: Expr = match &**left {
                        Expr::Alias(aliased_expr, _) => {
                            if let Expr::Column(_) = &**aliased_expr {
                                // Find the column on the inner projection
                                match inner_project_schema {
                                    Some(schema) => {
                                        let col_name = aliased_expr.name(schema).expect("Expr not found");
                                        let found = inner_project_expr.get(&col_name).expect("Col not found");
                                        found.clone()
                                    },
                                    None => {
                                        arg1.clone()
                                    },
                                }
                            } else {
                                *aliased_expr.clone()

                            }
                        }
                        _ => *left.clone(),
                    };
                    // Check left and right, are any of them column names?
                    let right = if let Expr::Alias(aliased_expr, _) = &**right {
                        aliased_expr.clone()
                    } else {
                        right.clone()
                    };
                    let expr = Expr::BinaryExpr { left: Box::new(left), op: *op, right };
                    // Get the original expression
                    Expr::AggregateFunction { fun: fun.clone(), args: vec![expr], distinct: *distinct }
                }
                Expr::Case { .. } => {
                    expr.clone()
                }
                e => todo!("{e} of variant {}", e.variant_name())
            }
        } else {
            expr.clone()
        };

        (name, agg_expr)
    }).collect::<HashMap<_, _>>()
}