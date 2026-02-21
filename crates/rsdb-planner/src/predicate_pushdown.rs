use rsdb_common::Result;
use rsdb_sql::expr::{BinaryOperator, Expr};
use rsdb_sql::logical_plan::{JoinCondition, JoinType, LogicalPlan};

/// Predicate Pushdown Rule
///
/// This rule pushes filters down through the plan and converts CrossJoins to InnerJoins.
pub struct PredicatePushdown;

impl PredicatePushdown {
    pub fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut optimizer = Self;
        optimizer.push_down(&plan)
    }

    fn push_down(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                // First, optimize the input
                let input = self.push_down(input)?;
                
                // Now try to push the predicate into the optimized input
                self.push_predicate(input, predicate.clone())
            }
            // For other nodes, just recurse
            LogicalPlan::Project { input, expr, schema } => {
                Ok(LogicalPlan::Project {
                    input: Box::new(self.push_down(input)?),
                    expr: expr.clone(),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Aggregate { input, group_expr, aggregate_expr, schema } => {
                Ok(LogicalPlan::Aggregate {
                    input: Box::new(self.push_down(input)?),
                    group_expr: group_expr.clone(),
                    aggregate_expr: aggregate_expr.clone(),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Sort { input, expr } => {
                Ok(LogicalPlan::Sort {
                    input: Box::new(self.push_down(input)?),
                    expr: expr.clone(),
                })
            }
            LogicalPlan::Limit { input, limit, offset } => {
                Ok(LogicalPlan::Limit {
                    input: Box::new(self.push_down(input)?),
                    limit: *limit,
                    offset: *offset,
                })
            }
            LogicalPlan::Join { left, right, join_type, join_condition, schema } => {
                Ok(LogicalPlan::Join {
                    left: Box::new(self.push_down(left)?),
                    right: Box::new(self.push_down(right)?),
                    join_type: *join_type,
                    join_condition: join_condition.clone(),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::CrossJoin { left, right, schema } => {
                Ok(LogicalPlan::CrossJoin {
                    left: Box::new(self.push_down(left)?),
                    right: Box::new(self.push_down(right)?),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::SubqueryAlias { .. } => {
                unreachable!("SubqueryAlias should have been eliminated by EliminateSubqueryAlias rule");
            }
            LogicalPlan::Explain { input } => {
                Ok(LogicalPlan::Explain {
                    input: Box::new(self.push_down(input)?),
                })
            }
            // Leaf nodes or nodes that block pushdown
            _ => Ok(plan.clone()),
        }
    }

    fn push_predicate(&self, plan: LogicalPlan, predicate: Expr) -> Result<LogicalPlan> {
        // Split predicate into conjunctions (ANDs)
        let predicates = self.split_conjunction(&predicate);
        
        // Recursively push these predicates
        self.push_predicates_recursive(plan, predicates)
    }

    fn split_conjunction(&self, predicate: &Expr) -> Vec<Expr> {
        let mut exprs = vec![];
        let mut stack = vec![predicate];
        while let Some(expr) = stack.pop() {
            match expr {
                Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                    stack.push(right);
                    stack.push(left);
                }
                _ => exprs.push(expr.clone()),
            }
        }
        exprs
    }

    fn push_predicates_recursive(&self, plan: LogicalPlan, predicates: Vec<Expr>) -> Result<LogicalPlan> {
        if predicates.is_empty() {
            return Ok(plan);
        }

        match plan {
            LogicalPlan::CrossJoin { left, right, schema } => {
                // Check which predicates reference only left, only right, or both
                let left_schema = left.schema();
                let right_schema = right.schema();
                
                let mut left_preds = vec![];
                let mut right_preds = vec![];
                let mut join_preds = vec![];
                let mut other_preds = vec![];

                for pred in predicates {
                    let cols = pred.get_columns();
                    let references_left = cols.iter().any(|c| left_schema.field_with_name(c).is_ok());
                    let references_right = cols.iter().any(|c| right_schema.field_with_name(c).is_ok());

                    if references_left && !references_right {
                        left_preds.push(pred);
                    } else if !references_left && references_right {
                        right_preds.push(pred);
                    } else if references_left && references_right {
                        join_preds.push(pred);
                    } else {
                        // Constant expression or references unknown columns? Keep it here.
                        other_preds.push(pred);
                    }
                }

                // Push down to children
                let new_left = self.push_predicates_recursive(*left, left_preds)?;
                let new_right = self.push_predicates_recursive(*right, right_preds)?;

                // Convert CrossJoin to InnerJoin if we have join predicates
                if !join_preds.is_empty() {
                    // Combine join predicates into one expression
                    let join_expr = self.combine_conjunction(join_preds);
                    
                    let join_plan = LogicalPlan::Join {
                        left: Box::new(new_left),
                        right: Box::new(new_right),
                        join_type: JoinType::Inner,
                        join_condition: JoinCondition::On(join_expr),
                        schema,
                    };

                    // If there are other predicates remaining, wrap in Filter
                    if !other_preds.is_empty() {
                        Ok(LogicalPlan::Filter {
                            input: Box::new(join_plan),
                            predicate: self.combine_conjunction(other_preds),
                        })
                    } else {
                        Ok(join_plan)
                    }
                } else {
                    // Still CrossJoin, but children might be filtered
                    let cross = LogicalPlan::CrossJoin {
                        left: Box::new(new_left),
                        right: Box::new(new_right),
                        schema,
                    };
                    
                    if !other_preds.is_empty() {
                        Ok(LogicalPlan::Filter {
                            input: Box::new(cross),
                            predicate: self.combine_conjunction(other_preds),
                        })
                    } else {
                        Ok(cross)
                    }
                }
            }
            LogicalPlan::Filter { input, predicate } => {
                // Merge with existing predicates
                let mut all_preds = predicates;
                all_preds.extend(self.split_conjunction(&predicate));
                self.push_predicates_recursive(*input, all_preds)
            }
            LogicalPlan::SubqueryAlias { .. } => {
                unreachable!("SubqueryAlias should have been eliminated by EliminateSubqueryAlias rule");
            }
            LogicalPlan::Project { input, expr: project_exprs, schema } => {
                // Map predicates through projection
                // We need to replace columns in predicates with the expression that generated them
                let mut mapped_predicates = Vec::new();
                let mut keep_predicates = Vec::new();

                for pred in predicates {
                    if let Some(new_pred) = self.rewrite_predicate(&pred, &project_exprs, &schema) {
                        mapped_predicates.push(new_pred);
                    } else {
                        // Could not map (e.g. non-deterministic expr?), keep it here
                        keep_predicates.push(pred);
                    }
                }

                let new_input = self.push_predicates_recursive(*input, mapped_predicates)?;
                
                if !keep_predicates.is_empty() {
                    Ok(LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::Project {
                            input: Box::new(new_input),
                            expr: project_exprs.clone(),
                            schema: schema.clone(),
                        }),
                        predicate: self.combine_conjunction(keep_predicates),
                    })
                } else {
                    Ok(LogicalPlan::Project {
                        input: Box::new(new_input),
                        expr: project_exprs.clone(),
                        schema: schema.clone(),
                    })
                }
            }
            _ => {
                // Cannot push down further
                let filter_expr = self.combine_conjunction(predicates);
                Ok(LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: filter_expr,
                })
            }
        }
    }

    fn rewrite_predicate(&self, pred: &Expr, project_exprs: &[Expr], schema: &arrow_schema::SchemaRef) -> Option<Expr> {
        // Simple implementation: replace Column(name) with the expression from project_exprs
        // that produces 'name'.
        // We use schema to find index of column name, then look up in project_exprs.
        
        match pred {
            Expr::Column(name) => {
                if let Ok(idx) = schema.index_of(name) {
                    if idx < project_exprs.len() {
                        let expr = &project_exprs[idx];
                        // If it's an alias, strip it
                        if let Expr::Alias { expr, .. } = expr {
                            return Some(*expr.clone());
                        }
                        return Some(expr.clone());
                    }
                }
                None
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.rewrite_predicate(left, project_exprs, schema)?;
                let r = self.rewrite_predicate(right, project_exprs, schema)?;
                Some(Expr::BinaryOp { left: Box::new(l), op: *op, right: Box::new(r) })
            }
            Expr::UnaryOp { op, expr } => {
                let e = self.rewrite_predicate(expr, project_exprs, schema)?;
                Some(Expr::UnaryOp { op: *op, expr: Box::new(e) })
            }
            Expr::Literal(_) => Some(pred.clone()),
            _ => None, // TODO: Support other expr types
        }
    }

    fn combine_conjunction(&self, exprs: Vec<Expr>) -> Expr {
        if exprs.is_empty() {
            return Expr::Literal(rsdb_sql::expr::Literal::Boolean(true));
        }
        let mut iter = exprs.into_iter();
        let first = iter.next().unwrap();
        iter.fold(first, |acc, e| Expr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOperator::And,
            right: Box::new(e),
        })
    }
}
