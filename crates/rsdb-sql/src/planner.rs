//! SQL to LogicalPlan planner - Robust Distributed Implementation

use crate::expr::{AggFunction, BinaryOperator, Expr as RsdbExpr, Literal};
use crate::logical_plan::{JoinCondition, JoinType};
use crate::{LogicalPlan, SqlParser};
use arrow_schema::{DataType, Field, Schema};
use rsdb_catalog::CatalogProvider;
use rsdb_common::{Result, RsdbError};
use std::collections::HashMap;
use std::sync::Arc;

/// SQL Planner - implementing standard DataFusion-compatible logic with suffix routing
pub struct SqlPlanner {
    catalog: Arc<dyn CatalogProvider>,
}

impl SqlPlanner {
    pub fn new(catalog: Arc<dyn CatalogProvider>) -> Self {
        Self { catalog }
    }

    pub fn plan(&self, sql: &str) -> Result<LogicalPlan> {
        let statements = SqlParser::parse(sql)?;
        if statements.is_empty() { return Err(RsdbError::Planner("Empty SQL".to_string())); }
        self.statement_to_plan(&statements[0])
    }

    fn statement_to_plan(&self, stmt: &sqlparser::ast::Statement) -> Result<LogicalPlan> {
        use sqlparser::ast::Statement;
        match stmt {
            Statement::Query(q) => self.query_to_plan(q),
            Statement::Analyze { table_name, .. } => {
                let name = table_name.0.first().map(|n| n.value.clone()).unwrap();
                Ok(LogicalPlan::Analyze { table_name: name })
            }
            Statement::Explain { statement, .. } => {
                let input = self.statement_to_plan(statement)?;
                Ok(LogicalPlan::Explain { input: Box::new(input) })
            }
            _ => Err(RsdbError::Planner("Only SELECT supported".to_string())),
        }
    }

    fn query_to_plan(&self, query: &sqlparser::ast::Query) -> Result<LogicalPlan> {
        let mut cte_map = HashMap::new();
        if let Some(ref with) = query.with {
            for cte in &with.cte_tables {
                let name = cte.alias.name.value.clone();
                let plan = self.query_to_plan(&cte.query)?;
                cte_map.insert(name, plan);
            }
        }
        let mut plan = self.set_expr_to_plan(&query.body, &cte_map)?;

        // Handle ORDER BY
        if let Some(ref order_by) = query.order_by {
            let mut order_exprs = Vec::new();
            for sort in &order_by.exprs {
                // Binding against current plan schema
                let expr = self.bind_names(&sort.expr, &plan)?;
                order_exprs.push(expr);
            }
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                expr: order_exprs,
            };
        }

        // Handle LIMIT / OFFSET
        if query.limit.is_some() || query.offset.is_some() {
            let limit = query.limit.as_ref()
                .and_then(|e| match e {
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => n.parse().ok(),
                    _ => None
                })
                .unwrap_or(usize::MAX);
            
            let offset = query.offset.as_ref()
                .and_then(|o| match &o.value {
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => n.parse().ok(),
                    _ => None
                })
                .unwrap_or(0);

            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
                offset,
            };
        }

        Ok(plan)
    }

    fn set_expr_to_plan(&self, set_expr: &sqlparser::ast::SetExpr, cte_map: &HashMap<String, LogicalPlan>) -> Result<LogicalPlan> {
        use sqlparser::ast::SetExpr;
        match set_expr {
            SetExpr::Select(s) => self.select_to_plan_with_cte(s, cte_map),
            SetExpr::SetOperation { op, left, right, set_quantifier } => {
                match op {
                    sqlparser::ast::SetOperator::Union => {
                         let left_plan = self.set_expr_to_plan(left, cte_map)?;
                         let right_plan = self.set_expr_to_plan(right, cte_map)?;
                         let schema = left_plan.schema();
                         let plan = LogicalPlan::Union { inputs: vec![left_plan, right_plan], schema };
                         match set_quantifier {
                             sqlparser::ast::SetQuantifier::All => Ok(plan),
                             _ => Err(RsdbError::Planner("Only UNION ALL supported".to_string())),
                         }
                    }
                    _ => Err(RsdbError::Planner("Only UNION supported".to_string())),
                }
            }
            _ => Err(RsdbError::Planner("Only SELECT supported".to_string())),
        }
    }

    fn select_to_plan_with_cte(
        &self,
        select: &sqlparser::ast::Select,
        cte_map: &HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let mut plan = if !select.from.is_empty() {
            let mut p = self.plan_table(&select.from[0], cte_map)?;
            for i in 1..select.from.len() {
                let right = self.plan_table(&select.from[i], cte_map)?;
                let mut fields = p.schema().fields().to_vec();
                fields.extend(right.schema().fields().to_vec());
                p = LogicalPlan::CrossJoin {
                    left: Box::new(p),
                    right: Box::new(right),
                    schema: Arc::new(Schema::new(fields)),
                };
            }
            p
        } else {
            LogicalPlan::EmptyRelation
        };

        if let Some(ref selection) = select.selection {
            let predicate = self.bind_names(selection, &plan)?;
            plan = LogicalPlan::Filter { input: Box::new(plan), predicate };
        }

        let mut projection_exprs: Vec<RsdbExpr> = select.projection.iter()
            .flat_map(|p| self.expand_projection(p, &plan))
            .collect();

        // Normalize aggregate expressions (e.g. COUNT(*) -> COUNT(1)) in projection_exprs
        for expr in &mut projection_exprs {
            self.normalize_aggregates(expr);
        }

        let has_aggr = projection_exprs.iter().any(|e| self.has_aggregate(e));
        if has_aggr {
            let group_exprs: Vec<RsdbExpr> = match &select.group_by {
                sqlparser::ast::GroupByExpr::Expressions(e, _) => {
                    e.iter().filter_map(|e| {
                        let bound = self.bind_names(e, &plan).ok();
                        if let Some(RsdbExpr::Column(ref name)) = bound {
                            // Check if this matches an alias in projection
                            for proj in &projection_exprs {
                                if let RsdbExpr::Alias { expr, name: alias_name } = proj {
                                    if name == alias_name {
                                        return Some((**expr).clone());
                                    }
                                }
                            }
                        }
                        bound
                    }).collect()
                }
                _ => vec![],
            };

            // Extract all unique aggregate functions
            let mut all_aggs = Vec::new();
            let mut seen_aggs = std::collections::HashSet::new();
            for expr in &projection_exprs {
                for agg in expr.get_aggregates() {
                    // They are already normalized now
                    if seen_aggs.insert(agg.clone()) {
                        all_aggs.push(agg);
                    }
                }
            }

            // Map each aggregate to a name for the Aggregate node output
            let mut agg_map = Vec::new();
            let mut agg_fields = Vec::new();
            for (i, agg) in all_aggs.iter().enumerate() {
                let name = format!("agg_{}", i);
                agg_map.push((agg.clone(), name.clone()));
                agg_fields.push(Field::new(name, DataType::Int64, true)); // TODO: Better type inference
            }

            // Also map group expressions to unique names to avoid resolution issues with dots
            let mut group_map = Vec::new();
            let mut group_fields = Vec::new();
            for (i, expr) in group_exprs.iter().enumerate() {
                let name = format!("group_{}", i);
                group_map.push((expr.clone(), name.clone()));
                group_fields.push(Field::new(name, DataType::Null, true));
            }
            
            let agg_schema_fields: Vec<Field> = group_fields.iter().cloned().chain(agg_fields.iter().cloned()).collect();
            let agg_schema = Arc::new(Schema::new(agg_schema_fields));

            let agg_plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_expr: group_exprs.clone(),
                aggregate_expr: all_aggs,
                schema: agg_schema,
            };

            // Create projection on top to reconstruct original expressions
            let project_exprs: Vec<RsdbExpr> = projection_exprs.iter().map(|expr| {
                // If the expression matches a group by expression, use the group alias
                for (g_expr, g_name) in &group_map {
                    if expr == g_expr {
                        return RsdbExpr::Column(g_name.clone());
                    }
                }
                // Otherwise replace aggregates with references to Aggregate node outputs
                let mut replaced = expr.replace_aggregates(&agg_map);
                // Also need to replace group expressions WITHIN complex expressions (e.g. group_col + 1)
                // We'll use a similar logic to replace_aggregates but for group columns
                for (g_expr, g_name) in &group_map {
                    replaced = self.replace_expr(replaced, g_expr, g_name);
                }
                replaced
            }).collect();

            let mut final_fields = Vec::new();
            for expr in &project_exprs {
                final_fields.push(Field::new(expr.to_string(), DataType::Null, true));
            }
            
            plan = LogicalPlan::Project {
                input: Box::new(agg_plan),
                expr: project_exprs,
                schema: Arc::new(Schema::new(final_fields)),
            };
        } else {
            let mut fields = Vec::new();
            for expr in &projection_exprs {
                fields.push(Field::new(expr.to_string(), DataType::Null, true));
            }
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                expr: projection_exprs,
                schema: Arc::new(Schema::new(fields)),
            };
        }
        Ok(plan)
    }

    fn expand_projection(&self, item: &sqlparser::ast::SelectItem, plan: &LogicalPlan) -> Vec<RsdbExpr> {
        match item {
            sqlparser::ast::SelectItem::Wildcard(_) => {
                // Use fully qualified names from the schema
                plan.schema().fields().iter().map(|f| RsdbExpr::Column(f.name().clone())).collect()
            }
            _ => if let Some(e) = self.projection_to_expr_with_bind(item, plan) { vec![e] } else { vec![] }
        }
    }

    fn has_aggregate(&self, expr: &RsdbExpr) -> bool {
        match expr {
            RsdbExpr::AggFunc { .. } => true,
            RsdbExpr::BinaryOp { left, right, .. } => self.has_aggregate(left) || self.has_aggregate(right),
            RsdbExpr::Alias { expr, .. } => self.has_aggregate(expr),
            _ => false,
        }
    }

    fn plan_table(&self, relation: &sqlparser::ast::TableWithJoins, cte_map: &HashMap<String, LogicalPlan>) -> Result<LogicalPlan> {
        let mut plan = self.plan_relation(&relation.relation, cte_map)?;
        for join in &relation.joins {
            let right = self.plan_relation(&join.relation, cte_map)?;
            
            let mut fields = plan.schema().fields().to_vec();
            fields.extend(right.schema().fields().to_vec());
            let combined_schema = Arc::new(Schema::new(fields));

            let cond = match &join.join_operator {
                sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(e)) => {
                    // Create a temporary join plan to resolve names against
                    let temp_join = LogicalPlan::Join {
                        left: Box::new(plan.clone()),
                        right: Box::new(right.clone()),
                        join_type: JoinType::Inner,
                        join_condition: JoinCondition::None,
                        schema: combined_schema.clone(),
                    };
                    JoinCondition::On(self.bind_names(e, &temp_join)?)
                },
                _ => JoinCondition::None,
            };

            plan = LogicalPlan::Join { 
                left: Box::new(plan), 
                right: Box::new(right), 
                join_type: JoinType::Inner, 
                join_condition: cond, 
                schema: combined_schema 
            };
        }
        Ok(plan)
    }

    fn plan_relation(&self, relation: &sqlparser::ast::TableFactor, cte_map: &HashMap<String, LogicalPlan>) -> Result<LogicalPlan> {
        use sqlparser::ast::TableFactor;
        match relation {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.0.first().map(|n| n.value.clone()).unwrap();
                let alias_name = alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| table_name.clone());
                let plan = if let Some(p) = cte_map.get(&table_name) { p.clone() } else {
                    let schema = self.catalog.schema("default").and_then(|s| s.table(&table_name)).map(|t| t.schema.clone())
                        .ok_or_else(|| RsdbError::Planner(format!("Table {} not found", table_name)))?;
                    LogicalPlan::Scan { table_name: table_name.clone(), schema, projection: None, filters: vec![] }
                };
                // Alias as Projection
                Ok(self.apply_alias(plan, &alias_name, None))
            }
            TableFactor::Derived { subquery, alias, .. } => {
                let plan = self.query_to_plan(subquery)?;
                if let Some(alias) = alias {
                    let alias_name = alias.name.value.clone();
                    let columns = if !alias.columns.is_empty() {
                        Some(alias.columns.iter().map(|c| c.name.value.clone()).collect())
                    } else {
                        None
                    };
                    Ok(self.apply_alias(plan, &alias_name, columns))
                } else {
                    Err(RsdbError::Planner("Subquery must have an alias".to_string()))
                }
            }
            TableFactor::NestedJoin { table_with_joins, .. } => {
                self.plan_table(table_with_joins, cte_map)
            }
            _ => Err(RsdbError::Planner("Unsupported relation".to_string())),
        }
    }

    fn apply_alias(&self, input: LogicalPlan, alias: &str, column_aliases: Option<Vec<String>>) -> LogicalPlan {
        let schema = input.schema();
        let mut exprs = Vec::with_capacity(schema.fields().len());
        let mut fields = Vec::with_capacity(schema.fields().len());

        for (i, field) in schema.fields().iter().enumerate() {
            let old_name = field.name();
            
            // Determine new name
            // 1. If explicit column aliases provided, use them
            // 2. Otherwise prefix with table alias
            let new_name = if let Some(cols) = &column_aliases {
                if i < cols.len() {
                    // Explicit alias: "alias.col" (we qualify it to be safe)
                    format!("{}.{}", alias, cols[i])
                } else {
                    // Fallback to prefix
                    format!("{}.{}", alias, old_name)
                }
            } else {
                // Prefix with table alias
                // Important: If old_name already has dots, we might just be re-aliasing.
                // Standard SQL: Alias REPLACES the old qualifier.
                // So t1.a aliased as t2 becomes t2.a.
                // We take the simple name (last part) and prepend the new alias.
                let simple_name = old_name.split('.').last().unwrap_or(old_name);
                format!("{}.{}", alias, simple_name)
            };

            exprs.push(RsdbExpr::Alias {
                expr: Box::new(RsdbExpr::Column(old_name.clone())),
                name: new_name.clone(),
            });
            
            fields.push(Field::new(new_name, field.data_type().clone(), field.is_nullable()));
        }

        LogicalPlan::Project {
            input: Box::new(input),
            expr: exprs,
            schema: Arc::new(Schema::new(fields)),
        }
    }

    fn bind_names(&self, expr: &sqlparser::ast::Expr, plan: &LogicalPlan) -> Result<RsdbExpr> {
        let e = self.expr_to_plan(expr)?;
        self.resolve_names(e, plan)
    }

    fn resolve_names(&self, expr: RsdbExpr, plan: &LogicalPlan) -> Result<RsdbExpr> {
        match expr {
            RsdbExpr::Column(name) => {
                let schema = plan.schema();
                // 1. Try exact match
                if schema.field_with_name(&name).is_ok() { 
                    return Ok(RsdbExpr::Column(name)); 
                }
                
                // 2. Try matching the name as a suffix
                let matches: Vec<String> = schema.fields().iter().map(|f| f.name().clone())
                    .filter(|f| {
                        if f == &name { return true; }
                        // Suffix match: if f is "a.b.c" and name is "b.c", it should match.
                        // We check if it ends with name AND (it's an exact match OR there is a dot before the match)
                        f.ends_with(&name) && (f.len() == name.len() || f.as_bytes()[f.len() - name.len() - 1] == b'.')
                    }).collect();
                
                if matches.len() == 1 {
                    // Return the FULL qualified name
                    Ok(RsdbExpr::Column(matches[0].clone()))
                } else if matches.len() > 1 {
                    Err(RsdbError::Planner(format!("Ambiguous column reference: {}", name)))
                } else {
                    // If not found, return as is (might be resolved later or valid in context)
                    Ok(RsdbExpr::Column(name))
                }
            }
            RsdbExpr::BinaryOp { left, op, right } => Ok(RsdbExpr::BinaryOp { left: Box::new(self.resolve_names(*left, plan)?), op, right: Box::new(self.resolve_names(*right, plan)?) }),
            RsdbExpr::AggFunc { func, args, distinct } => {
                let mut res = Vec::new();
                for a in args { res.push(self.resolve_names(a, plan)?); }
                Ok(RsdbExpr::AggFunc { func, args: res, distinct })
            }
            RsdbExpr::Alias { expr, name } => Ok(RsdbExpr::Alias { expr: Box::new(self.resolve_names(*expr, plan)?), name }),
            _ => Ok(expr),
        }
    }

    fn projection_to_expr_with_bind(&self, p: &sqlparser::ast::SelectItem, plan: &LogicalPlan) -> Option<RsdbExpr> {
        match p {
            sqlparser::ast::SelectItem::UnnamedExpr(e) => self.bind_names(e, plan).ok(),
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                let e = self.bind_names(expr, plan).ok()?;
                Some(RsdbExpr::Alias { expr: Box::new(e), name: alias.value.clone() })
            }
            _ => None
        }
    }

    fn expr_to_plan(&self, expr: &sqlparser::ast::Expr) -> Result<RsdbExpr> {
        use sqlparser::ast::Expr as SqlExpr;
        match expr {
            SqlExpr::Identifier(id) => Ok(RsdbExpr::Column(id.value.clone())),
            SqlExpr::CompoundIdentifier(ids) => Ok(RsdbExpr::Column(ids.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join("."))),
            SqlExpr::Value(v) => {
                let lit = match v {
                    sqlparser::ast::Value::Number(n, _) => if let Ok(i) = n.parse() { Literal::Int(i) } else { Literal::Float(n.parse().unwrap_or(0.0)) },
                    sqlparser::ast::Value::SingleQuotedString(s) => Literal::String(s.clone()),
                    sqlparser::ast::Value::Boolean(b) => Literal::Boolean(*b),
                    _ => Literal::Null,
                };
                Ok(RsdbExpr::Literal(lit))
            }
            SqlExpr::BinaryOp { left, op, right } => {
                let operator = match format!("{:?}", op).as_str() {
                    "Eq" => BinaryOperator::Eq, "Neq" => BinaryOperator::Neq, "Lt" => BinaryOperator::Lt, "Gt" => BinaryOperator::Gt,
                    "Lte" => BinaryOperator::Lte, "Gte" => BinaryOperator::Gte, "And" => BinaryOperator::And, "Or" => BinaryOperator::Or,
                    "Plus" => BinaryOperator::Plus, "Minus" => BinaryOperator::Minus, "Multiply" => BinaryOperator::Multiply, "Divide" => BinaryOperator::Divide,
                    _ => BinaryOperator::Eq,
                };
                Ok(RsdbExpr::BinaryOp { left: Box::new(self.expr_to_plan(left)?), op: operator, right: Box::new(self.expr_to_plan(right)?) })
            }
            SqlExpr::Function(f) => {
                let name = f.name.to_string().to_lowercase();
                let mut args = Vec::new();
                if let sqlparser::ast::FunctionArguments::List(l) = &f.args {
                    for a in &l.args { if let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) = a { args.push(self.expr_to_plan(e)?); } }
                }
                let agg = match name.as_str() { "count" => Some(AggFunction::Count), "sum" => Some(AggFunction::Sum), "min" => Some(AggFunction::Min), "max" => Some(AggFunction::Max), "avg" => Some(AggFunction::Avg), _ => None };
                if let Some(af) = agg { Ok(RsdbExpr::AggFunc { func: af, args, distinct: false }) } else { Ok(RsdbExpr::Function { name, args }) }
            }
            SqlExpr::Case { operand, conditions, results, else_result } => {
                let op = operand.as_ref().map(|o| self.expr_to_plan(o)).transpose()?.map(Box::new);
                let conds = conditions.iter().map(|c| self.expr_to_plan(c)).collect::<Result<Vec<_>>>()?;
                let res = results.iter().map(|r| self.expr_to_plan(r)).collect::<Result<Vec<_>>>()?;
                let else_res = else_result.as_ref().map(|e| self.expr_to_plan(e)).transpose()?.map(Box::new);
                Ok(RsdbExpr::Case { operand: op, conditions: conds, results: res, else_result: else_res })
            }
            _ => Ok(RsdbExpr::Literal(Literal::Null)),
        }
    }

    fn normalize_aggregates(&self, expr: &mut RsdbExpr) {
        match expr {
            RsdbExpr::AggFunc { func: AggFunction::Count, args, distinct } if args.is_empty() || (args.len()==1 && matches!(args[0], RsdbExpr::Column(ref c) if c=="*")) => {
                *args = vec![RsdbExpr::Literal(Literal::Int(1))];
            }
            RsdbExpr::BinaryOp { left, right, .. } => {
                self.normalize_aggregates(left);
                self.normalize_aggregates(right);
            }
            RsdbExpr::UnaryOp { expr, .. } => {
                self.normalize_aggregates(expr);
            }
            RsdbExpr::Alias { expr, .. } => {
                self.normalize_aggregates(expr);
            }
            RsdbExpr::AggFunc { args, .. } => {
                for arg in args {
                    self.normalize_aggregates(arg);
                }
            }
            RsdbExpr::Function { args, .. } => {
                for arg in args {
                    self.normalize_aggregates(arg);
                }
            }
            RsdbExpr::Cast { expr, .. } => {
                self.normalize_aggregates(expr);
            }
            RsdbExpr::Case { operand, conditions, results, else_result } => {
                if let Some(op) = operand {
                    self.normalize_aggregates(op);
                }
                for c in conditions {
                    self.normalize_aggregates(c);
                }
                for r in results {
                    self.normalize_aggregates(r);
                }
                if let Some(e) = else_result {
                    self.normalize_aggregates(e);
                }
            }
            _ => {}
        }
    }

    fn replace_expr(&self, expr: RsdbExpr, from: &RsdbExpr, to_name: &str) -> RsdbExpr {
        if &expr == from {
            return RsdbExpr::Column(to_name.to_string());
        }
        match expr {
            RsdbExpr::BinaryOp { left, op, right } => RsdbExpr::BinaryOp {
                left: Box::new(self.replace_expr(*left, from, to_name)),
                op,
                right: Box::new(self.replace_expr(*right, from, to_name)),
            },
            RsdbExpr::UnaryOp { op, expr } => RsdbExpr::UnaryOp {
                op,
                expr: Box::new(self.replace_expr(*expr, from, to_name)),
            },
            RsdbExpr::Alias { expr, name } => RsdbExpr::Alias {
                expr: Box::new(self.replace_expr(*expr, from, to_name)),
                name,
            },
            RsdbExpr::AggFunc { func, args, distinct } => RsdbExpr::AggFunc {
                func,
                args: args.into_iter().map(|a| self.replace_expr(a, from, to_name)).collect(),
                distinct,
            },
            RsdbExpr::Function { name, args } => RsdbExpr::Function {
                name,
                args: args.into_iter().map(|a| self.replace_expr(a, from, to_name)).collect(),
            },
            RsdbExpr::Cast { expr, data_type } => RsdbExpr::Cast {
                expr: Box::new(self.replace_expr(*expr, from, to_name)),
                data_type,
            },
            RsdbExpr::Case { operand, conditions, results, else_result } => RsdbExpr::Case {
                operand: operand.map(|o| Box::new(self.replace_expr(*o, from, to_name))),
                conditions: conditions.into_iter().map(|c| self.replace_expr(c, from, to_name)).collect(),
                results: results.into_iter().map(|r| self.replace_expr(r, from, to_name)).collect(),
                else_result: else_result.map(|e| Box::new(self.replace_expr(*e, from, to_name))),
            },
            _ => expr,
        }
    }
}
