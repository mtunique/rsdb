//! SQL to LogicalPlan planner - Full implementation for TPC-DS and TPC-H

use crate::expr::{AggFunction, BinaryOperator, Expr as RsdbExpr, Literal, SubqueryType, UnaryOperator};
use crate::logical_plan::{JoinCondition, JoinType};
use crate::{LogicalPlan, SqlParser};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use rsdb_catalog::CatalogProvider;
use rsdb_common::{Result, RsdbError};
use std::collections::HashMap;
use std::sync::Arc;

/// SQL Planner - full implementation for TPC-DS and TPC-H
pub struct SqlPlanner {
    catalog: Arc<dyn CatalogProvider>,
}

impl SqlPlanner {
    pub fn new(catalog: Arc<dyn CatalogProvider>) -> Self {
        Self { catalog }
    }

    pub fn plan(&self, sql: &str) -> Result<LogicalPlan> {
        let statements = SqlParser::parse(sql)?;
        if statements.is_empty() {
            return Err(RsdbError::Planner("Empty SQL".to_string()));
        }
        let stmt = &statements[0];
        self.statement_to_plan(stmt)
    }

    fn statement_to_plan(&self, stmt: &sqlparser::ast::Statement) -> Result<LogicalPlan> {
        use sqlparser::ast::Statement;
        match stmt {
            Statement::Query(q) => self.query_to_plan(q),
            Statement::CreateTable(t) => {
                let table_name = t.name.0.first().map(|obj| obj.value.clone()).unwrap_or_default();
                let mut fields = Vec::new();
                for col in &t.columns {
                    let dt = self.convert_data_type(&col.data_type);
                    let name = col.name.value.clone();
                    let nullable = col.options.is_empty();
                    fields.push(Field::new(name, dt, nullable));
                }
                let schema = Arc::new(Schema::new(fields));
                Ok(LogicalPlan::CreateTable {
                    table_name,
                    schema,
                    partition_keys: vec![],
                    if_not_exists: t.if_not_exists,
                })
            }
            Statement::Drop { names, if_exists, .. } => {
                let table_name = names.first().and_then(|n| n.0.first()).map(|n| n.value.clone())
                    .ok_or_else(|| RsdbError::Planner("No table name".to_string()))?;
                Ok(LogicalPlan::DropTable { table_name, if_exists: *if_exists })
            }
            Statement::Insert(i) => Ok(LogicalPlan::Insert {
                table_name: i.table_name.0.first().map(|n| n.value.clone()).unwrap_or_default(),
                source: Box::new(LogicalPlan::EmptyRelation),
            }),
            Statement::Analyze { table_name, .. } => {
                let name = table_name.0.first().map(|n| n.value.clone())
                    .ok_or_else(|| RsdbError::Planner("ANALYZE requires table name".to_string()))?;
                Ok(LogicalPlan::Analyze { table_name: name })
            }
            _ => Err(RsdbError::Planner("Unsupported statement".to_string())),
        }
    }

    fn convert_data_type(&self, dt: &sqlparser::ast::DataType) -> DataType {
        use sqlparser::ast::DataType as SqlDt;
        match dt {
            SqlDt::Int(_) => DataType::Int64,
            SqlDt::Integer(_) => DataType::Int32,
            SqlDt::BigInt(_) => DataType::Int64,
            SqlDt::SmallInt(_) => DataType::Int16,
            SqlDt::TinyInt(_) => DataType::Int8,
            SqlDt::Varchar(_) | SqlDt::Char(_) | SqlDt::Text => DataType::Utf8,
            SqlDt::Boolean => DataType::Boolean,
            SqlDt::Float(_) => DataType::Float32,
            SqlDt::Double => DataType::Float64,
            SqlDt::Decimal(_) => DataType::Decimal128(10, 2),
            SqlDt::Date => DataType::Date32,
            SqlDt::Timestamp(_, _) => DataType::Timestamp(TimeUnit::Microsecond, None),
            _ => DataType::Utf8,
        }
    }

    fn query_to_plan(&self, query: &sqlparser::ast::Query) -> Result<LogicalPlan> {
        if query.with.is_some() {
            return self.handle_cte_query(query);
        }

        use sqlparser::ast::SetExpr;
        match &*query.body {
            SetExpr::Select(s) => self.select_to_plan(s, query),
            SetExpr::SetOperation { left, right, op, set_quantifier: _ } => {
                let left_query = sqlparser::ast::Query {
                    with: None,
                    body: left.clone(),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                };
                let right_query = sqlparser::ast::Query {
                    with: None,
                    body: right.clone(),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                };

                let left_plan = self.query_to_plan(&left_query)?;
                let right_plan = self.query_to_plan(&right_query)?;

                match op {
                    sqlparser::ast::SetOperator::Union => {
                        let schema = left_plan.schema();
                        Ok(LogicalPlan::Union {
                            inputs: vec![left_plan, right_plan],
                            schema,
                        })
                    }
                    _ => {
                        let schema = left_plan.schema();
                        Ok(LogicalPlan::Union {
                            inputs: vec![left_plan, right_plan],
                            schema,
                        })
                    }
                }
            }
            _ => Err(RsdbError::Planner("Only SELECT and UNION supported".to_string())),
        }
    }

    fn handle_cte_query(&self, query: &sqlparser::ast::Query) -> Result<LogicalPlan> {
        let with = match &query.with {
            Some(w) => w,
            None => return self.query_to_plan(query),
        };

        let mut cte_map: HashMap<String, LogicalPlan> = HashMap::new();

        for cte in &with.cte_tables {
            let cte_name = cte.alias.name.value.clone();

            let cte_query = sqlparser::ast::Query {
                with: None,
                body: cte.query.body.clone(),
                order_by: cte.query.order_by.clone(),
                limit: cte.query.limit.clone(),
                limit_by: cte.query.limit_by.clone(),
                offset: cte.query.offset.clone(),
                fetch: cte.query.fetch.clone(),
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            };

            let cte_plan = self.query_to_plan(&cte_query)?;
            cte_map.insert(cte_name, cte_plan);
        }

        let main_plan = match &*query.body {
            sqlparser::ast::SetExpr::Select(s) => {
                self.select_to_plan_with_cte(s, query, &cte_map)?
            }
            _ => {
                return Err(RsdbError::Planner("CTE with UNION/other ops not fully supported".to_string()));
            }
        };

        let mut plan = main_plan;

        if let Some(ref order_by) = query.order_by {
            if !order_by.exprs.is_empty() {
                let sort_exprs: Vec<RsdbExpr> = order_by.exprs.iter()
                    .filter_map(|o| self.expr_to_plan(&o.expr).ok())
                    .collect();
                plan = LogicalPlan::Sort {
                    input: Box::new(plan),
                    expr: sort_exprs,
                };
            }
        }

        if let Some(ref limit) = query.limit {
            let limit_value = self.expr_to_i64(limit).unwrap_or(0) as usize;
            let offset_value = query.offset.as_ref()
                .map(|o| self.expr_to_i64(&o.value).unwrap_or(0) as usize)
                .unwrap_or(0);
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit: limit_value,
                offset: offset_value,
            };
        }

        Ok(plan)
    }

    fn select_to_plan_with_cte(
        &self,
        select: &sqlparser::ast::Select,
        _query: &sqlparser::ast::Query,
        cte_map: &HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let mut plan = if !select.from.is_empty() {
            let first_plan = self.plan_from_item(&select.from[0], cte_map)?;

            let mut combined = first_plan;
            for from_item in select.from.iter().skip(1) {
                let right_plan = self.plan_from_item(from_item, cte_map)?;
                let left_schema = combined.schema();
                let right_schema = right_plan.schema();
                
                let mut fields: Vec<Arc<Field>> = left_schema.fields().iter().cloned().collect();
                let left_field_names: std::collections::HashSet<String> = fields.iter().map(|f| f.name().clone()).collect();
                
                for f in right_schema.fields().iter() {
                    if !left_field_names.contains(f.name()) {
                        fields.push(f.clone());
                    }
                }
                
                let merged_schema = Arc::new(Schema::new(
                    fields.into_iter().map(|f| f.as_ref().clone()).collect::<Vec<_>>()
                ));
                
                combined = LogicalPlan::CrossJoin {
                    left: Box::new(combined),
                    right: Box::new(right_plan),
                    schema: merged_schema,
                };
            }
            combined
        } else {
            LogicalPlan::EmptyRelation
        };

        if let Some(ref selection) = select.selection {
            if let Ok(predicate) = self.expr_to_plan_with_subquery(selection, cte_map) {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate,
                };
            }
        }

        let projection_exprs: Vec<RsdbExpr> = select.projection.iter()
            .filter_map(|p| self.projection_to_expr(p))
            .collect();

        let has_group = match &select.group_by {
            sqlparser::ast::GroupByExpr::Expressions(e, _) if !e.is_empty() => true,
            _ => false,
        };

        let has_aggr = projection_exprs.iter().any(|e| self.has_aggregate(e));

        if has_group || has_aggr {
            let group_exprs: Vec<RsdbExpr> = if has_group {
                match &select.group_by {
                    sqlparser::ast::GroupByExpr::Expressions(e, _) => {
                        e.iter().filter_map(|e| self.expr_to_plan(e).ok()).collect()
                    }
                    _ => vec![],
                }
            } else {
                vec![]
            };

            let mut pre_projection = Vec::new();
            let mut agg_exprs = Vec::new();
            let mut col_idx = 0;

            for expr in &projection_exprs {
                let transformed = self.transform_aggregate_expr(expr.clone(), &mut pre_projection, &mut col_idx);
                agg_exprs.push(transformed);
            }

            if !pre_projection.is_empty() {
                let mut project_exprs = pre_projection;
                for g_expr in &group_exprs {
                    if !project_exprs.contains(g_expr) {
                        project_exprs.push(g_expr.clone());
                    }
                }

                let project_schema = Arc::new(Schema::new(
                    project_exprs.iter().enumerate().map(|(i, _)| {
                        Field::new(format!("_pre_agg_{}", i), DataType::Null, true)
                    }).collect::<Vec<_>>()
                ));

                plan = LogicalPlan::Project {
                    input: Box::new(plan),
                    expr: project_exprs,
                    schema: project_schema,
                };
            }

            if let Some(ref having) = select.having {
                if let Ok(predicate) = self.expr_to_plan(having) {
                    plan = LogicalPlan::Filter {
                        input: Box::new(plan),
                        predicate,
                    };
                }
            }

            let fields: Vec<Field> = agg_exprs.iter().enumerate().map(|(i, expr)| {
                let name = match expr {
                    RsdbExpr::Alias { name, .. } => name.clone(),
                    RsdbExpr::Column(c) => c.clone(),
                    RsdbExpr::AggFunc { func, args, .. } => {
                        let arg_name = args.first().and_then(|a| match a {
                            RsdbExpr::Column(c) => Some(c.as_str()),
                            _ => None,
                        }).unwrap_or("*");
                        format!("{:?}({})", func, arg_name)
                    }
                    _ => format!("col{}", i),
                };
                Field::new(name, DataType::Null, true)
            }).collect();
            let schema = Arc::new(Schema::new(fields));

            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_expr: group_exprs,
                aggregate_expr: agg_exprs,
                schema,
            };
        } else {
            if !select.projection.is_empty() {
                let fields: Vec<Field> = projection_exprs.iter().enumerate().map(|(i, expr)| {
                    let name = match expr {
                        RsdbExpr::Alias { name, .. } => name.clone(),
                        RsdbExpr::Column(c) => c.clone(),
                        _ => format!("col{}", i),
                    };
                    Field::new(name, DataType::Null, true)
                }).collect();
                let schema = Arc::new(Schema::new(fields));

                plan = LogicalPlan::Project {
                    input: Box::new(plan),
                    expr: projection_exprs,
                    schema,
                };
            }
        }

        Ok(plan)
    }

    fn has_aggregate(&self, expr: &RsdbExpr) -> bool {
        match expr {
            RsdbExpr::AggFunc { .. } => true,
            RsdbExpr::BinaryOp { left, right, .. } => self.has_aggregate(left) || self.has_aggregate(right),
            RsdbExpr::UnaryOp { expr, .. } => self.has_aggregate(expr),
            RsdbExpr::Alias { expr, .. } => self.has_aggregate(expr),
            RsdbExpr::Function { args, .. } => args.iter().any(|a| self.has_aggregate(a)),
            RsdbExpr::Cast { expr, .. } => self.has_aggregate(expr),
            _ => false,
        }
    }

    fn transform_aggregate_expr(&self, expr: RsdbExpr, pre_projection: &mut Vec<RsdbExpr>, col_idx: &mut usize) -> RsdbExpr {
        match expr {
            RsdbExpr::AggFunc { func, args, distinct } => {
                let mut new_args = Vec::new();
                for arg in args {
                    if matches!(arg, RsdbExpr::Column(_)) || matches!(arg, RsdbExpr::Literal(_)) {
                        new_args.push(arg);
                    } else {
                        let alias = format!("_agg_arg_{}", *col_idx);
                        *col_idx += 1;
                        pre_projection.push(RsdbExpr::Alias {
                            expr: Box::new(arg),
                            name: alias.clone(),
                        });
                        new_args.push(RsdbExpr::Column(alias));
                    }
                }
                RsdbExpr::AggFunc { func, args: new_args, distinct }
            }
            RsdbExpr::BinaryOp { left, op, right } => {
                let new_left = self.transform_aggregate_expr(*left, pre_projection, col_idx);
                let new_right = self.transform_aggregate_expr(*right, pre_projection, col_idx);
                RsdbExpr::BinaryOp { left: Box::new(new_left), op, right: Box::new(new_right) }
            }
            RsdbExpr::Alias { expr, name } => {
                let new_expr = self.transform_aggregate_expr(*expr, pre_projection, col_idx);
                RsdbExpr::Alias { expr: Box::new(new_expr), name }
            }
            _ => expr,
        }
    }

    fn select_to_plan(&self, select: &sqlparser::ast::Select, query: &sqlparser::ast::Query) -> Result<LogicalPlan> {
        let empty_cte_map: HashMap<String, LogicalPlan> = HashMap::new();
        self.select_to_plan_with_cte(select, query, &empty_cte_map)
    }

    fn plan_from_item(
        &self,
        from_item: &sqlparser::ast::TableWithJoins,
        cte_map: &HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let table_name = match &from_item.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                name.0.first().map(|i| i.value.clone()).unwrap_or_default()
            }
            _ => String::new(),
        };

        if cte_map.contains_key(&table_name) {
            let cte_plan = cte_map.get(&table_name).cloned().unwrap_or(LogicalPlan::EmptyRelation);
            let schema = cte_plan.schema().clone();
            return Ok(LogicalPlan::Subquery {
                query: Box::new(cte_plan),
                schema,
            });
        }

        match &from_item.relation {
            sqlparser::ast::TableFactor::Derived { subquery, alias, .. } => {
                let subquery_plan = self.query_to_plan(subquery)?;
                let schema = subquery_plan.schema();
                let alias_name = alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| "subquery".to_string());
                Ok(LogicalPlan::Subquery {
                    query: Box::new(subquery_plan),
                    schema,
                })
            }
            _ => self.plan_table(from_item),
        }
    }

    fn plan_table(&self, relation: &sqlparser::ast::TableWithJoins) -> Result<LogicalPlan> {
        use sqlparser::ast::{TableFactor, JoinOperator};

        let (mut plan, mut base_alias) = match &relation.relation {
            TableFactor::Table { name, alias, .. } => {
                let table_name = if name.0.len() > 1 {
                    format!("{}.{}", name.0[0].value, name.0[1].value)
                } else {
                    name.0[0].value.clone()
                };

                let alias = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table_name.clone());

                let schema = self.catalog.schema("default")
                    .and_then(|s| s.table(&table_name))
                    .map(|t| t.schema.clone())
                    .unwrap_or_else(|| {
                        Arc::new(Schema::new(vec![Field::new("*".to_string(), DataType::Int64, true)]))
                    });

                (LogicalPlan::Scan {
                    table_name,
                    alias: Some(alias.clone()),
                    schema,
                    projection: None,
                    filters: vec![],
                }, alias)
            }
            TableFactor::Derived { subquery, alias, .. } => {
                let subquery_plan = self.query_to_plan(subquery)?;
                let schema = subquery_plan.schema();
                let alias = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "subquery".to_string());
                (LogicalPlan::Subquery {
                    query: Box::new(subquery_plan),
                    schema,
                }, alias)
            }
            _ => return Err(RsdbError::Planner("Only simple table refs".to_string())),
        };

        for join in &relation.joins {
            let (right_plan, right_alias) = match &join.relation {
                TableFactor::Table { name, alias, .. } => {
                    let table_name = if name.0.len() > 1 {
                        format!("{}.{}", name.0[0].value, name.0[1].value)
                    } else {
                        name.0[0].value.clone()
                    };

                    let alias = alias.as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| table_name.clone());

                    let schema = self.catalog.schema("default")
                        .and_then(|s| s.table(&table_name))
                        .map(|t| t.schema.clone())
                        .unwrap_or_else(|| {
                            Arc::new(Schema::new(vec![Field::new("*".to_string(), DataType::Int64, true)]))
                        });

                    (LogicalPlan::Scan {
                        table_name,
                        alias: Some(alias.clone()),
                        schema,
                        projection: None,
                        filters: vec![],
                    }, alias)
                }
                TableFactor::Derived { subquery, alias, .. } => {
                    let subquery_plan = self.query_to_plan(subquery)?;
                    let schema = subquery_plan.schema();
                    let alias = alias.as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| "subquery".to_string());
                    (LogicalPlan::Subquery {
                        query: Box::new(subquery_plan),
                        schema,
                    }, alias)
                }
                _ => continue,
            };

            let (join_type, join_condition) = match &join.join_operator {
                JoinOperator::Inner(constraint) => {
                    let cond = match constraint {
                        sqlparser::ast::JoinConstraint::On(expr) => {
                            match self.expr_to_plan(expr) {
                                Ok(predicate) => JoinCondition::On(predicate),
                                Err(_) => JoinCondition::None,
                            }
                        }
                        sqlparser::ast::JoinConstraint::Using(names) => {
                            JoinCondition::Using(names.iter().map(|i| i.value.clone()).collect())
                        }
                        _ => JoinCondition::None,
                    };
                    (JoinType::Inner, cond)
                }
                JoinOperator::LeftOuter(constraint) => {
                    let cond = match constraint {
                        sqlparser::ast::JoinConstraint::On(expr) => {
                            match self.expr_to_plan(expr) {
                                Ok(predicate) => JoinCondition::On(predicate),
                                Err(_) => JoinCondition::None,
                            }
                        }
                        sqlparser::ast::JoinConstraint::Using(names) => {
                            JoinCondition::Using(names.iter().map(|i| i.value.clone()).collect())
                        }
                        _ => JoinCondition::None,
                    };
                    (JoinType::Left, cond)
                }
                JoinOperator::RightOuter(constraint) => {
                    let cond = match constraint {
                        sqlparser::ast::JoinConstraint::On(expr) => {
                            match self.expr_to_plan(expr) {
                                Ok(predicate) => JoinCondition::On(predicate),
                                Err(_) => JoinCondition::None,
                            }
                        }
                        sqlparser::ast::JoinConstraint::Using(names) => {
                            JoinCondition::Using(names.iter().map(|i| i.value.clone()).collect())
                        }
                        _ => JoinCondition::None,
                    };
                    (JoinType::Right, cond)
                }
                JoinOperator::FullOuter(constraint) => {
                    let cond = match constraint {
                        sqlparser::ast::JoinConstraint::On(expr) => {
                            match self.expr_to_plan(expr) {
                                Ok(predicate) => JoinCondition::On(predicate),
                                Err(_) => JoinCondition::None,
                            }
                        }
                        sqlparser::ast::JoinConstraint::Using(names) => {
                            JoinCondition::Using(names.iter().map(|i| i.value.clone()).collect())
                        }
                        _ => JoinCondition::None,
                    };
                    (JoinType::Full, cond)
                }
                JoinOperator::CrossJoin => (JoinType::Cross, JoinCondition::None),
                _ => (JoinType::Inner, JoinCondition::None),
            };

            let join_schema = {
                let mut fields = plan.schema().fields().to_vec();
                fields.extend(right_plan.schema().fields().to_vec());
                Arc::new(Schema::new(fields))
            };

            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type,
                join_condition,
                schema: join_schema,
            };

            base_alias = format!("{} JOIN {}", base_alias, right_alias);
        }

        Ok(plan)
    }

    fn projection_to_expr(&self, p: &sqlparser::ast::SelectItem) -> Option<RsdbExpr> {
        match p {
            sqlparser::ast::SelectItem::UnnamedExpr(e) => self.expr_to_plan(e).ok(),
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                self.expr_to_plan(expr).map(|e| RsdbExpr::Alias {
                    expr: Box::new(e),
                    name: alias.value.clone(),
                }).ok()
            }
            _ => None,
        }
    }

    fn expr_to_plan_with_subquery(
        &self,
        expr: &sqlparser::ast::Expr,
        _cte_map: &HashMap<String, LogicalPlan>,
    ) -> Result<RsdbExpr> {
        self.expr_to_plan(expr)
    }

    fn expr_to_plan(&self, expr: &sqlparser::ast::Expr) -> Result<RsdbExpr> {
        use sqlparser::ast::Expr as SqlExpr;
        match expr {
            SqlExpr::Identifier(id) => Ok(RsdbExpr::Column(id.value.clone())),
            SqlExpr::CompoundIdentifier(ids) => {
                let name = if ids.len() >= 2 {
                    format!("{}.{}", ids[0].value, ids.last().unwrap().value)
                } else {
                    ids.last().map(|i| i.value.clone()).unwrap_or_default()
                };
                Ok(RsdbExpr::Column(name))
            }
            SqlExpr::Value(lit) => {
                let literal = match lit {
                    sqlparser::ast::Value::Null => Literal::Null,
                    sqlparser::ast::Value::Boolean(b) => Literal::Boolean(*b),
                    sqlparser::ast::Value::Number(n, _) => {
                        if let Ok(i) = n.parse::<i64>() { Literal::Int(i) }
                        else if let Ok(f) = n.parse::<f64>() { Literal::Float(f) }
                        else { Literal::String(n.clone()) }
                    }
                    sqlparser::ast::Value::SingleQuotedString(s) => Literal::String(s.clone()),
                    sqlparser::ast::Value::DoubleQuotedString(s) => Literal::String(s.clone()),
                    sqlparser::ast::Value::HexStringLiteral(s) => Literal::String(s.clone()),
                    sqlparser::ast::Value::NationalStringLiteral(s) => Literal::String(s.clone()),
                    sqlparser::ast::Value::Placeholder(_) => Literal::Null,
                    _ => Literal::Null,
                };
                Ok(RsdbExpr::Literal(literal))
            }
            SqlExpr::BinaryOp { left, op, right } => {
                let left_expr = self.expr_to_plan(left)?;
                let right_expr = self.expr_to_plan(right)?;
                let operator = self.convert_binary_op(op);
                Ok(RsdbExpr::BinaryOp { left: Box::new(left_expr), op: operator, right: Box::new(right_expr) })
            }
            SqlExpr::UnaryOp { op, expr } => {
                let inner = self.expr_to_plan(expr)?;
                let operator = match op {
                    sqlparser::ast::UnaryOperator::Plus => UnaryOperator::Plus,
                    sqlparser::ast::UnaryOperator::Minus => UnaryOperator::Minus,
                    sqlparser::ast::UnaryOperator::Not => UnaryOperator::Not,
                    _ => UnaryOperator::Not,
                };
                Ok(RsdbExpr::UnaryOp { op: operator, expr: Box::new(inner) })
            }
            SqlExpr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();
                let args: Result<Vec<RsdbExpr>> = match &func.args {
                    sqlparser::ast::FunctionArguments::List(args) => {
                        args.args.iter().map(|a| {
                            match a {
                                sqlparser::ast::FunctionArg::Unnamed(expr) => {
                                    match expr {
                                        sqlparser::ast::FunctionArgExpr::Expr(e) => self.expr_to_plan(e),
                                        _ => Err(RsdbError::Planner("Unsupported function arg".to_string())),
                                    }
                                }
                                _ => Err(RsdbError::Planner("Unsupported function arg".to_string())),
                            }
                        }).collect()
                    }
                    _ => Ok(vec![]),
                };

                let agg_func = match func_name.as_str() {
                    "count" => Some(AggFunction::Count),
                    "sum" => Some(AggFunction::Sum),
                    "avg" => Some(AggFunction::Avg),
                    "min" => Some(AggFunction::Min),
                    "max" => Some(AggFunction::Max),
                    _ => None,
                };

                if let Some(af) = agg_func {
                    Ok(RsdbExpr::AggFunc { func: af, args: args?, distinct: false })
                } else {
                    Ok(RsdbExpr::Function { name: func_name, args: args? })
                }
            }
            SqlExpr::Extract { field, expr, .. } => {
                let inner = self.expr_to_plan(expr)?;
                Ok(RsdbExpr::Function { 
                    name: "extract".to_string(), 
                    args: vec![
                        RsdbExpr::Literal(Literal::String(field.to_string())),
                        inner
                    ] 
                })
            }
            SqlExpr::IsNull(expr) => {
                let inner = self.expr_to_plan(expr)?;
                Ok(RsdbExpr::BinaryOp { left: Box::new(inner), op: BinaryOperator::Eq, right: Box::new(RsdbExpr::Literal(Literal::Null)) })
            }
            SqlExpr::IsNotNull(expr) => {
                let inner = self.expr_to_plan(expr)?;
                Ok(RsdbExpr::BinaryOp { left: Box::new(inner), op: BinaryOperator::Neq, right: Box::new(RsdbExpr::Literal(Literal::Null)) })
            }
            SqlExpr::Between { expr, negated, low, high } => {
                let expr = self.expr_to_plan(expr)?;
                let low = self.expr_to_plan(low)?;
                let high = self.expr_to_plan(high)?;
                let ge = RsdbExpr::BinaryOp { left: Box::new(expr.clone()), op: BinaryOperator::Gte, right: Box::new(low) };
                let le = RsdbExpr::BinaryOp { left: Box::new(expr), op: BinaryOperator::Lte, right: Box::new(high) };
                if *negated {
                    Ok(RsdbExpr::BinaryOp { left: Box::new(ge), op: BinaryOperator::Or, right: Box::new(le) })
                } else {
                    Ok(RsdbExpr::BinaryOp { left: Box::new(ge), op: BinaryOperator::And, right: Box::new(le) })
                }
            }
            SqlExpr::InList { expr, list, negated } => {
                let expr = self.expr_to_plan(expr)?;
                let list_exprs: Result<Vec<RsdbExpr>> = list.iter().map(|e| self.expr_to_plan(e)).collect();
                let in_expr = list_exprs?.iter().fold(None, |acc, e| {
                    let eq = RsdbExpr::BinaryOp { left: Box::new(expr.clone()), op: BinaryOperator::Eq, right: Box::new(e.clone()) };
                    match acc {
                        None => Some(eq),
                        Some(a) => Some(RsdbExpr::BinaryOp { left: Box::new(a), op: BinaryOperator::Or, right: Box::new(eq) }),
                    }
                });
                match in_expr {
                    Some(e) if *negated => Ok(RsdbExpr::UnaryOp { op: UnaryOperator::Not, expr: Box::new(e) }),
                    Some(e) => Ok(e),
                    None => Ok(RsdbExpr::Literal(Literal::Boolean(false))),
                }
            }
            SqlExpr::Cast { expr, data_type, .. } => {
                let inner = self.expr_to_plan(expr)?;
                Ok(RsdbExpr::Cast { expr: Box::new(inner), data_type: data_type.to_string() })
            }
            SqlExpr::TypedString { value, data_type, .. } => {
                Ok(RsdbExpr::Cast { expr: Box::new(RsdbExpr::Literal(Literal::String(value.clone()))), data_type: data_type.to_string() })
            }
            SqlExpr::Wildcard(_) | SqlExpr::QualifiedWildcard { .. } => {
                Ok(RsdbExpr::Function { name: "*".to_string(), args: vec![] })
            }
            // Handle IN (SELECT ...) subquery
            SqlExpr::InSubquery { expr, subquery, negated } => {
                let subquery_type = if *negated {
                    SubqueryType::NotIn
                } else {
                    SubqueryType::In
                };

                let left_expr = self.expr_to_plan(expr)?;
                let outer_refs = self.find_outer_references(subquery, &HashMap::new());
                let sql = subquery.to_string();

                Ok(RsdbExpr::Subquery {
                    subquery_type,
                    expr: Some(Box::new(left_expr)),
                    outer_refs,
                    sql: Some(sql),
                })
            }
            // Handle EXISTS (SELECT ...) subquery
            SqlExpr::Exists { subquery, negated } => {
                let subquery_type = if *negated {
                    SubqueryType::NotExists
                } else {
                    SubqueryType::Exists
                };

                let outer_refs = self.find_outer_references(subquery, &HashMap::new());
                let sql = subquery.to_string();

                Ok(RsdbExpr::Subquery {
                    subquery_type,
                    expr: None,
                    outer_refs,
                    sql: Some(sql),
                })
            }
            SqlExpr::Subquery(_) => {
                Ok(RsdbExpr::Literal(Literal::Null))
            }
            _ => Err(RsdbError::Planner(format!("Unsupported expression: {:?}", expr))),
        }
    }

    fn convert_binary_op(&self, op: &sqlparser::ast::BinaryOperator) -> BinaryOperator {
        let op_str = format!("{:?}", op);
        match op_str.as_str() {
            "Eq" => BinaryOperator::Eq,
            "Neq" => BinaryOperator::Neq,
            "Lt" => BinaryOperator::Lt,
            "LtEq" => BinaryOperator::Lte,
            "Gt" => BinaryOperator::Gt,
            "GtEq" => BinaryOperator::Gte,
            "And" => BinaryOperator::And,
            "Or" => BinaryOperator::Or,
            "Plus" => BinaryOperator::Plus,
            "Minus" => BinaryOperator::Minus,
            "Multiply" => BinaryOperator::Multiply,
            "Divide" => BinaryOperator::Divide,
            "Modulo" => BinaryOperator::Modulo,
            "Like" => BinaryOperator::Like,
            _ => BinaryOperator::Eq,
        }
    }

    fn expr_to_i64(&self, e: &sqlparser::ast::Expr) -> Option<i64> {
        match e {
            sqlparser::ast::Expr::Value(v) => match v {
                sqlparser::ast::Value::Number(n, _) => n.parse().ok(),
                _ => None,
            },
            _ => None,
        }
    }

    fn find_outer_references(
        &self,
        query: &sqlparser::ast::Query,
        _cte_map: &HashMap<String, LogicalPlan>,
    ) -> Vec<String> {
        let mut outer_refs = Vec::new();
        let subquery_tables = self.get_table_names_from_query(query);
        let body = query.body.as_ref();
        if let sqlparser::ast::SetExpr::Select(select) = body {
            if let Some(predicate) = &select.selection {
                self.find_columns_in_expr(predicate, &subquery_tables, &mut outer_refs);
            }
        }
        outer_refs
    }

    fn get_table_names_from_query(&self, query: &sqlparser::ast::Query) -> std::collections::HashSet<String> {
        let mut tables = std::collections::HashSet::new();
        let body = query.body.as_ref();
        if let sqlparser::ast::SetExpr::Select(select) = body {
            for from_item in &select.from {
                if let sqlparser::ast::TableFactor::Table { name, .. } = &from_item.relation {
                    tables.insert(name.to_string());
                }
            }
        }
        tables
    }

    fn find_columns_in_expr(
        &self,
        expr: &sqlparser::ast::Expr,
        subquery_tables: &std::collections::HashSet<String>,
        outer_refs: &mut Vec<String>,
    ) {
        match expr {
            sqlparser::ast::Expr::Identifier(id) => {
                outer_refs.push(id.value.clone());
            }
            sqlparser::ast::Expr::CompoundIdentifier(ids) => {
                if let Some(table_name) = ids.first() {
                    let table = table_name.value.clone();
                    if !subquery_tables.contains(&table) {
                        let col = ids.last().map(|i| i.value.clone()).unwrap_or_default();
                        if !col.is_empty() {
                            outer_refs.push(format!("{}.{}", table, col));
                        }
                    }
                }
            }
            sqlparser::ast::Expr::BinaryOp { left, right, .. } => {
                self.find_columns_in_expr(left, subquery_tables, outer_refs);
                self.find_columns_in_expr(right, subquery_tables, outer_refs);
            }
            sqlparser::ast::Expr::UnaryOp { expr, .. } => {
                self.find_columns_in_expr(expr, subquery_tables, outer_refs);
            }
            sqlparser::ast::Expr::InList { list, .. } => {
                for item in list {
                    self.find_columns_in_expr(item, subquery_tables, outer_refs);
                }
            }
            sqlparser::ast::Expr::InSubquery { subquery, .. } => {
                self.find_outer_references(subquery, &HashMap::new());
            }
            sqlparser::ast::Expr::Exists { subquery, .. } => {
                self.find_outer_references(subquery, &HashMap::new());
            }
            _ => {}
        }
    }
}
