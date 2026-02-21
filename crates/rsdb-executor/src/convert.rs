//! RSDB LogicalPlan to DataFusion LogicalPlan direct conversion

use datafusion::catalog::TableProvider;
use datafusion::common::{Column, ScalarValue, TableReference};
use datafusion::datasource::provider_as_source;
use datafusion::functions_aggregate::expr_fn as agg_fn;
use datafusion::logical_expr::{
    table_scan, LogicalPlan, LogicalPlanBuilder, Operator, SortExpr,
    SubqueryAlias, expr::ScalarFunction,
};
use datafusion::prelude::{col, lit, Expr, JoinType, SessionContext};
use rsdb_common::{Result, RsdbError};
use rsdb_sql::expr::{
    AggFunction as RsdbAgg, Expr as RsdbExpr, Literal,
};
use rsdb_sql::logical_plan::{
    JoinCondition as RsdbJoinCond, JoinType as RsdbJoinType, LogicalPlan as RsdbLogicalPlan,
};
use std::sync::Arc;

pub fn to_datafusion_plan(plan: &RsdbLogicalPlan, ctx: &SessionContext) -> Result<LogicalPlan> {
    to_datafusion_plan_with_sources(plan, ctx, &[])
}

pub fn to_datafusion_plan_with_sources(
    plan: &RsdbLogicalPlan,
    ctx: &SessionContext,
    remote_sources: &[rsdb_common::rpc::RemoteSource],
) -> Result<LogicalPlan> {
    match plan {
        RsdbLogicalPlan::Scan { table_name, schema, projection, filters } => {
            let projection_idx: Option<Vec<usize>> = projection.as_ref().map(|cols| {
                cols.iter().filter_map(|c| schema.index_of(c).ok()).collect()
            });
            let mut builder = table_scan(Some(table_name.as_str()), &schema, projection_idx)
                .map_err(|e| RsdbError::Execution(format!("scan error: {e}")))?;
            for filter in filters {
                builder = builder.filter(to_datafusion_expr(filter, ctx)?)
                    .map_err(|e| RsdbError::Execution(format!("filter error: {e}")))?;
            }
            let scan = builder.build().map_err(|e| RsdbError::Execution(format!("build error: {e}")))?;
            Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(scan), table_name.clone()).unwrap()))
        }

        RsdbLogicalPlan::Filter { input, predicate } => {
            let input_plan = to_datafusion_plan_with_sources(input, ctx, remote_sources)?;
            LogicalPlanBuilder::from(input_plan).filter(to_datafusion_expr(predicate, ctx)?)
                .map_err(|e| RsdbError::Execution(format!("filter error: {e}")))?
                .build().map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Project { input, expr: exprs, schema } => {
            let input_plan = to_datafusion_plan_with_sources(input, ctx, remote_sources)?;
            let df_exprs: Result<Vec<_>> = exprs.iter().enumerate().map(|(i, e)| {
                let df_e = to_datafusion_expr(e, ctx)?;
                Ok(df_e.alias(schema.field(i).name()))
            }).collect();
            LogicalPlanBuilder::from(input_plan).project(df_exprs?)
                .map_err(|e| RsdbError::Execution(format!("project error: {e}")))?
                .build().map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Aggregate { input, group_expr, aggregate_expr, schema } => {
            let input_plan = to_datafusion_plan_with_sources(input, ctx, remote_sources)?;
            let df_group: Result<Vec<_>> = group_expr.iter().enumerate().map(|(i, e)| {
                let df_e = to_datafusion_expr(e, ctx)?;
                Ok(df_e.alias(schema.field(i).name()))
            }).collect();
            let df_agg: Result<Vec<_>> = aggregate_expr.iter().enumerate().map(|(i, e)| {
                let df_e = to_datafusion_expr(e, ctx)?;
                Ok(df_e.alias(schema.field(group_expr.len() + i).name()))
            }).collect();

            LogicalPlanBuilder::from(input_plan).aggregate(df_group?, df_agg?)
                .map_err(|e| RsdbError::Execution(format!("aggregate error: {e}")))?
                .build().map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Join { left, right, join_type, join_condition, .. } => {
            let left_plan = to_datafusion_plan_with_sources(left, ctx, remote_sources)?;
            let right_plan = to_datafusion_plan_with_sources(right, ctx, remote_sources)?;
            let df_join_type = match join_type {
                RsdbJoinType::Inner => JoinType::Inner,
                RsdbJoinType::Left => JoinType::Left,
                RsdbJoinType::Right => JoinType::Right,
                RsdbJoinType::Full => JoinType::Full,
                _ => JoinType::Inner,
            };
            let mut builder = LogicalPlanBuilder::from(left_plan);
            match join_condition {
                RsdbJoinCond::On(expr) => {
                    builder = builder.join(right_plan, df_join_type, (Vec::<Column>::new(), Vec::<Column>::new()), Some(to_datafusion_expr(expr, ctx)?))
                        .map_err(|e| RsdbError::Execution(format!("join error: {e}")))?;
                }
                _ => { builder = builder.cross_join(right_plan).map_err(|e| RsdbError::Execution(format!("cross join error: {e}")))?; }
            }
            builder.build().map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::CrossJoin { left, right, .. } => {
            let left_plan = to_datafusion_plan_with_sources(left, ctx, remote_sources)?;
            let right_plan = to_datafusion_plan_with_sources(right, ctx, remote_sources)?;
            LogicalPlanBuilder::from(left_plan).cross_join(right_plan)
                .map_err(|e| RsdbError::Execution(format!("cross join error: {e}")))?
                .build().map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::SubqueryAlias { input, alias } => {
            let input_plan = to_datafusion_plan_with_sources(input, ctx, remote_sources)?;
            Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(input_plan), alias.clone()).unwrap()))
        }

        RsdbLogicalPlan::Sort { input, expr } => {
            let input_plan = to_datafusion_plan_with_sources(input, ctx, remote_sources)?;
            let df_sorts: Result<Vec<SortExpr>> = expr.iter().map(|e| Ok(to_datafusion_expr(e, ctx)?.sort(true, false))).collect();
            LogicalPlanBuilder::from(input_plan).sort(df_sorts?)
                .map_err(|e| RsdbError::Execution(format!("sort error: {e}")))?
                .build().map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::RemoteExchange { input, sources, .. } => {
            use crate::physical_plan::shuffle_reader::{PartitionLocation, RemoteExchangeProvider};
            let effective_sources = if !sources.is_empty() { sources.clone() } else {
                remote_sources.iter().map(|s| rsdb_sql::logical_plan::RemoteSource { worker_addr: s.worker_addr.clone(), task_id: s.task_id.clone() }).collect()
            };
            let locations: Vec<PartitionLocation> = effective_sources.iter().map(|s| PartitionLocation { worker_addr: s.worker_addr.clone(), ticket: s.task_id.as_bytes().to_vec() }).collect();
            let input_df_plan = to_datafusion_plan_with_sources(input, ctx, remote_sources)?;
            let provider = Arc::new(RemoteExchangeProvider { partitions: vec![locations], schema: Arc::new(input_df_plan.schema().as_arrow().clone()) });
            LogicalPlanBuilder::scan("remote_exchange", provider_as_source(provider), None)
                .map_err(|e| RsdbError::Execution(format!("scan error: {e}")))?
                .build().map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Union { inputs, .. } => {
            if inputs.is_empty() {
                 return Ok(LogicalPlanBuilder::empty(false).build().unwrap());
            }
            let mut df_inputs = Vec::new();
            for input in inputs {
                df_inputs.push(to_datafusion_plan_with_sources(input, ctx, remote_sources)?);
            }

            let mut builder = LogicalPlanBuilder::from(df_inputs[0].clone());
            for i in 1..df_inputs.len() {
                builder = builder.union(df_inputs[i].clone())
                    .map_err(|e| RsdbError::Execution(format!("union error: {e}")))?;
            }
            builder.build().map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        _ => Ok(LogicalPlanBuilder::empty(false).build().unwrap()),
    }
}

pub fn to_datafusion_expr(expr: &RsdbExpr, ctx: &SessionContext) -> Result<Expr> {
    match expr {
        RsdbExpr::Column(name) => {
            // Planner produces flat names with dots (e.g. "customer.c_acctbal" or "custsale.customer.c_acctbal").
            // DataFusion's Column struct has separate relation and name fields.
            // We split at the first dot to match DataFusion's SubqueryAlias behavior where the alias becomes the relation.
            if let Some(i) = name.find('.') {
                let relation = &name[..i];
                let column = &name[i+1..];
                Ok(Expr::Column(datafusion::common::Column::new(Some(relation), column)))
            } else {
                Ok(col(name))
            }
        }
        RsdbExpr::Literal(l) => Ok(match l {
            Literal::Int(i) => lit(*i),
            Literal::Float(f) => lit(*f),
            Literal::String(s) => lit(s.as_str()),
            Literal::Boolean(b) => lit(*b),
            Literal::Null => lit(ScalarValue::Null),
        }),
        RsdbExpr::BinaryOp { left, op, right } => {
            let l = to_datafusion_expr(left, ctx)?;
            let r = to_datafusion_expr(right, ctx)?;
            let df_op = match format!("{:?}", op).as_str() {
                "Eq" => Operator::Eq, "Neq" => Operator::NotEq, "Lt" => Operator::Lt, "Gt" => Operator::Gt,
                "Lte" => Operator::LtEq, "Gte" => Operator::GtEq, "And" => Operator::And, "Or" => Operator::Or,
                "Plus" => Operator::Plus, "Minus" => Operator::Minus, "Multiply" => Operator::Multiply, "Divide" => Operator::Divide,
                _ => Operator::Eq,
            };
            Ok(Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(Box::new(l), df_op, Box::new(r))))
        }
        RsdbExpr::AggFunc { func, args, distinct } => {
            let arg = args.first().map(|a| to_datafusion_expr(a, ctx)).transpose()?.unwrap_or_else(|| lit(1i64));
            match (func, *distinct) {
                (RsdbAgg::Count, _) => Ok(agg_fn::count(arg)),
                (RsdbAgg::Sum, _) => Ok(agg_fn::sum(arg)),
                (RsdbAgg::Avg, _) => Ok(agg_fn::avg(arg)),
                (RsdbAgg::Min, _) => Ok(agg_fn::min(arg)),
                (RsdbAgg::Max, _) => Ok(agg_fn::max(arg)),
                _ => Ok(agg_fn::count(arg)),
            }
        }
        RsdbExpr::Alias { expr: inner, name } => {
            Ok(to_datafusion_expr(inner, ctx)?.alias(name))
        }
        RsdbExpr::Function { name, args } => {
            let df_args: Result<Vec<_>> = args.iter().map(|a| to_datafusion_expr(a, ctx)).collect();
            let df_args = df_args?;
            if let Some(udf) = ctx.state().scalar_functions().get(name) {
                Ok(Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(udf), df_args)))
            } else {
                Err(RsdbError::Execution(format!("Unknown function: {name}")))
            }
        }
        RsdbExpr::Case { operand, conditions, results, else_result } => {
            let expr = operand.as_ref().map(|o| to_datafusion_expr(o, ctx)).transpose()?.map(Box::new);
            let when_then_pairs = conditions.iter().zip(results.iter())
                .map(|(c, r)| Ok((Box::new(to_datafusion_expr(c, ctx)?), Box::new(to_datafusion_expr(r, ctx)?))))
                .collect::<Result<Vec<_>>>()?;
            let else_expr = else_result.as_ref().map(|e| to_datafusion_expr(e, ctx)).transpose()?.map(Box::new);
            
            Ok(datafusion::logical_expr::Expr::Case(datafusion::logical_expr::Case {
                expr,
                when_then_expr: when_then_pairs,
                else_expr,
            }))
        }
        _ => Ok(lit(true)),
    }
}
