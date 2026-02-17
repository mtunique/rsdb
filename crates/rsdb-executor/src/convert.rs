//! RSDB LogicalPlan to DataFusion LogicalPlan direct conversion
//!
//! Converts RSDB's own LogicalPlan representation into DataFusion's LogicalPlan
//! so it can be executed by DataFusion's physical execution engine without
//! going through SQL string conversion.

use datafusion::catalog::TableProvider;
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::datasource::provider_as_source;
use datafusion::functions_aggregate::expr_fn as agg_fn;
use datafusion::logical_expr::{
    table_scan, BinaryExpr, Cast, Like, LogicalPlan, LogicalPlanBuilder, Operator, SortExpr,
    SubqueryAlias, expr::ScalarFunction,
};
use datafusion::prelude::{col, lit, Expr, JoinType, SessionContext};
use futures::FutureExt;
use rsdb_common::{Result, RsdbError};
use rsdb_sql::expr::{
    AggFunction as RsdbAgg, BinaryOperator as RsdbBinOp, Expr as RsdbExpr, Literal,
    UnaryOperator as RsdbUnaryOp,
};
use rsdb_sql::logical_plan::{
    JoinCondition as RsdbJoinCond, JoinType as RsdbJoinType, LogicalPlan as RsdbLogicalPlan,
};
use std::sync::Arc;

/// Convert RSDB LogicalPlan to DataFusion LogicalPlan.
pub fn to_datafusion_plan(plan: &RsdbLogicalPlan, ctx: &SessionContext) -> Result<LogicalPlan> {
    match plan {
        RsdbLogicalPlan::Scan {
            table_name,
            alias,
            schema,
            projection,
            filters,
        } => {
            let table_provider = resolve_table_provider(ctx, table_name);

            let builder = if let Some(tp) = table_provider {
                let real_schema = Arc::new(tp.schema().as_ref().clone());
                let projection_idx: Option<Vec<usize>> = projection.as_ref().map(|cols| {
                    cols.iter()
                        .filter_map(|c| real_schema.index_of(c).ok())
                        .collect()
                });
                let source = provider_as_source(tp);
                LogicalPlanBuilder::scan(table_name.as_str(), source, projection_idx)
                    .map_err(|e| RsdbError::Execution(format!("scan error: {e}")))?
            } else {
                let projection_idx: Option<Vec<usize>> = projection.as_ref().map(|cols| {
                    cols.iter()
                        .filter_map(|c| schema.index_of(c).ok())
                        .collect()
                });
                table_scan(Some(table_name.as_str()), schema, projection_idx)
                    .map_err(|e| RsdbError::Execution(format!("table_scan error: {e}")))?
            };

            let mut builder = builder;
            if let Some(a) = alias {
                builder = builder.alias(a.as_str())
                    .map_err(|e| RsdbError::Execution(format!("alias error: {e}")))?
            }

            for filter in filters {
                let df_expr = to_datafusion_expr(filter, ctx)?;
                builder = builder
                    .filter(df_expr)
                    .map_err(|e| RsdbError::Execution(format!("filter error: {e}")))?;
            }

            builder
                .build()
                .map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Filter { input, predicate } => {
            let input_plan = to_datafusion_plan(input, ctx)?;
            let df_predicate = to_datafusion_expr(predicate, ctx)?;
            LogicalPlanBuilder::from(input_plan)
                .filter(df_predicate)
                .map_err(|e| RsdbError::Execution(format!("filter error: {e}")))?
                .build()
                .map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Project {
            input,
            expr: exprs,
            schema: _,
        } => {
            let input_plan = to_datafusion_plan(input, ctx)?;
            let df_exprs: Result<Vec<_>> =
                exprs.iter().map(|e| to_datafusion_expr(e, ctx)).collect();
            LogicalPlanBuilder::from(input_plan)
                .project(df_exprs?)
                .map_err(|e| RsdbError::Execution(format!("project error: {e}")))?
                .build()
                .map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Aggregate {
            input,
            group_expr,
            aggregate_expr,
            schema: _,
        } => {
            let input_plan = to_datafusion_plan(input, ctx)?;
            let df_group: Result<Vec<_>> = group_expr
                .iter()
                .map(|e| to_datafusion_expr(e, ctx))
                .collect();
            let df_group = df_group?;

            let df_agg: Result<Vec<_>> = aggregate_expr
                .iter()
                .map(|e| to_datafusion_expr(e, ctx))
                .collect();
            let df_agg = df_agg?;

            // Filter out aggregate expressions that are already in group expressions
            let group_set: std::collections::HashSet<String> = df_group.iter().map(|e| format!("{:?}", e)).collect();
            let df_agg_filtered: Vec<_> = df_agg.into_iter().filter(|e| {
                let name = format!("{:?}", e);
                !group_set.contains(&name)
            }).collect();

            LogicalPlanBuilder::from(input_plan)
                .aggregate(df_group, df_agg_filtered)
                .map_err(|e| RsdbError::Execution(format!("aggregate error: {e}")))?
                .build()
                .map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Sort { input, expr: exprs } => {
            let input_plan = to_datafusion_plan(input, ctx)?;
            let df_sorts: Result<Vec<SortExpr>> = exprs
                .iter()
                .map(|e| {
                    let df_expr = to_datafusion_expr(e, ctx)?;
                    Ok(df_expr.sort(true, false))
                })
                .collect();
            LogicalPlanBuilder::from(input_plan)
                .sort(df_sorts?)
                .map_err(|e| RsdbError::Execution(format!("sort error: {e}")))?
                .build()
                .map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let input_plan = to_datafusion_plan(input, ctx)?;
            LogicalPlanBuilder::from(input_plan)
                .limit(*offset, Some(*limit))
                .map_err(|e| RsdbError::Execution(format!("limit error: {e}")))?
                .build()
                .map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Join {
            left,
            right,
            join_type,
            join_condition,
            schema: _,
        } => {
            let left_plan = to_datafusion_plan(left, ctx)?;
            let right_plan = to_datafusion_plan(right, ctx)?;
            let df_join_type = to_df_join_type(join_type);

            match join_condition {
                RsdbJoinCond::On(on_expr) => {
                    let left_schema = left_plan.schema();
                    let right_schema = right_plan.schema();

                    let df_expr = to_datafusion_expr_qualified(
                        on_expr,
                        left_schema.as_ref(),
                        right_schema.as_ref(),
                        ctx,
                    )?;

                    let join_plan = LogicalPlanBuilder::from(left_plan.clone())
                        .join_on(right_plan.clone(), df_join_type, vec![df_expr.clone()])
                        .map_err(|e| RsdbError::Execution(format!("join_on error: {e}")))?
                        .build()
                        .map_err(|e| RsdbError::Execution(format!("join build error: {e}")))?;

                    Ok(join_plan)
                }
                RsdbJoinCond::Using(names) => {
                    let using_cols: Vec<Column> =
                        names.iter().map(|s| Column::from_name(s.clone())).collect();
                    LogicalPlanBuilder::from(left_plan)
                        .join_using(right_plan, df_join_type, using_cols)
                        .map_err(|e| RsdbError::Execution(format!("join_using error: {e}")))?
                        .build()
                        .map_err(|e| RsdbError::Execution(format!("build error: {e}")))
                }
                RsdbJoinCond::None => LogicalPlanBuilder::from(left_plan)
                    .cross_join(right_plan)
                    .map_err(|e| RsdbError::Execution(format!("cross_join error: {e}")))?
                    .build()
                    .map_err(|e| RsdbError::Execution(format!("build error: {e}"))),
            }
        }

        RsdbLogicalPlan::CrossJoin {
            left,
            right,
            schema: _,
        } => {
            let left_plan = to_datafusion_plan(left, ctx)?;
            let right_plan = to_datafusion_plan(right, ctx)?;
            LogicalPlanBuilder::from(left_plan)
                .cross_join(right_plan)
                .map_err(|e| RsdbError::Execution(format!("cross_join error: {e}")))?
                .build()
                .map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Union { inputs, schema: _ } => {
            let plans: Result<Vec<_>> = inputs.iter().map(|p| to_datafusion_plan(p, ctx)).collect();
            let plans = plans?;
            if plans.is_empty() {
                return LogicalPlanBuilder::empty(false)
                    .build()
                    .map_err(|e| RsdbError::Execution(format!("build error: {e}")));
            }
            let mut builder = LogicalPlanBuilder::from(plans[0].clone());
            for p in plans.iter().skip(1) {
                builder = builder
                    .union(p.clone())
                    .map_err(|e| RsdbError::Execution(format!("union error: {e}")))?;
            }
            builder
                .build()
                .map_err(|e| RsdbError::Execution(format!("build error: {e}")))
        }

        RsdbLogicalPlan::Subquery { query, schema: _ } => {
            let plan = to_datafusion_plan(query, ctx)?;
            // Use alias "subquery" for consistency
            Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(plan),
                "subquery",
            ).map_err(|e| RsdbError::Execution(format!("subquery alias error: {e}")))?))
        }

        RsdbLogicalPlan::EmptyRelation => LogicalPlanBuilder::empty(false)
            .build()
            .map_err(|e| RsdbError::Execution(format!("build error: {e}"))),

        RsdbLogicalPlan::CreateTable { .. }
        | RsdbLogicalPlan::DropTable { .. }
        | RsdbLogicalPlan::Insert { .. }
        | RsdbLogicalPlan::Analyze { .. } => Err(RsdbError::Execution(
            "DDL/DML operations not supported in direct plan conversion".to_string(),
        )),
    }
}

/// Convert RSDB Expression to DataFusion Expression with table qualification.
#[allow(clippy::only_used_in_recursion)]
fn to_datafusion_expr_qualified(
    expr: &RsdbExpr,
    _left_schema: &DFSchema,
    _right_schema: &DFSchema,
    ctx: &SessionContext,
) -> Result<Expr> {
    to_datafusion_expr(expr, ctx)
}

/// Convert RSDB Expression to DataFusion Expression
pub fn to_datafusion_expr(expr: &RsdbExpr, ctx: &SessionContext) -> Result<Expr> {
    match expr {
        RsdbExpr::Column(name) => {
            if name.contains('.') {
                let parts: Vec<&str> = name.split('.').collect();
                // If it's a compound identifier like nation.n_nationkey
                if parts.len() >= 2 {
                    Ok(Expr::Column(Column::new(Some(parts[0]), parts[1])))
                } else {
                    Ok(col(name.as_str()))
                }
            } else {
                Ok(col(name.as_str()))
            }
        }

        RsdbExpr::Literal(l) => Ok(to_df_literal(l)),

        RsdbExpr::BinaryOp { left, op, right } => {
            let l = to_datafusion_expr(left, ctx)?;
            let r = to_datafusion_expr(right, ctx)?;

            if matches!(op, RsdbBinOp::Like) {
                return Ok(Expr::Like(Like::new(
                    false,
                    Box::new(l),
                    Box::new(r),
                    None,
                    false,
                )));
            }

            let df_op = to_df_binary_op(op);
            Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(l),
                df_op,
                Box::new(r),
            )))
        }

        RsdbExpr::UnaryOp { op, expr: inner } => {
            let inner_expr = to_datafusion_expr(inner, ctx)?;
            match op {
                RsdbUnaryOp::Not => Ok(Expr::Not(Box::new(inner_expr))),
                RsdbUnaryOp::Minus => Ok(Expr::Negative(Box::new(inner_expr))),
                RsdbUnaryOp::Plus => Ok(inner_expr),
            }
        }

        RsdbExpr::AggFunc {
            func,
            args,
            distinct,
        } => {
            let df_args: Result<Vec<_>> = args.iter().map(|a| to_datafusion_expr(a, ctx)).collect();
            let df_args = df_args?;

            let first_arg = df_args.into_iter().next().unwrap_or_else(|| lit(1i64));

            match (func, *distinct) {
                (RsdbAgg::Count, false) => Ok(agg_fn::count(first_arg)),
                (RsdbAgg::Count, true) | (RsdbAgg::CountDistinct, _) => {
                    Ok(agg_fn::count_distinct(first_arg))
                }
                (RsdbAgg::Sum, false) => Ok(agg_fn::sum(first_arg)),
                (RsdbAgg::Sum, true) => Ok(agg_fn::sum_distinct(first_arg)),
                (RsdbAgg::Avg, false) => Ok(agg_fn::avg(first_arg)),
                (RsdbAgg::Avg, true) => Ok(agg_fn::avg_distinct(first_arg)),
                (RsdbAgg::Min, _) => Ok(agg_fn::min(first_arg)),
                (RsdbAgg::Max, _) => Ok(agg_fn::max(first_arg)),
            }
        }

        RsdbExpr::Alias { expr: inner, name } => {
            let inner_expr = to_datafusion_expr(inner, ctx)?;
            Ok(inner_expr.alias(name.as_str()))
        }

        RsdbExpr::Function { name, args } => {
            let df_args: Result<Vec<_>> = args.iter().map(|a| to_datafusion_expr(a, ctx)).collect();
            let df_args = df_args?;
            
            if name.to_lowercase() == "extract" && df_args.len() == 2 {
                return scalar_function_expr(ctx, "date_part", df_args);
            }
            
            scalar_function_expr(ctx, name, df_args)
        }

        RsdbExpr::Cast {
            expr: inner,
            data_type,
        } => {
            let inner_expr = to_datafusion_expr(inner, ctx)?;
            let dt = parse_data_type(data_type)?;
            Ok(Expr::Cast(Cast::new(Box::new(inner_expr), dt)))
        }

        RsdbExpr::Subquery { .. } => Ok(lit(true)),
    }
}

/// Convert RSDB Literal to DataFusion Expr::Literal
fn to_df_literal(l: &Literal) -> Expr {
    match l {
        Literal::Null => lit(ScalarValue::Null),
        Literal::Boolean(b) => lit(*b),
        Literal::Int(i) => lit(*i),
        Literal::Float(f) => lit(*f),
        Literal::String(s) => lit(s.as_str()),
    }
}

/// Convert RSDB BinaryOperator to DataFusion Operator
fn to_df_binary_op(op: &RsdbBinOp) -> Operator {
    match op {
        RsdbBinOp::Eq => Operator::Eq,
        RsdbBinOp::Neq => Operator::NotEq,
        RsdbBinOp::Lt => Operator::Lt,
        RsdbBinOp::Lte => Operator::LtEq,
        RsdbBinOp::Gt => Operator::Gt,
        RsdbBinOp::Gte => Operator::GtEq,
        RsdbBinOp::And => Operator::And,
        RsdbBinOp::Or => Operator::Or,
        RsdbBinOp::Plus => Operator::Plus,
        RsdbBinOp::Minus => Operator::Minus,
        RsdbBinOp::Multiply => Operator::Multiply,
        RsdbBinOp::Divide => Operator::Divide,
        RsdbBinOp::Modulo => Operator::Modulo,
        RsdbBinOp::Like => Operator::LikeMatch,
    }
}

/// Convert RSDB JoinType to DataFusion JoinType
fn to_df_join_type(jt: &RsdbJoinType) -> JoinType {
    match jt {
        RsdbJoinType::Inner => JoinType::Inner,
        RsdbJoinType::Left => JoinType::Left,
        RsdbJoinType::Right => JoinType::Right,
        RsdbJoinType::Full => JoinType::Full,
        RsdbJoinType::Cross => JoinType::Inner,
        RsdbJoinType::LeftSemi => JoinType::LeftSemi,
        RsdbJoinType::RightSemi => JoinType::RightSemi,
        RsdbJoinType::LeftAnti => JoinType::LeftAnti,
        RsdbJoinType::RightAnti => JoinType::RightAnti,
    }
}

/// Resolve the real TableProvider from DataFusion's catalog.
fn resolve_table_provider(
    ctx: &SessionContext,
    table_name: &str,
) -> Option<Arc<dyn TableProvider>> {
    let state = ctx.state();
    let catalog_list = state.catalog_list();
    for catalog_name in catalog_list.catalog_names() {
        if let Some(catalog) = catalog_list.catalog(&catalog_name) {
            for schema_name in catalog.schema_names() {
                if let Some(schema_provider) = catalog.schema(&schema_name) {
                    let result = schema_provider.table(table_name).now_or_never();
                    if let Some(Ok(Some(tp))) = result {
                        return Some(tp);
                    }
                }
            }
        }
    }
    None
}

/// Resolve a scalar function by name from DataFusion's registry
fn scalar_function_expr(ctx: &SessionContext, name: &str, args: Vec<Expr>) -> Result<Expr> {
    let state = ctx.state();
    let name_lower = name.to_lowercase();
    if let Some(udf) = state
        .scalar_functions()
        .get(name)
        .or_else(|| state.scalar_functions().get(&name_lower))
    {
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::clone(udf),
            args,
        )))
    } else {
        Err(RsdbError::Execution(format!(
            "Unknown scalar function: {name}"
        )))
    }
}

/// Parse a data type string to Arrow DataType
fn parse_data_type(dt: &str) -> Result<arrow_schema::DataType> {
    use arrow_schema::{DataType, TimeUnit};
    let dt_lower = dt.to_lowercase();
    let result = match dt_lower.as_str() {
        "tinyint" | "int8" => DataType::Int8,
        "smallint" | "int16" => DataType::Int16,
        "int" | "integer" | "int32" => DataType::Int32,
        "bigint" | "int64" => DataType::Int64,
        "float" | "real" | "float32" => DataType::Float32,
        "double" | "float64" => DataType::Float64,
        "varchar" | "string" | "text" | "utf8" | "char" => DataType::Utf8,
        "boolean" | "bool" => DataType::Boolean,
        "date" | "date32" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        s if s.starts_with("decimal") || s.starts_with("numeric") => DataType::Decimal128(38, 10),
        _ => DataType::Utf8,
    };
    Ok(result)
}
