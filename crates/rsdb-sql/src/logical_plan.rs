//! RSDB Logical Plan

use crate::expr::Expr;
use arrow_schema::{Schema, SchemaRef, Field};
use std::sync::Arc;

/// Logical plan node - represents a query execution plan
#[derive(Debug, Clone)]
// TODO: Add serde support when arrow-schema supports Serialize/Deserialize for SchemaRef
pub enum LogicalPlan {
    /// Scan a table with optional projection and filters
    Scan {
        table_name: String,
        alias: Option<String>,
        schema: SchemaRef,
        projection: Option<Vec<String>>,
        filters: Vec<Expr>,
    },

    /// Filter rows based on predicate
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },

    /// Project columns (select specific columns/expressions)
    Project {
        input: Box<LogicalPlan>,
        expr: Vec<Expr>,
        schema: SchemaRef,
    },

    /// Aggregate with group by
    Aggregate {
        input: Box<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggregate_expr: Vec<Expr>,
        schema: SchemaRef,
    },

    /// Sort by expressions
    Sort {
        input: Box<LogicalPlan>,
        expr: Vec<Expr>,
    },

    /// Limit number of rows
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
        offset: usize,
    },

    /// Table join
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        join_condition: JoinCondition,
        schema: SchemaRef,
    },

    /// Cross join (Cartesian product)
    CrossJoin {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        schema: SchemaRef,
    },

    /// Union of two plans
    Union {
        inputs: Vec<LogicalPlan>,
        schema: SchemaRef,
    },

    /// Subquery
    Subquery {
        query: Box<LogicalPlan>,
        schema: SchemaRef,
    },

    /// Create table DDL
    CreateTable {
        table_name: String,
        schema: SchemaRef,
        partition_keys: Vec<String>,
        if_not_exists: bool,
    },

    /// Drop table DDL
    DropTable { table_name: String, if_exists: bool },

    /// Insert data
    Insert {
        table_name: String,
        source: Box<LogicalPlan>,
    },

    /// Analyze table - compute statistics
    Analyze { table_name: String },

    /// Empty relation (for DDL that doesn't return rows)
    EmptyRelation,
}

/// Join type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

/// Join condition
#[derive(Debug, Clone)]
pub enum JoinCondition {
    On(Expr),
    Using(Vec<String>),
    None,
}

impl LogicalPlan {
    /// Get the schema (output columns) of this plan
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Scan { schema, alias, .. } => {
                if let Some(a) = alias {
                    // Return schema with aliased field names "alias.column"
                    let fields: Vec<Field> = schema.fields().iter().map(|f| {
                        let name = format!("{}.{}", a, f.name());
                        Field::new(name, f.data_type().clone(), f.is_nullable())
                    }).collect();
                    Arc::new(Schema::new(fields))
                } else {
                    schema.clone()
                }
            }
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Project { schema, .. } => schema.clone(),
            LogicalPlan::Aggregate { schema, .. } => schema.clone(),
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::Join { schema, .. } => schema.clone(),
            LogicalPlan::CrossJoin { schema, .. } => schema.clone(),
            LogicalPlan::Union { schema, .. } => schema.clone(),
            LogicalPlan::Subquery { schema, .. } => schema.clone(),
            LogicalPlan::CreateTable { schema, .. } => schema.clone(),
            LogicalPlan::DropTable { .. } => Arc::new(Schema::empty()),
            LogicalPlan::Insert { source, .. } => source.schema(),
            LogicalPlan::Analyze { .. } => Arc::new(Schema::empty()),
            LogicalPlan::EmptyRelation => Arc::new(Schema::empty()),
        }
    }

    /// Get table references from this plan
    pub fn table_refs(&self) -> Vec<String> {
        match self {
            LogicalPlan::Scan { table_name, .. } => vec![table_name.clone()],
            LogicalPlan::Filter { input, .. } => input.table_refs(),
            LogicalPlan::Project { input, .. } => input.table_refs(),
            LogicalPlan::Aggregate { input, .. } => input.table_refs(),
            LogicalPlan::Sort { input, .. } => input.table_refs(),
            LogicalPlan::Limit { input, .. } => input.table_refs(),
            LogicalPlan::Join { left, right, .. } => {
                let mut refs = left.table_refs();
                refs.extend(right.table_refs());
                refs
            }
            LogicalPlan::CrossJoin { left, right, .. } => {
                let mut refs = left.table_refs();
                refs.extend(right.table_refs());
                refs
            }
            LogicalPlan::Union { inputs, .. } => {
                let mut refs = Vec::new();
                for input in inputs {
                    refs.extend(input.table_refs());
                }
                refs
            }
            LogicalPlan::Subquery { query, .. } => query.table_refs(),
            LogicalPlan::CreateTable { .. } => vec![],
            LogicalPlan::DropTable { table_name, .. } => vec![table_name.clone()],
            LogicalPlan::Insert { source, .. } => source.table_refs(),
            LogicalPlan::Analyze { table_name } => vec![table_name.clone()],
            LogicalPlan::EmptyRelation => vec![],
        }
    }
}
