//! Logical Query Plan representation

use crate::expr::Expr;
use arrow_schema::{Schema, SchemaRef, Field};
use std::sync::Arc;
use serde::{Serialize, Deserialize};

/// Logical Query Plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    /// Scan a table with optional projection and filters
    Scan {
        table_name: String,
        schema: SchemaRef,
        projection: Option<Vec<String>>,
        filters: Vec<Expr>,
    },

    /// Filter rows
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },

    /// Project columns
    Project {
        input: Box<LogicalPlan>,
        expr: Vec<Expr>,
        schema: SchemaRef,
    },

    /// Aggregate data
    Aggregate {
        input: Box<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggregate_expr: Vec<Expr>,
        schema: SchemaRef,
    },

    /// Sort data
    Sort {
        input: Box<LogicalPlan>,
        expr: Vec<Expr>,
    },

    /// Limit rows
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
        offset: usize,
    },

    /// Join two relations
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        join_condition: JoinCondition,
        schema: SchemaRef,
    },

    /// Cross Join two relations
    CrossJoin {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        schema: SchemaRef,
    },

    /// Union relations
    Union {
        inputs: Vec<LogicalPlan>,
        schema: SchemaRef,
    },

    /// Subquery
    Subquery {
        query: Box<LogicalPlan>,
        schema: SchemaRef,
    },

    /// Alias for a subquery or relation
    SubqueryAlias {
        input: Box<LogicalPlan>,
        alias: String,
    },

    /// Create table DDL
    CreateTable {
        table_name: String,
        schema: SchemaRef,
        partition_keys: Vec<String>,
        if_not_exists: bool,
    },

    /// Drop table DDL
    DropTable {
        table_name: String,
        if_exists: bool,
    },

    /// Insert into table
    Insert {
        table_name: String,
        source: Box<LogicalPlan>,
    },

    /// Analyze table
    Analyze {
        table_name: String,
    },

    /// Logical Exchange (Repartitioning)
    Exchange {
        input: Box<LogicalPlan>,
        partitioning: Partitioning,
    },

    /// Remote Exchange (Physical data movement)
    RemoteExchange {
        input: Box<LogicalPlan>,
        partitioning: Partitioning,
        sources: Vec<RemoteSource>,
    },

    /// Empty relation (Dual)
    EmptyRelation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinCondition {
    On(Expr),
    Using(Vec<String>),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Partitioning {
    RoundRobin(usize),
    Hash(Vec<Expr>, usize),
    Broadcast,
    Single,
    Any,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteSource {
    pub worker_addr: String,
    pub task_id: String,
}

impl LogicalPlan {
    /// Get the schema (output columns) of this plan
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Scan { schema, .. } => schema.clone(),
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Project { schema, .. } => schema.clone(),
            LogicalPlan::Aggregate { schema, .. } => schema.clone(),
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::Join { schema, .. } => schema.clone(),
            LogicalPlan::CrossJoin { schema, .. } => schema.clone(),
            LogicalPlan::Union { schema, .. } => schema.clone(),
            LogicalPlan::Subquery { schema, .. } => schema.clone(),
            LogicalPlan::SubqueryAlias { input, alias } => {
                let schema = input.schema();
                let fields: Vec<_> = schema.fields().iter().map(|f| {
                    Field::new(format!("{}.{}", alias, f.name()), f.data_type().clone(), f.is_nullable())
                }).collect();
                Arc::new(Schema::new(fields))
            }
            LogicalPlan::CreateTable { schema, .. } => schema.clone(),
            LogicalPlan::DropTable { .. } => Arc::new(Schema::empty()),
            LogicalPlan::Insert { source, .. } => source.schema(),
            LogicalPlan::Analyze { .. } => Arc::new(Schema::empty()),
            LogicalPlan::Exchange { input, .. } | LogicalPlan::RemoteExchange { input, .. } => input.schema(),
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
            LogicalPlan::SubqueryAlias { input, .. } => input.table_refs(),
            LogicalPlan::CreateTable { .. } => vec![],
            LogicalPlan::DropTable { table_name, .. } => vec![table_name.clone()],
            LogicalPlan::Insert { source, .. } => source.table_refs(),
            LogicalPlan::Analyze { table_name } => vec![table_name.clone()],
            LogicalPlan::Exchange { input, .. } | LogicalPlan::RemoteExchange { input, .. } => input.table_refs(),
            LogicalPlan::EmptyRelation => vec![],
        }
    }
}
