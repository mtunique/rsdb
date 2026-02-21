//! Memo Data Structure
//!
//! The Memo is the core data structure of the Cascades optimizer.
//! It stores a forest of expression trees (AND/OR graph) where
//! logically equivalent expressions are grouped together.

use crate::cbo::Cost;
use crate::property::{Property, Statistics};
use rsdb_sql::logical_plan::{JoinCondition, JoinType, LogicalPlan};
use std::collections::HashMap;

// ============================================================================
// ExprFingerprint
// ============================================================================

/// Fingerprint for memo deduplication.
/// Two expressions with the same fingerprint are structurally identical.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExprFingerprint {
    pub op_tag: String,
    pub op_key: String,
    pub children: Vec<usize>,
}

impl ExprFingerprint {
    pub fn from_plan(plan: &LogicalPlan, children: &[usize]) -> Self {
        let (op_tag, op_key) = match plan {
            LogicalPlan::Scan {
                table_name,
                projection,
                ..
            } => (
                "Scan".to_string(),
                format!("{}:{:?}", table_name, projection),
            ),
            LogicalPlan::Filter { predicate, .. } => {
                ("Filter".to_string(), format!("{:?}", predicate))
            }
            LogicalPlan::Project { expr, .. } => ("Project".to_string(), format!("{:?}", expr)),
            LogicalPlan::Aggregate {
                group_expr,
                aggregate_expr,
                ..
            } => (
                "Aggregate".to_string(),
                format!("{:?}:{:?}", group_expr, aggregate_expr),
            ),
            LogicalPlan::Sort { expr, .. } => ("Sort".to_string(), format!("{:?}", expr)),
            LogicalPlan::Limit { limit, offset, .. } => {
                ("Limit".to_string(), format!("{}:{}", limit, offset))
            }
            LogicalPlan::Join {
                join_type,
                join_condition,
                ..
            } => (
                "Join".to_string(),
                format!("{:?}:{:?}", join_type, join_condition),
            ),
            LogicalPlan::CrossJoin { .. } => ("CrossJoin".to_string(), String::new()),
            LogicalPlan::Union { .. } => ("Union".to_string(), String::new()),
            LogicalPlan::Subquery { .. } => ("Subquery".to_string(), String::new()),
            LogicalPlan::SubqueryAlias { alias, .. } => ("SubqueryAlias".to_string(), alias.clone()),
            LogicalPlan::Exchange { partitioning, .. } => {
                ("Exchange".to_string(), format!("{:?}", partitioning))
            }
            LogicalPlan::RemoteExchange { partitioning, sources, .. } => {
                ("RemoteExchange".to_string(), format!("{:?}:{:?}", partitioning, sources))
            }
            LogicalPlan::CreateTable { table_name, .. } => {
                ("CreateTable".to_string(), table_name.clone())
            }
            LogicalPlan::DropTable { table_name, .. } => {
                ("DropTable".to_string(), table_name.clone())
            }
            LogicalPlan::Insert { table_name, .. } => ("Insert".to_string(), table_name.clone()),
            LogicalPlan::Analyze { table_name } => ("Analyze".to_string(), table_name.clone()),
            LogicalPlan::EmptyRelation => ("EmptyRelation".to_string(), String::new()),
        };
        Self {
            op_tag,
            op_key,
            children: children.to_vec(),
        }
    }
}

// ============================================================================
// Winner
// ============================================================================

/// Winner - best expression for a given property in a group
#[derive(Debug, Clone)]
pub struct Winner {
    pub expr_id: usize,
    pub cost: Cost,
}

// ============================================================================
// GroupExpression
// ============================================================================

/// Expression type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprType {
    Logical,
    Physical,
}

/// Physical implementation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalType {
    TableScan,
    Filter,
    Project,
    HashJoin,
    Aggregate,
    Sort,
    Limit,
    Exchange,
}

/// A logical or physical operator with child groups, stored flat in Memo
#[derive(Debug, Clone)]
pub struct GroupExpression {
    pub id: usize,
    pub group_id: usize,
    pub operator: LogicalPlan,
    pub children: Vec<usize>,
    pub expr_type: ExprType,
    pub physical_type: Option<PhysicalType>,
    /// Bitset tracking which rules have been applied
    pub applied_rules: u64,
    /// Whether this expression has been explored (transformation rules applied)
    pub explored: bool,
    pub cost: Cost,
}

impl GroupExpression {
    pub fn new(id: usize, group_id: usize, operator: LogicalPlan, children: Vec<usize>) -> Self {
        Self {
            id,
            group_id,
            operator,
            children,
            expr_type: ExprType::Logical,
            physical_type: None,
            applied_rules: 0,
            explored: false,
            cost: Cost::infinite(),
        }
    }

    pub fn is_logical(&self) -> bool {
        self.expr_type == ExprType::Logical
    }

    pub fn is_physical(&self) -> bool {
        self.expr_type == ExprType::Physical
    }

    pub fn has_applied_rule(&self, rule_id: usize) -> bool {
        (self.applied_rules & (1u64 << rule_id)) != 0
    }

    pub fn mark_rule_applied(&mut self, rule_id: usize) {
        self.applied_rules |= 1u64 << rule_id;
    }

    pub fn fingerprint(&self) -> ExprFingerprint {
        ExprFingerprint::from_plan(&self.operator, &self.children)
    }

    /// Return the operator tag for rule matching
    pub fn op_tag(&self) -> &str {
        match &self.operator {
            LogicalPlan::Scan { .. } => "Scan",
            LogicalPlan::Filter { .. } => "Filter",
            LogicalPlan::Project { .. } => "Project",
            LogicalPlan::Aggregate { .. } => "Aggregate",
            LogicalPlan::Sort { .. } => "Sort",
            LogicalPlan::Limit { .. } => "Limit",
            LogicalPlan::Join { .. } => "Join",
            LogicalPlan::CrossJoin { .. } => "CrossJoin",
            LogicalPlan::Union { .. } => "Union",
            LogicalPlan::Subquery { .. } => "Subquery",
            LogicalPlan::Exchange { .. } => "Exchange",
            _ => "Other",
        }
    }
}

// ============================================================================
// Group
// ============================================================================

/// Group: a set of logically equivalent expressions
#[derive(Debug, Clone)]
pub struct Group {
    pub id: usize,
    pub expressions: Vec<usize>, // IDs into Memo::expressions
    pub winners: HashMap<Property, Winner>,
    pub statistics: Option<Statistics>,
    pub explored: bool,
}

impl Group {
    pub fn new(id: usize) -> Self {
        Self {
            id,
            expressions: Vec::new(),
            winners: HashMap::new(),
            statistics: None,
            explored: false,
        }
    }

    pub fn has_winner(&self, property: &Property) -> bool {
        self.winners.contains_key(property)
    }

    pub fn get_winner(&self, property: &Property) -> Option<&Winner> {
        self.winners.get(property)
    }

    pub fn get_best_cost(&self, property: &Property) -> Cost {
        self.winners
            .get(property)
            .map(|w| w.cost)
            .unwrap_or(Cost::infinite())
    }
}

// ============================================================================
// Memo
// ============================================================================

/// Memo: stores all groups and expressions with deduplication
#[derive(Debug, Clone, Default)]
pub struct Memo {
    pub groups: HashMap<usize, Group>,
    pub expressions: HashMap<usize, GroupExpression>,
    fingerprint_index: HashMap<ExprFingerprint, (usize, usize)>, // fingerprint -> (group_id, expr_id)
    next_group_id: usize,
    next_expr_id: usize,
    pub root_group_id: Option<usize>,
}

impl Memo {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            expressions: HashMap::new(),
            fingerprint_index: HashMap::new(),
            next_group_id: 0,
            next_expr_id: 0,
            root_group_id: None,
        }
    }

    pub fn create_group(&mut self) -> usize {
        let id = self.next_group_id;
        self.next_group_id += 1;
        self.groups.insert(id, Group::new(id));
        id
    }

    pub fn get_group(&self, id: usize) -> Option<&Group> {
        self.groups.get(&id)
    }

    pub fn get_group_mut(&mut self, id: usize) -> Option<&mut Group> {
        self.groups.get_mut(&id)
    }

    pub fn get_expr(&self, id: usize) -> Option<&GroupExpression> {
        self.expressions.get(&id)
    }

    pub fn get_expr_mut(&mut self, id: usize) -> Option<&mut GroupExpression> {
        self.expressions.get_mut(&id)
    }

    /// Insert an expression. Returns (group_id, expr_id, is_new).
    /// If an expression with the same fingerprint already exists, returns the existing one.
    pub fn insert_expression(
        &mut self,
        group_id: usize,
        operator: LogicalPlan,
        children: Vec<usize>,
        expr_type: ExprType,
        physical_type: Option<PhysicalType>,
    ) -> (usize, usize, bool) {
        let fp = ExprFingerprint::from_plan(&operator, &children);

        // Check for existing expression with same fingerprint
        if let Some(&(existing_group, existing_expr)) = self.fingerprint_index.get(&fp) {
            return (existing_group, existing_expr, false);
        }

        let expr_id = self.next_expr_id;
        self.next_expr_id += 1;

        let mut expr = GroupExpression::new(expr_id, group_id, operator, children);
        expr.expr_type = expr_type;
        expr.physical_type = physical_type;

        self.fingerprint_index.insert(fp, (group_id, expr_id));
        self.expressions.insert(expr_id, expr);

        if let Some(group) = self.groups.get_mut(&group_id) {
            group.expressions.push(expr_id);
        }

        (group_id, expr_id, true)
    }

    /// Add a logical expression to a group
    pub fn add_logical_expr(
        &mut self,
        group_id: usize,
        operator: LogicalPlan,
        children: Vec<usize>,
    ) -> (usize, usize, bool) {
        self.insert_expression(group_id, operator, children, ExprType::Logical, None)
    }

    /// Add a physical expression to a group
    pub fn add_physical_expr(
        &mut self,
        group_id: usize,
        operator: LogicalPlan,
        children: Vec<usize>,
        physical_type: PhysicalType,
    ) -> (usize, usize, bool) {
        self.insert_expression(
            group_id,
            operator,
            children,
            ExprType::Physical,
            Some(physical_type),
        )
    }

    /// Update winner for a group+property. Returns true if updated.
    pub fn update_winner(
        &mut self,
        group_id: usize,
        expr_id: usize,
        cost: Cost,
        property: Property,
    ) -> bool {
        if let Some(group) = self.groups.get_mut(&group_id) {
            if let Some(existing) = group.winners.get(&property) {
                if !cost.is_less_than(&existing.cost) {
                    return false;
                }
            }
            group.winners.insert(property, Winner { expr_id, cost });
            return true;
        }
        false
    }

    pub fn get_root_group(&self) -> Option<&Group> {
        self.root_group_id.and_then(|id| self.groups.get(&id))
    }
}
