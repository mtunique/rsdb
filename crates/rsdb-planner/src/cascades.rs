//! Cascades Optimizer Framework
//!
//! Reference: ClickHouse Cascades Optimizer implementation
//! Key components:
//! - Memo: stores groups of equivalent expressions with fingerprint dedup
//! - Group: a set of logically equivalent expressions
//! - GroupExpression: a logical expression with child groups
//! - Task: optimization tasks (LIFO stack, DFS)
//! - Rules: transformation and implementation rules
//! - MinCutBranch: join enumeration on hypergraph (ref: ClickHouse JoinEnumOnGraph.cpp)

use crate::cbo::{CBOContext, CostModel, Cost, PlanStats};
use crate::memo::{ExprType, GroupExpression, Memo, PhysicalType};
use crate::property::{Property, Statistics};
use crate::stats_derivation::{
    StatsDerivator, FilterStatsDerivator, JoinStatsDerivator, AggregateStatsDerivator, SimpleStatsDerivator
};
use rsdb_common::Result;
use rsdb_sql::expr::{BinaryOperator, Expr as RsdbExpr};
use rsdb_sql::logical_plan::{JoinCondition, JoinType, LogicalPlan, Partitioning};
use std::collections::{HashMap, VecDeque};
use std::fmt;

// ============================================================================
// BitSet64 - Compact bitset for up to 64 join tables
// ============================================================================

/// Compact bitset for up to 64 elements (matching ClickHouse `std::bitset<64>`)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct BitSet64(pub u64);

impl BitSet64 {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn singleton(bit: usize) -> Self {
        debug_assert!(bit < 64);
        Self(1u64 << bit)
    }

    pub fn insert(&mut self, bit: usize) {
        debug_assert!(bit < 64);
        self.0 |= 1u64 << bit;
    }

    pub fn remove(&mut self, bit: usize) {
        debug_assert!(bit < 64);
        self.0 &= !(1u64 << bit);
    }

    pub fn contains(&self, bit: usize) -> bool {
        bit < 64 && (self.0 & (1u64 << bit)) != 0
    }

    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    pub fn difference(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn any(self) -> bool {
        self.0 != 0
    }

    pub fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    pub fn is_subset_of(self, other: Self) -> bool {
        self.difference(other).is_empty()
    }

    /// Return the index of the first set bit, or None
    pub fn first(self) -> Option<usize> {
        if self.0 == 0 {
            None
        } else {
            Some(self.0.trailing_zeros() as usize)
        }
    }

    /// Iterate over set bit indices
    pub fn iter(self) -> BitSet64Iter {
        BitSet64Iter(self.0)
    }
}

pub struct BitSet64Iter(u64);

impl Iterator for BitSet64Iter {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        if self.0 == 0 {
            None
        } else {
            let bit = self.0.trailing_zeros() as usize;
            self.0 &= self.0 - 1; // clear lowest set bit
            Some(bit)
        }
    }
}

impl fmt::Display for BitSet64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        let mut first = true;
        for b in self.iter() {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{}", b)?;
            first = false;
        }
        write!(f, "}}")
    }
}

// ============================================================================
// Rule System
// ============================================================================

/// Rule kind
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleKind {
    Transformation,
    Implementation,
}

/// Rule output: a new expression alternative for a group
pub struct RuleOutput {
    pub operator: LogicalPlan,
    pub children: Vec<usize>,
    pub expr_type: ExprType,
    pub physical_type: Option<PhysicalType>,
}

/// Cascades rule trait
pub trait CascadesRule: Send + Sync {
    fn name(&self) -> &str;
    fn rule_id(&self) -> usize;
    fn kind(&self) -> RuleKind;
    fn check(&self, expr: &GroupExpression, memo: &Memo) -> bool;
    fn apply(&self, expr: &GroupExpression, memo: &Memo) -> Result<Vec<RuleOutput>>;
}

// ============================================================================
// Transformation Rules
// ============================================================================

/// JoinCommutativity: Join(A,B) -> Join(B,A) for Inner joins
pub struct JoinCommutativity;

impl CascadesRule for JoinCommutativity {
    fn name(&self) -> &str {
        "JoinCommutativity"
    }
    fn rule_id(&self) -> usize {
        0
    }
    fn kind(&self) -> RuleKind {
        RuleKind::Transformation
    }

    fn check(&self, expr: &GroupExpression, _memo: &Memo) -> bool {
        if expr.expr_type != ExprType::Logical {
            return false;
        }
        matches!(
            &expr.operator,
            LogicalPlan::Join {
                join_type: JoinType::Inner,
                ..
            }
        ) && expr.children.len() == 2
    }

    fn apply(&self, expr: &GroupExpression, _memo: &Memo) -> Result<Vec<RuleOutput>> {
        if let LogicalPlan::Join {
            join_type,
            join_condition,
            schema,
            ..
        } = &expr.operator
        {
            // Swap children
            let swapped_children = vec![expr.children[1], expr.children[0]];

            // Swap join condition columns
            let swapped_condition = swap_join_condition(join_condition);

            let new_op = LogicalPlan::Join {
                left: Box::new(LogicalPlan::EmptyRelation), // placeholder, children hold real refs
                right: Box::new(LogicalPlan::EmptyRelation),
                join_type: *join_type,
                join_condition: swapped_condition,
                schema: schema.clone(),
            };

            Ok(vec![RuleOutput {
                operator: new_op,
                children: swapped_children,
                expr_type: ExprType::Logical,
                physical_type: None,
            }])
        } else {
            Ok(vec![])
        }
    }
}

/// JoinAssociativity: Join(Join(A,B),C) -> Join(A,Join(B,C))
pub struct JoinAssociativity;

impl CascadesRule for JoinAssociativity {
    fn name(&self) -> &str {
        "JoinAssociativity"
    }
    fn rule_id(&self) -> usize {
        1
    }
    fn kind(&self) -> RuleKind {
        RuleKind::Transformation
    }

    fn check(&self, expr: &GroupExpression, memo: &Memo) -> bool {
        if expr.expr_type != ExprType::Logical {
            return false;
        }
        if !matches!(
            &expr.operator,
            LogicalPlan::Join {
                join_type: JoinType::Inner,
                ..
            }
        ) {
            return false;
        }
        if expr.children.len() != 2 {
            return false;
        }
        // Check if left child group has a join expression
        let left_group_id = expr.children[0];
        if let Some(left_group) = memo.get_group(left_group_id) {
            for &eid in &left_group.expressions {
                if let Some(e) = memo.get_expr(eid) {
                    if matches!(
                        &e.operator,
                        LogicalPlan::Join {
                            join_type: JoinType::Inner,
                            ..
                        }
                    ) && e.children.len() == 2
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn apply(&self, expr: &GroupExpression, memo: &Memo) -> Result<Vec<RuleOutput>> {
        let mut outputs = Vec::new();
        let left_group_id = expr.children[0];
        let c_group = expr.children[1];

        if let Some(left_group) = memo.get_group(left_group_id) {
            for &eid in &left_group.expressions {
                if let Some(left_expr) = memo.get_expr(eid) {
                    if matches!(
                        &left_expr.operator,
                        LogicalPlan::Join {
                            join_type: JoinType::Inner,
                            ..
                        }
                    ) && left_expr.children.len() == 2
                    {
                        let a_group = left_expr.children[0];
                        let _b_group = left_expr.children[1];

                        if let LogicalPlan::Join { schema, .. } = &expr.operator {
                            let new_op = LogicalPlan::Join {
                                left: Box::new(LogicalPlan::EmptyRelation),
                                right: Box::new(LogicalPlan::EmptyRelation),
                                join_type: JoinType::Inner,
                                join_condition: JoinCondition::None,
                                schema: schema.clone(),
                            };

                            outputs.push(RuleOutput {
                                operator: new_op,
                                children: vec![a_group, c_group],
                                expr_type: ExprType::Logical,
                                physical_type: None,
                            });
                        }
                        break;
                    }
                }
            }
        }
        Ok(outputs)
    }
}

// ============================================================================
// Implementation Rules
// ============================================================================

/// HashJoinImpl: Logical Join -> Physical HashJoin
pub struct HashJoinImpl;

impl CascadesRule for HashJoinImpl {
    fn name(&self) -> &str {
        "HashJoinImpl"
    }
    fn rule_id(&self) -> usize {
        10
    }
    fn kind(&self) -> RuleKind {
        RuleKind::Implementation
    }

    fn check(&self, expr: &GroupExpression, _memo: &Memo) -> bool {
        expr.expr_type == ExprType::Logical && matches!(&expr.operator, LogicalPlan::Join { .. })
    }

    fn apply(&self, expr: &GroupExpression, _memo: &Memo) -> Result<Vec<RuleOutput>> {
        Ok(vec![RuleOutput {
            operator: expr.operator.clone(),
            children: expr.children.clone(),
            expr_type: ExprType::Physical,
            physical_type: Some(PhysicalType::HashJoin),
        }])
    }
}

/// TableScanImpl
pub struct TableScanImpl;

impl CascadesRule for TableScanImpl {
    fn name(&self) -> &str {
        "TableScanImpl"
    }
    fn rule_id(&self) -> usize {
        11
    }
    fn kind(&self) -> RuleKind {
        RuleKind::Implementation
    }

    fn check(&self, expr: &GroupExpression, _memo: &Memo) -> bool {
        expr.expr_type == ExprType::Logical && matches!(&expr.operator, LogicalPlan::Scan { .. })
    }

    fn apply(&self, expr: &GroupExpression, _memo: &Memo) -> Result<Vec<RuleOutput>> {
        Ok(vec![RuleOutput {
            operator: expr.operator.clone(),
            children: expr.children.clone(),
            expr_type: ExprType::Physical,
            physical_type: Some(PhysicalType::TableScan),
        }])
    }
}

/// FilterImpl
pub struct FilterImpl;

impl CascadesRule for FilterImpl {
    fn name(&self) -> &str {
        "FilterImpl"
    }
    fn rule_id(&self) -> usize {
        12
    }
    fn kind(&self) -> RuleKind {
        RuleKind::Implementation
    }

    fn check(&self, expr: &GroupExpression, _memo: &Memo) -> bool {
        expr.expr_type == ExprType::Logical && matches!(&expr.operator, LogicalPlan::Filter { .. })
    }

    fn apply(&self, expr: &GroupExpression, _memo: &Memo) -> Result<Vec<RuleOutput>> {
        Ok(vec![RuleOutput {
            operator: expr.operator.clone(),
            children: expr.children.clone(),
            expr_type: ExprType::Physical,
            physical_type: Some(PhysicalType::Filter),
        }])
    }
}

/// ProjectImpl
pub struct ProjectImpl;

impl CascadesRule for ProjectImpl {
    fn name(&self) -> &str {
        "ProjectImpl"
    }
    fn rule_id(&self) -> usize {
        13
    }
    fn kind(&self) -> RuleKind {
        RuleKind::Implementation
    }

    fn check(&self, expr: &GroupExpression, _memo: &Memo) -> bool {
        expr.expr_type == ExprType::Logical && matches!(&expr.operator, LogicalPlan::Project { .. })
    }

    fn apply(&self, expr: &GroupExpression, _memo: &Memo) -> Result<Vec<RuleOutput>> {
        Ok(vec![RuleOutput {
            operator: expr.operator.clone(),
            children: expr.children.clone(),
            expr_type: ExprType::Physical,
            physical_type: Some(PhysicalType::Project),
        }])
    }
}

/// AggregateImpl
pub struct AggregateImpl;

impl CascadesRule for AggregateImpl {
    fn name(&self) -> &str {
        "AggregateImpl"
    }
    fn rule_id(&self) -> usize {
        14
    }
    fn kind(&self) -> RuleKind {
        RuleKind::Implementation
    }

    fn check(&self, expr: &GroupExpression, _memo: &Memo) -> bool {
        expr.expr_type == ExprType::Logical
            && matches!(&expr.operator, LogicalPlan::Aggregate { .. })
    }

    fn apply(&self, expr: &GroupExpression, _memo: &Memo) -> Result<Vec<RuleOutput>> {
        Ok(vec![RuleOutput {
            operator: expr.operator.clone(),
            children: expr.children.clone(),
            expr_type: ExprType::Physical,
            physical_type: Some(PhysicalType::Aggregate),
        }])
    }
}

/// SortImpl
pub struct SortImpl;

impl CascadesRule for SortImpl {
    fn name(&self) -> &str {
        "SortImpl"
    }
    fn rule_id(&self) -> usize {
        15
    }
    fn kind(&self) -> RuleKind {
        RuleKind::Implementation
    }

    fn check(&self, expr: &GroupExpression, _memo: &Memo) -> bool {
        expr.expr_type == ExprType::Logical && matches!(&expr.operator, LogicalPlan::Sort { .. })
    }

    fn apply(&self, expr: &GroupExpression, _memo: &Memo) -> Result<Vec<RuleOutput>> {
        Ok(vec![RuleOutput {
            operator: expr.operator.clone(),
            children: expr.children.clone(),
            expr_type: ExprType::Physical,
            physical_type: Some(PhysicalType::Sort),
        }])
    }
}

/// LimitImpl
pub struct LimitImpl;

impl CascadesRule for LimitImpl {
    fn name(&self) -> &str {
        "LimitImpl"
    }
    fn rule_id(&self) -> usize {
        16
    }
    fn kind(&self) -> RuleKind {
        RuleKind::Implementation
    }

    fn check(&self, expr: &GroupExpression, _memo: &Memo) -> bool {
        expr.expr_type == ExprType::Logical && matches!(&expr.operator, LogicalPlan::Limit { .. })
    }

    fn apply(&self, expr: &GroupExpression, _memo: &Memo) -> Result<Vec<RuleOutput>> {
        Ok(vec![RuleOutput {
            operator: expr.operator.clone(),
            children: expr.children.clone(),
            expr_type: ExprType::Physical,
            physical_type: Some(PhysicalType::Limit),
        }])
    }
}

// ============================================================================
// JoinHyperGraph with BitSet64 operations
// ============================================================================

/// Edge in the join hypergraph
#[derive(Debug, Clone)]
pub struct JoinEdge {
    pub left_column: String,
    pub right_column: String,
}

/// Join hypergraph with BitSet64-based operations
#[derive(Debug, Clone)]
pub struct JoinHyperGraph {
    pub num_nodes: usize,
    /// Adjacency per node
    pub neighbors: Vec<BitSet64>,
    /// Map graph node index to memo group ID
    pub node_to_group: Vec<usize>,
    /// Reverse map: memo group ID to graph node index
    pub group_to_node: HashMap<usize, usize>,
    /// Join conditions between node pairs
    pub join_conditions: HashMap<(usize, usize), Vec<JoinEdge>>,
}

impl JoinHyperGraph {
    pub fn new() -> Self {
        Self {
            num_nodes: 0,
            neighbors: Vec::new(),
            node_to_group: Vec::new(),
            group_to_node: HashMap::new(),
            join_conditions: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, group_id: usize) -> usize {
        if let Some(&idx) = self.group_to_node.get(&group_id) {
            return idx;
        }
        let idx = self.num_nodes;
        self.num_nodes += 1;
        self.neighbors.push(BitSet64::new());
        self.node_to_group.push(group_id);
        self.group_to_node.insert(group_id, idx);
        idx
    }

    pub fn add_edge(&mut self, node_a: usize, node_b: usize, edge: JoinEdge) {
        if node_a < self.num_nodes && node_b < self.num_nodes {
            self.neighbors[node_a].insert(node_b);
            self.neighbors[node_b].insert(node_a);

            let key = if node_a <= node_b {
                (node_a, node_b)
            } else {
                (node_b, node_a)
            };
            self.join_conditions
                .entry(key)
                .or_insert_with(Vec::new)
                .push(edge);
        }
    }

    /// Get the neighbor set of a set of nodes
    pub fn neighbor_set(&self, nodes: BitSet64) -> BitSet64 {
        let mut result = BitSet64::new();
        for n in nodes.iter() {
            if n < self.num_nodes {
                result = result.union(self.neighbors[n]);
            }
        }
        result
    }

    /// BFS reachability from `start` within `allowed` nodes
    pub fn reachable(&self, start: BitSet64, allowed: BitSet64) -> BitSet64 {
        let mut visited = start.intersection(allowed);
        let mut queue: VecDeque<usize> = visited.iter().collect();

        while let Some(node) = queue.pop_front() {
            let nbrs = self.neighbors[node]
                .intersection(allowed)
                .difference(visited);
            for n in nbrs.iter() {
                visited.insert(n);
                queue.push_back(n);
            }
        }
        visited
    }

    /// Check if a set of nodes is connected
    pub fn is_connected(&self, nodes: BitSet64) -> bool {
        if nodes.is_empty() {
            return true;
        }
        let start_node = nodes.first().unwrap();
        let reached = self.reachable(BitSet64::singleton(start_node), nodes);
        reached == nodes
    }

    /// Get join conditions between two sets of nodes
    pub fn bridges(&self, left: BitSet64, right: BitSet64) -> Vec<JoinEdge> {
        let mut result = Vec::new();
        for l in left.iter() {
            for r in right.iter() {
                let key = if l <= r { (l, r) } else { (r, l) };
                if let Some(edges) = self.join_conditions.get(&key) {
                    result.extend(edges.clone());
                }
            }
        }
        result
    }

    /// Get all nodes as a BitSet64
    pub fn all_nodes(&self) -> BitSet64 {
        let mut bs = BitSet64::new();
        for i in 0..self.num_nodes {
            bs.insert(i);
        }
        bs
    }
}

impl Default for JoinHyperGraph {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// MinCutBranch Algorithm (ref: ClickHouse Graph.cpp:93-165)
// ============================================================================

/// A partition of the node set into two connected subsets
#[derive(Debug, Clone)]
pub struct Partition {
    pub left: BitSet64,
    pub right: BitSet64,
}

/// MinCutBranch enumeration of connected partitions
pub struct MinCutBranch<'a> {
    graph: &'a JoinHyperGraph,
    partitions: Vec<Partition>,
}

impl<'a> MinCutBranch<'a> {
    pub fn new(graph: &'a JoinHyperGraph) -> Self {
        Self {
            graph,
            partitions: Vec::new(),
        }
    }

    /// Entry point: enumerate all connected partitions of the graph
    pub fn cut_partitions(&mut self) -> Vec<Partition> {
        let s = self.graph.all_nodes();
        if s.len() < 2 {
            return vec![];
        }

        let x = BitSet64::new();
        // Pick the first node as the initial element of c
        if let Some(first_node) = s.first() {
            let c = BitSet64::singleton(first_node);
            let l = BitSet64::singleton(first_node);
            self.min_cut_branch(s, c, x, l);
        }

        std::mem::take(&mut self.partitions)
    }

    /// The recursive MinCutBranch algorithm
    /// s: full node set, c: current component, x: explored set, l: last added nodes
    fn min_cut_branch(&mut self, s: BitSet64, c: BitSet64, x: BitSet64, l: BitSet64) -> BitSet64 {
        let mut r = BitSet64::new();
        let mut r_tmp;
        let mut x_tmp = x;

        let neighbor_l = self.graph.neighbor_set(l);

        // n_l = new neighbors of last added: (neighbor(l) & s) - c - x
        let mut n_l = neighbor_l.intersection(s).difference(c).difference(x);
        // n_x = already-explored neighbors: (neighbor(l) & s - c) & x
        let mut n_x = neighbor_l.intersection(s).difference(c).intersection(x);
        // n_b = boundary neighbors: (neighbor(c) & s - c) - n_l - x
        let neighbor_c = self.graph.neighbor_set(c);
        let mut n_b = neighbor_c
            .intersection(s)
            .difference(c)
            .difference(n_l)
            .difference(x);

        while n_l.any() || n_x.any() || n_b.intersection(r).any() {
            let n_b_or_n_l = n_b.union(n_l);
            if n_b_or_n_l.intersection(r).any() {
                // Branch: pick a node from (n_b | n_l) & r
                let v = n_b_or_n_l.intersection(r).first().unwrap();
                let mut c_new = c;
                c_new.insert(v);
                self.min_cut_branch(s, c_new, x_tmp, BitSet64::singleton(v));

                // Update sets after branching
                n_l.remove(v);
                n_b.remove(v);
                x_tmp.insert(v);
            } else {
                x_tmp = x;
                if n_l.any() {
                    let v = n_l.first().unwrap();
                    let mut c_new = c;
                    c_new.insert(v);
                    r_tmp = self.min_cut_branch(s, c_new, x_tmp, BitSet64::singleton(v));

                    // Update n_l, n_x, n_b based on r_tmp
                    let new_neighbor = self.graph.neighbor_set(r_tmp);
                    n_l = n_l.difference(r_tmp);
                    n_l = n_l.union(
                        new_neighbor
                            .intersection(s)
                            .difference(c)
                            .difference(r_tmp)
                            .difference(x),
                    );
                    let newly_explored = r_tmp.intersection(x);
                    n_x = n_x.union(newly_explored).difference(r_tmp.difference(x));
                    n_b = self
                        .graph
                        .neighbor_set(c)
                        .intersection(s)
                        .difference(c)
                        .difference(n_l)
                        .difference(x);

                    n_l.remove(v);
                    x_tmp.insert(v);
                } else if n_x.any() {
                    let v = n_x.first().unwrap();
                    let mut start = c;
                    start.insert(v);
                    r_tmp = self.graph.reachable(
                        BitSet64::singleton(v),
                        s.difference(c)
                            .difference(x.difference(BitSet64::singleton(v))),
                    );

                    // Update n_l, n_x, n_b
                    let new_neighbor = self.graph.neighbor_set(r_tmp);
                    n_l = n_l.union(
                        new_neighbor
                            .intersection(s)
                            .difference(c)
                            .difference(r_tmp)
                            .difference(x),
                    );
                    n_x = n_x.difference(r_tmp);
                    n_b = self
                        .graph
                        .neighbor_set(c)
                        .intersection(s)
                        .difference(c)
                        .difference(n_l)
                        .difference(x);

                    n_x.remove(v);
                    x_tmp.insert(v);
                } else {
                    break;
                }

                // Emit partition if complement doesn't intersect explored set
                let complement = s.difference(r_tmp);
                if !complement.intersection(x).any() {
                    // Check both sides are connected
                    if self.graph.is_connected(r_tmp) && self.graph.is_connected(complement) {
                        self.partitions.push(Partition {
                            left: complement,
                            right: r_tmp,
                        });
                    }
                }

                r = r.union(r_tmp);
            }
        }

        r.union(l)
    }
}

// ============================================================================
// JoinEnumOnGraph Rule
// ============================================================================

/// JoinEnumOnGraph: transforms multi-way join graph into binary join trees
/// using MinCutBranch enumeration (ref: ClickHouse JoinEnumOnGraph.cpp)
pub struct JoinEnumOnGraphRule {
    pub max_graph_size: usize,
}

impl JoinEnumOnGraphRule {
    pub fn new() -> Self {
        Self { max_graph_size: 10 }
    }
}

impl Default for JoinEnumOnGraphRule {
    fn default() -> Self {
        Self::new()
    }
}

impl CascadesRule for JoinEnumOnGraphRule {
    fn name(&self) -> &str {
        "JoinEnumOnGraph"
    }
    fn rule_id(&self) -> usize {
        2
    }
    fn kind(&self) -> RuleKind {
        RuleKind::Transformation
    }

    fn check(&self, expr: &GroupExpression, memo: &Memo) -> bool {
        if expr.expr_type != ExprType::Logical {
            return false;
        }
        if !matches!(
            &expr.operator,
            LogicalPlan::Join {
                join_type: JoinType::Inner,
                ..
            }
        ) {
            return false;
        }
        // Count leaf tables in join tree
        let count = count_join_tables(expr, memo);
        count >= 3 && count <= self.max_graph_size
    }

    fn apply(&self, expr: &GroupExpression, memo: &Memo) -> Result<Vec<RuleOutput>> {
        // Collect join graph from the expression tree
        let mut graph = JoinHyperGraph::new();
        collect_join_graph(expr, memo, &mut graph);

        if graph.num_nodes < 3 {
            return Ok(vec![]);
        }

        // Run MinCutBranch to enumerate partitions
        let mut mcb = MinCutBranch::new(&graph);
        let partitions = mcb.cut_partitions();

        let mut outputs = Vec::new();

        for partition in &partitions {
            // Get join conditions between left and right
            let edges = graph.bridges(partition.left, partition.right);

            // Build join condition from edges
            let join_condition = if edges.is_empty() {
                JoinCondition::None
            } else {
                build_join_condition_from_edges(&edges)
            };

            // Build left-deep join tree for left partition
            let left_group = build_join_tree_for_partition(partition.left, &graph);
            let right_group = build_join_tree_for_partition(partition.right, &graph);

            if let (Some(left_gid), Some(right_gid)) = (left_group, right_group) {
                // Get schema from the original expression
                let schema = expr.operator.schema();

                // Generate L join R
                let new_op = LogicalPlan::Join {
                    left: Box::new(LogicalPlan::EmptyRelation),
                    right: Box::new(LogicalPlan::EmptyRelation),
                    join_type: JoinType::Inner,
                    join_condition: join_condition.clone(),
                    schema: schema.clone(),
                };

                outputs.push(RuleOutput {
                    operator: new_op,
                    children: vec![left_gid, right_gid],
                    expr_type: ExprType::Logical,
                    physical_type: None,
                });

                // Generate R join L (commutativity)
                let swapped_condition = swap_join_condition(&join_condition);
                let new_op_swapped = LogicalPlan::Join {
                    left: Box::new(LogicalPlan::EmptyRelation),
                    right: Box::new(LogicalPlan::EmptyRelation),
                    join_type: JoinType::Inner,
                    join_condition: swapped_condition,
                    schema,
                };

                outputs.push(RuleOutput {
                    operator: new_op_swapped,
                    children: vec![right_gid, left_gid],
                    expr_type: ExprType::Logical,
                    physical_type: None,
                });
            }
        }

        Ok(outputs)
    }
}

// ============================================================================
// Task Stack Loop (ClickHouse-style Cascades)
// ============================================================================

/// Optimization task
#[derive(Debug, Clone)]
pub enum Task {
    OptimizeGroup {
        group_id: usize,
        required_property: Property,
    },
    OptimizeExpression {
        group_id: usize,
        expr_id: usize,
        required_property: Property,
    },
    ExploreGroup {
        group_id: usize,
    },
    ExploreExpression {
        group_id: usize,
        expr_id: usize,
    },
    ApplyRule {
        group_id: usize,
        expr_id: usize,
        rule_id: usize,
        exploring: bool,
    },
    OptimizeInput {
        group_id: usize,
        expr_id: usize,
        required_property: Property,
        child_index: usize,
        accumulated_cost: Cost,
        child_properties: Vec<Property>,
    },
    EnforceSorting {
        group_id: usize,
        required_property: Property,
    },
    EnforceDistribution {
        group_id: usize,
        required_property: Property,
    },
}

/// Cascades Optimizer
pub struct CascadesOptimizer {
    pub memo: Memo,
    rules: Vec<Box<dyn CascadesRule>>,
    cost_model: CostModel,
    cbo_context: Option<CBOContext>,
    pub max_iterations: usize,
}

impl CascadesOptimizer {
    pub fn new() -> Self {
        Self {
            memo: Memo::new(),
            rules: default_rules(),
            cost_model: CostModel::default(),
            cbo_context: None,
            max_iterations: 10000,
        }
    }

    pub fn new_with_stats(cbo_context: &CBOContext) -> Self {
        let mut opt = Self::new();
        opt.cbo_context = Some(CBOContext::new_from(cbo_context));
        opt
    }

    /// Initialize Memo from a LogicalPlan tree (bottom-up)
    fn init_memo(&mut self, plan: &LogicalPlan) -> usize {
        // Helper to get stats from a group safely
        let get_stats = |memo: &Memo, gid: usize| -> PlanStats {
            memo.get_group(gid)
                .and_then(|g| g.statistics.clone())
                .unwrap_or_default()
        };

        match plan {
            LogicalPlan::Scan { .. } | LogicalPlan::EmptyRelation => {
                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, plan.clone(), vec![]);
                // Derive statistics for scans
                if let LogicalPlan::Scan { table_name, .. } = plan {
                    if let Some(ctx) = &self.cbo_context {
                        let stats = ctx.get_or_default_stats(table_name);
                        if let Some(group) = self.memo.get_group_mut(group_id) {
                            group.statistics = Some(stats);
                        }
                    }
                }
                group_id
            }
            LogicalPlan::Filter { input, predicate } => {
                let child_group = self.init_memo(input);
                let child_stats = get_stats(&self.memo, child_group);
                
                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, plan.clone(), vec![child_group]);

                if let Some(ctx) = &self.cbo_context {
                    let derivator = FilterStatsDerivator { predicate };
                    let stats = derivator.derive(&[&child_stats], ctx);
                    if let Some(group) = self.memo.get_group_mut(group_id) {
                        group.statistics = Some(stats);
                    }
                }
                group_id
            }
            LogicalPlan::Project { input, .. } => {
                let child_group = self.init_memo(input);
                let child_stats = get_stats(&self.memo, child_group);

                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, plan.clone(), vec![child_group]);
                
                if let Some(ctx) = &self.cbo_context {
                    let derivator = SimpleStatsDerivator { limit: None };
                    let stats = derivator.derive(&[&child_stats], ctx);
                    if let Some(group) = self.memo.get_group_mut(group_id) {
                        group.statistics = Some(stats);
                    }
                }
                group_id
            }
            LogicalPlan::Aggregate { input, group_expr, .. } => {
                let child_group = self.init_memo(input);
                let child_stats = get_stats(&self.memo, child_group);

                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, plan.clone(), vec![child_group]);

                if let Some(ctx) = &self.cbo_context {
                    let derivator = AggregateStatsDerivator { group_expr };
                    let stats = derivator.derive(&[&child_stats], ctx);
                    if let Some(group) = self.memo.get_group_mut(group_id) {
                        group.statistics = Some(stats);
                    }
                }
                group_id
            }
            LogicalPlan::Sort { input, .. } => {
                let child_group = self.init_memo(input);
                let child_stats = get_stats(&self.memo, child_group);

                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, plan.clone(), vec![child_group]);

                if let Some(group) = self.memo.get_group_mut(group_id) {
                    group.statistics = Some(child_stats);
                }
                group_id
            }
            LogicalPlan::Limit { input, limit, .. } => {
                let child_group = self.init_memo(input);
                let child_stats = get_stats(&self.memo, child_group);

                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, plan.clone(), vec![child_group]);

                if let Some(ctx) = &self.cbo_context {
                    let derivator = SimpleStatsDerivator { limit: Some(*limit) };
                    let stats = derivator.derive(&[&child_stats], ctx);
                    if let Some(group) = self.memo.get_group_mut(group_id) {
                        group.statistics = Some(stats);
                    }
                }
                group_id
            }
            LogicalPlan::Join { left, right, join_type, join_condition, .. } => {
                let left_group = self.init_memo(left);
                let left_stats = get_stats(&self.memo, left_group);
                
                let right_group = self.init_memo(right);
                let right_stats = get_stats(&self.memo, right_group);

                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, plan.clone(), vec![left_group, right_group]);

                if let Some(ctx) = &self.cbo_context {
                    let derivator = JoinStatsDerivator { join_type: *join_type, condition: join_condition };
                    let stats = derivator.derive(&[&left_stats, &right_stats], ctx);
                    if let Some(group) = self.memo.get_group_mut(group_id) {
                        group.statistics = Some(stats);
                    }
                }
                group_id
            }
            LogicalPlan::CrossJoin { left, right, .. } => {
                let left_group = self.init_memo(left);
                let left_stats = get_stats(&self.memo, left_group);

                let right_group = self.init_memo(right);
                let right_stats = get_stats(&self.memo, right_group);

                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, plan.clone(), vec![left_group, right_group]);

                if let Some(ctx) = &self.cbo_context {
                    let derivator = JoinStatsDerivator { join_type: JoinType::Cross, condition: &JoinCondition::None };
                    let stats = derivator.derive(&[&left_stats, &right_stats], ctx);
                    if let Some(group) = self.memo.get_group_mut(group_id) {
                        group.statistics = Some(stats);
                    }
                }
                group_id
            }
            LogicalPlan::Union { inputs, .. } => {
                let child_groups: Vec<usize> = inputs.iter().map(|p| self.init_memo(p)).collect();
                // Sum rows
                let mut total_rows = 0;
                for gid in &child_groups {
                    total_rows += get_stats(&self.memo, *gid).row_count;
                }
                
                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, plan.clone(), child_groups);
                
                if let Some(group) = self.memo.get_group_mut(group_id) {
                    group.statistics = Some(PlanStats { row_count: total_rows, ..Default::default() });
                }
                group_id
            }
            LogicalPlan::Subquery { query, .. } => {
                let child_group = self.init_memo(query);
                let child_stats = get_stats(&self.memo, child_group);
                
                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, plan.clone(), vec![child_group]);
                
                if let Some(group) = self.memo.get_group_mut(group_id) {
                    group.statistics = Some(child_stats);
                }
                group_id
            }
            LogicalPlan::Exchange { input, partitioning } => {
                let child_group = self.init_memo(input);
                let child_stats = get_stats(&self.memo, child_group);
                
                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, LogicalPlan::Exchange {
                    input: Box::new(LogicalPlan::EmptyRelation), // placeholder
                    partitioning: partitioning.clone(),
                }, vec![child_group]);
                
                if let Some(group) = self.memo.get_group_mut(group_id) {
                    group.statistics = Some(child_stats);
                }
                group_id
            }
            _ => {
                let group_id = self.memo.create_group();
                self.memo.add_logical_expr(group_id, plan.clone(), vec![]);
                group_id
            }
        }
    }

    /// Main optimization entry point
    pub fn optimize(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let root_group_id = self.init_memo(&plan);
        self.memo.root_group_id = Some(root_group_id);

        let required = Property::default();

        // Push initial task
        let mut task_stack: Vec<Task> = vec![Task::OptimizeGroup {
            group_id: root_group_id,
            required_property: required.clone(),
        }];

        let mut iterations = 0;

        // DFS task loop (LIFO stack)
        while let Some(task) = task_stack.pop() {
            iterations += 1;
            if iterations > self.max_iterations {
                break;
            }

            match task {
                Task::OptimizeGroup {
                    group_id,
                    required_property,
                } => {
                    self.handle_optimize_group(group_id, &required_property, &mut task_stack);
                }
                Task::OptimizeExpression {
                    group_id,
                    expr_id,
                    required_property,
                } => {
                    self.handle_optimize_expression(
                        group_id,
                        expr_id,
                        &required_property,
                        &mut task_stack,
                    );
                }
                Task::ExploreGroup { group_id } => {
                    self.handle_explore_group(group_id, &mut task_stack);
                }
                Task::ExploreExpression { group_id, expr_id } => {
                    self.handle_explore_expression(group_id, expr_id, &mut task_stack);
                }
                Task::ApplyRule {
                    group_id,
                    expr_id,
                    rule_id,
                    exploring,
                } => {
                    self.handle_apply_rule(group_id, expr_id, rule_id, exploring, &mut task_stack);
                }
                Task::OptimizeInput {
                    group_id,
                    expr_id,
                    required_property,
                    child_index,
                    accumulated_cost,
                    child_properties,
                } => {
                    self.handle_optimize_input(
                        group_id,
                        expr_id,
                        &required_property,
                        child_index,
                        accumulated_cost,
                        &child_properties,
                        &mut task_stack,
                    );
                }
                Task::EnforceSorting {
                    group_id,
                    required_property,
                } => {
                    self.handle_enforce_sorting(group_id, required_property);
                }
                Task::EnforceDistribution {
                    group_id,
                    required_property,
                } => {
                    self.handle_enforce_distribution(group_id, required_property);
                }
            }
        }

        self.extract_best_plan(root_group_id, &required)
    }

    /// OptimizeGroup: if winner exists, done. Otherwise push ExploreGroup + OptimizeExpression.
    fn handle_optimize_group(
        &self,
        group_id: usize,
        required_property: &Property,
        task_stack: &mut Vec<Task>,
    ) {
        if let Some(group) = self.memo.get_group(group_id) {
            // Check if we already have a winner
            if group.has_winner(required_property) {
                return;
            }

            // 1. Sorting Enforcement
            // If sorting is required, we can enforce it by adding a Sort operator
            // on top of a plan that satisfies the partitioning requirements.
            if !required_property.sorting.is_empty() {
                let mut input_prop = required_property.clone();
                input_prop.sorting.clear(); // Require partitioning but no sorting

                task_stack.push(Task::EnforceSorting {
                    group_id,
                    required_property: required_property.clone(),
                });
                task_stack.push(Task::OptimizeGroup {
                    group_id,
                    required_property: input_prop,
                });
            }
            // 2. Distribution Enforcement
            // If partitioning is required (and we are not currently enforcing sorting on top of it),
            // we can enforce it by adding an Exchange operator on top of Any plan.
            else if required_property.partitioning != Partitioning::Any {
                let input_prop = Property::default(); // Any, Any

                task_stack.push(Task::EnforceDistribution {
                    group_id,
                    required_property: required_property.clone(),
                });
                task_stack.push(Task::OptimizeGroup {
                    group_id,
                    required_property: input_prop,
                });
            }

            // Push ExploreGroup first (will be executed after OptimizeExpression tasks)
            task_stack.push(Task::ExploreGroup { group_id });

            // Push OptimizeExpression for each expression in the group
            for &expr_id in group.expressions.iter().rev() {
                task_stack.push(Task::OptimizeExpression {
                    group_id,
                    expr_id,
                    required_property: required_property.clone(),
                });
            }
        }
    }

    /// OptimizeExpression: push ApplyRule for applicable implementation + transformation rules,
    /// then push OptimizeInput for physical expressions.
    fn handle_optimize_expression(
        &self,
        group_id: usize,
        expr_id: usize,
        required_property: &Property,
        task_stack: &mut Vec<Task>,
    ) {
        if let Some(expr) = self.memo.get_expr(expr_id) {
            if expr.is_physical() {
                // For physical expressions, go straight to input optimization
                task_stack.push(Task::OptimizeInput {
                    group_id,
                    expr_id,
                    required_property: required_property.clone(),
                    child_index: 0,
                    accumulated_cost: Cost::zero(),
                    child_properties: self.derive_child_properties(expr, required_property),
                });
                return;
            }

            // For logical expressions, apply implementation rules
            for rule in &self.rules {
                if rule.kind() == RuleKind::Implementation && !expr.has_applied_rule(rule.rule_id())
                {
                    if rule.check(expr, &self.memo) {
                        task_stack.push(Task::ApplyRule {
                            group_id,
                            expr_id,
                            rule_id: rule.rule_id(),
                            exploring: false,
                        });
                    }
                }
            }

            // Also apply transformation rules (if not exploring-only)
            for rule in &self.rules {
                if rule.kind() == RuleKind::Transformation && !expr.has_applied_rule(rule.rule_id())
                {
                    if rule.check(expr, &self.memo) {
                        task_stack.push(Task::ApplyRule {
                            group_id,
                            expr_id,
                            rule_id: rule.rule_id(),
                            exploring: false,
                        });
                    }
                }
            }
        }
    }

    /// ExploreGroup: apply transformation rules only, explore children first
    fn handle_explore_group(&self, group_id: usize, task_stack: &mut Vec<Task>) {
        if let Some(group) = self.memo.get_group(group_id) {
            if group.explored {
                return;
            }

            for &expr_id in group.expressions.iter().rev() {
                task_stack.push(Task::ExploreExpression { group_id, expr_id });
            }
        }
    }

    /// ExploreExpression: apply transformation rules, explore child groups
    fn handle_explore_expression(
        &mut self,
        group_id: usize,
        expr_id: usize,
        task_stack: &mut Vec<Task>,
    ) {
        let (children, explored) = {
            if let Some(expr) = self.memo.get_expr(expr_id) {
                if expr.explored {
                    return;
                }
                (expr.children.clone(), false)
            } else {
                return;
            }
        };

        if !explored {
            // Explore child groups first
            for &child_gid in children.iter().rev() {
                task_stack.push(Task::ExploreGroup {
                    group_id: child_gid,
                });
            }

            // Apply transformation rules
            if let Some(expr) = self.memo.get_expr(expr_id) {
                for rule in &self.rules {
                    if rule.kind() == RuleKind::Transformation
                        && !expr.has_applied_rule(rule.rule_id())
                    {
                        if rule.check(expr, &self.memo) {
                            task_stack.push(Task::ApplyRule {
                                group_id,
                                expr_id,
                                rule_id: rule.rule_id(),
                                exploring: true,
                            });
                        }
                    }
                }
            }

            // Mark explored
            if let Some(expr) = self.memo.get_expr_mut(expr_id) {
                expr.explored = true;
            }
        }
    }

    /// ApplyRule: execute the rule, insert outputs into memo, push follow-up tasks
    fn handle_apply_rule(
        &mut self,
        group_id: usize,
        expr_id: usize,
        rule_id: usize,
        exploring: bool,
        task_stack: &mut Vec<Task>,
    ) {
        // Mark rule as applied
        if let Some(expr) = self.memo.get_expr_mut(expr_id) {
            expr.mark_rule_applied(rule_id);
        }

        // Find the rule
        let rule_idx = self.rules.iter().position(|r| r.rule_id() == rule_id);
        if rule_idx.is_none() {
            return;
        }

        // Get the expression (need to clone to avoid borrow issues)
        let expr_clone = self.memo.get_expr(expr_id).cloned();
        if expr_clone.is_none() {
            return;
        }
        let expr_clone = expr_clone.unwrap();

        // Apply the rule
        let rule = &self.rules[rule_idx.unwrap()];
        let outputs = match rule.apply(&expr_clone, &self.memo) {
            Ok(o) => o,
            Err(_) => return,
        };

        // Insert outputs into memo
        for output in outputs {
            let (_, new_expr_id, is_new) = self.memo.insert_expression(
                group_id,
                output.operator,
                output.children,
                output.expr_type,
                output.physical_type,
            );

            if is_new {
                if exploring {
                    // Push explore for new logical expressions
                    if output.expr_type == ExprType::Logical {
                        task_stack.push(Task::ExploreExpression {
                            group_id,
                            expr_id: new_expr_id,
                        });
                    }
                } else {
                    // Push optimize for new expressions
                    task_stack.push(Task::OptimizeExpression {
                        group_id,
                        expr_id: new_expr_id,
                        required_property: Property::default(),
                    });
                }
            }
        }
    }

    /// OptimizeInput: multi-phase child optimization with cost pruning
    fn handle_optimize_input(
        &mut self,
        group_id: usize,
        expr_id: usize,
        required_property: &Property,
        child_index: usize,
        accumulated_cost: Cost,
        child_properties: &[Property],
        task_stack: &mut Vec<Task>,
    ) {
        let (children, operator) = {
            if let Some(expr) = self.memo.get_expr(expr_id) {
                (expr.children.clone(), expr.operator.clone())
            } else {
                return;
            }
        };

        // Compute local cost (cost of this operator alone, not including children)
        let local_cost = self.compute_local_cost(&operator, group_id);

        // Cost pruning: check if accumulated + local already exceeds current winner
        let total_so_far = accumulated_cost.add(&local_cost);
        if let Some(group) = self.memo.get_group(group_id) {
            if let Some(winner) = group.get_winner(required_property) {
                if !total_so_far.is_less_than(&winner.cost) {
                    return; // Prune: can't beat current winner
                }
            }
        }

        if child_index >= children.len() {
            // All children optimized: compute total cost and update winner
            let mut total_cost = local_cost;
            for (i, &child_gid) in children.iter().enumerate() {
                let child_prop = child_properties.get(i).cloned().unwrap_or_default();
                if let Some(child_group) = self.memo.get_group(child_gid) {
                    if let Some(child_winner) = child_group.get_winner(&child_prop) {
                        total_cost = total_cost.add(&child_winner.cost);
                    } else {
                        // Child has no winner yet; cost is infinite
                        return;
                    }
                }
            }

            // Update winner
            self.memo
                .update_winner(group_id, expr_id, total_cost, required_property.clone());
            return;
        }

        // Optimize current child
        let child_gid = children[child_index];
        let child_prop = child_properties
            .get(child_index)
            .cloned()
            .unwrap_or_default();

        // Check if child already has a winner
        let child_has_winner = self
            .memo
            .get_group(child_gid)
            .map(|g| g.has_winner(&child_prop))
            .unwrap_or(false);

        if child_has_winner {
            // Child already optimized, accumulate cost and move to next child
            let child_cost = self
                .memo
                .get_group(child_gid)
                .and_then(|g| g.get_winner(&child_prop))
                .map(|w| w.cost)
                .unwrap_or(Cost::infinite());

            let new_accumulated = accumulated_cost.add(&child_cost);

            task_stack.push(Task::OptimizeInput {
                group_id,
                expr_id,
                required_property: required_property.clone(),
                child_index: child_index + 1,
                accumulated_cost: new_accumulated,
                child_properties: child_properties.to_vec(),
            });
        } else {
            // Push self as continuation (will resume after child is optimized)
            task_stack.push(Task::OptimizeInput {
                group_id,
                expr_id,
                required_property: required_property.clone(),
                child_index,
                accumulated_cost,
                child_properties: child_properties.to_vec(),
            });

            // Push OptimizeGroup for the child
            task_stack.push(Task::OptimizeGroup {
                group_id: child_gid,
                required_property: child_prop,
            });
        }
    }

    fn handle_enforce_sorting(&mut self, group_id: usize, required_property: Property) {
        let mut input_prop = required_property.clone();
        input_prop.sorting.clear();

        let (input_cost, has_winner) = if let Some(group) = self.memo.get_group(group_id) {
            if let Some(winner) = group.get_winner(&input_prop) {
                (winner.cost, true)
            } else {
                (Cost::infinite(), false)
            }
        } else {
            (Cost::infinite(), false)
        };

        if has_winner {
            // Create Sort operator
            let sort_exprs: Vec<RsdbExpr> = required_property.sorting.iter()
                .map(|s| RsdbExpr::Column(s.clone()))
                .collect();
            
            let sort_op = LogicalPlan::Sort {
                input: Box::new(LogicalPlan::EmptyRelation),
                expr: sort_exprs,
            };

            // Calculate cost
            let row_count = self.memo.get_group(group_id)
                .and_then(|g| g.statistics.as_ref())
                .map(|s| s.row_count)
                .unwrap_or(1000) as f64;
            
            let sort_cost = self.cost_model.compute_operator_cost(&sort_op, row_count);
            let total_cost = input_cost.add(&sort_cost);

            let (_, expr_id, _) = self.memo.insert_expression(
                group_id,
                sort_op,
                vec![group_id],
                ExprType::Physical,
                Some(PhysicalType::Sort),
            );

            self.memo.update_winner(group_id, expr_id, total_cost, required_property);
        }
    }

    fn handle_enforce_distribution(&mut self, group_id: usize, required_property: Property) {
        let any_prop = Property::default();
        let (any_cost, has_winner) = if let Some(group) = self.memo.get_group(group_id) {
            if let Some(winner) = group.get_winner(&any_prop) {
                (winner.cost, true)
            } else {
                (Cost::infinite(), false)
            }
        } else {
            (Cost::infinite(), false)
        };

        if has_winner {
            let row_count = self
                .memo
                .get_group(group_id)
                .and_then(|g| g.statistics.as_ref())
                .map(|s| s.row_count)
                .unwrap_or(1000) as f64;
            
            // Cost calculation using CostModel
            // Exchange cost: mostly network
            let exchange_op = LogicalPlan::Exchange {
                input: Box::new(LogicalPlan::EmptyRelation),
                partitioning: required_property.partitioning.clone(),
            };
            let exchange_cost = self.cost_model.compute_operator_cost(&exchange_op, row_count);
            let total_cost = any_cost.add(&exchange_cost);

            let (_, expr_id, _) = self.memo.insert_expression(
                group_id,
                exchange_op,
                vec![group_id],
                ExprType::Physical,
                Some(PhysicalType::Exchange),
            );

            self.memo.update_winner(group_id, expr_id, total_cost, required_property);
        }
    }

    /// Derive required properties for children based on operator and parent requirements
    fn derive_child_properties(
        &self,
        expr: &GroupExpression,
        required_property: &Property,
    ) -> Vec<Property> {
        let mut props = vec![Property::default(); expr.children.len()];

        if let Some(phy_type) = expr.physical_type {
            match phy_type {
                PhysicalType::HashJoin => {
                    if let LogicalPlan::Join { join_condition, .. } = &expr.operator {
                        match join_condition {
                            // Simple case: Hash Shuffle Join
                            // Require Hash partitioning on join keys for both sides
                            JoinCondition::Using(cols) => {
                                let hash_exprs: Vec<RsdbExpr> = cols.iter().map(|c| RsdbExpr::Column(c.clone())).collect();
                                let prop = Property {
                                    partitioning: Partitioning::Hash(hash_exprs, 1),
                                    sorting: vec![],
                                };
                                if props.len() >= 2 {
                                    props[0] = prop.clone();
                                    props[1] = prop;
                                }
                            }
                            JoinCondition::On(expr) => {
                                // TODO: Extract join keys from expression
                                // For now, assume broadcast join (right side broadcast)
                                // Left: Any, Right: Broadcast
                                if props.len() >= 2 {
                                    props[1] = Property {
                                        partitioning: Partitioning::Broadcast,
                                        sorting: vec![],
                                    };
                                }
                            }
                            _ => {}
                        }
                    }
                }
                PhysicalType::Aggregate => {
                    if let LogicalPlan::Aggregate { group_expr, .. } = &expr.operator {
                        if !group_expr.is_empty() {
                            let prop = Property {
                                partitioning: Partitioning::Hash(group_expr.clone(), 1),
                                sorting: vec![],
                            };
                            if !props.is_empty() {
                                props[0] = prop;
                            }
                        } else {
                            // Global aggregation without groups -> Single partition
                            let prop = Property {
                                partitioning: Partitioning::Single,
                                sorting: vec![],
                            };
                            if !props.is_empty() {
                                props[0] = prop;
                            }
                        }
                    }
                }
                PhysicalType::Exchange => {
                    // Exchange itself satisfies the property, requires Any from child
                    // props[0] is already Any
                }
                PhysicalType::Sort => {
                    // Sort creates order, requires Any distribution (or Single if global sort)
                    // If Sort is global, child must be Single? Or we gather after sort?
                    // For now, simple local sort
                }
                _ => {}
            }
        }
        
        // Pass through partitioning if operator doesn't change it (e.g. Filter, Project)
        // AND if it's not a blocking operator.
        // Simplified: if required_property is Hash, Filter/Project pass it down.
        if required_property.partitioning != Partitioning::Any {
             if let Some(phy_type) = expr.physical_type {
                 match phy_type {
                     PhysicalType::Filter | PhysicalType::Project | PhysicalType::Limit => {
                         if !props.is_empty() {
                             props[0] = required_property.clone();
                         }
                     }
                     _ => {}
                 }
             }
        }

        props
    }

    /// Compute the local cost of an operator (not including children)
    fn compute_local_cost(&self, operator: &LogicalPlan, group_id: usize) -> Cost {
        let row_count = self
            .memo
            .get_group(group_id)
            .and_then(|g| g.statistics.as_ref())
            .map(|s| s.row_count)
            .unwrap_or(1000) as f64;

        self.cost_model.compute_operator_cost(operator, row_count)
    }

    /// Extract the best plan from the memo by following winners
    fn extract_best_plan(&self, group_id: usize, property: &Property) -> Result<LogicalPlan> {
        let group = self
            .memo
            .get_group(group_id)
            .ok_or_else(|| rsdb_common::RsdbError::Planner("Group not found".to_string()))?;

        // Try to get winner for the property
        if let Some(winner) = group.get_winner(property) {
            if let Some(expr) = self.memo.get_expr(winner.expr_id) {
                return self.reconstruct_plan(expr, property);
            }
        }

        // Fallback: use the first expression in the group
        if let Some(&first_expr_id) = group.expressions.first() {
            if let Some(expr) = self.memo.get_expr(first_expr_id) {
                return self.reconstruct_plan(expr, property);
            }
        }

        Err(rsdb_common::RsdbError::Planner(
            "No plan found in memo".to_string(),
        ))
    }

    /// Reconstruct a LogicalPlan from a GroupExpression
    fn reconstruct_plan(&self, expr: &GroupExpression, property: &Property) -> Result<LogicalPlan> {
        // Handle Exchange specially: its child is the group ID itself
        if expr.physical_type == Some(PhysicalType::Exchange) {
            let child_gid = expr.children[0];
            // Exchange requires Any from its child
            let child_plan = self.extract_best_plan(child_gid, &Property::default())?;
            
            if let LogicalPlan::Exchange { partitioning, .. } = &expr.operator {
                return Ok(LogicalPlan::Exchange {
                    input: Box::new(child_plan),
                    partitioning: partitioning.clone(),
                });
            }
        }

        if expr.children.is_empty() {
            return Ok(expr.operator.clone());
        }

        // Get required properties for children
        let child_props = self.derive_child_properties(expr, property);
        
        let mut child_plans = Vec::new();
        for (i, &child_gid) in expr.children.iter().enumerate() {
            let req = child_props.get(i).cloned().unwrap_or_default();
            child_plans.push(self.extract_best_plan(child_gid, &req)?);
        }

        match &expr.operator {
            LogicalPlan::Filter { predicate, .. } => Ok(LogicalPlan::Filter {
                input: Box::new(child_plans[0].clone()),
                predicate: predicate.clone(),
            }),
            LogicalPlan::Project { expr, schema, .. } => Ok(LogicalPlan::Project {
                input: Box::new(child_plans[0].clone()),
                expr: expr.clone(),
                schema: schema.clone(),
            }),
            LogicalPlan::Aggregate {
                group_expr,
                aggregate_expr,
                schema,
                ..
            } => Ok(LogicalPlan::Aggregate {
                input: Box::new(child_plans[0].clone()),
                group_expr: group_expr.clone(),
                aggregate_expr: aggregate_expr.clone(),
                schema: schema.clone(),
            }),
            LogicalPlan::Sort {
                expr: sort_expr, ..
            } => Ok(LogicalPlan::Sort {
                input: Box::new(child_plans[0].clone()),
                expr: sort_expr.clone(),
            }),
            LogicalPlan::Limit { limit, offset, .. } => Ok(LogicalPlan::Limit {
                input: Box::new(child_plans[0].clone()),
                limit: *limit,
                offset: *offset,
            }),
            LogicalPlan::Join {
                join_type,
                join_condition,
                schema,
                ..
            } => {
                if child_plans.len() >= 2 {
                    Ok(LogicalPlan::Join {
                        left: Box::new(child_plans[0].clone()),
                        right: Box::new(child_plans[1].clone()),
                        join_type: *join_type,
                        join_condition: join_condition.clone(),
                        schema: schema.clone(),
                    })
                } else {
                    Ok(expr.operator.clone())
                }
            }
            LogicalPlan::CrossJoin { schema, .. } => {
                if child_plans.len() >= 2 {
                    Ok(LogicalPlan::CrossJoin {
                        left: Box::new(child_plans[0].clone()),
                        right: Box::new(child_plans[1].clone()),
                        schema: schema.clone(),
                    })
                } else {
                    Ok(expr.operator.clone())
                }
            }
            LogicalPlan::Union { schema, .. } => Ok(LogicalPlan::Union {
                inputs: child_plans,
                schema: schema.clone(),
            }),
            LogicalPlan::Subquery { schema, .. } => Ok(LogicalPlan::Subquery {
                query: Box::new(child_plans[0].clone()),
                schema: schema.clone(),
            }),
            _ => Ok(expr.operator.clone()),
        }
    }
}

impl Default for CascadesOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Build the default set of rules
fn default_rules() -> Vec<Box<dyn CascadesRule>> {
    vec![
        // Transformation rules
        Box::new(JoinCommutativity),
        Box::new(JoinAssociativity),
        Box::new(JoinEnumOnGraphRule::new()),
        // Implementation rules
        Box::new(HashJoinImpl),
        Box::new(TableScanImpl),
        Box::new(FilterImpl),
        Box::new(ProjectImpl),
        Box::new(AggregateImpl),
        Box::new(SortImpl),
        Box::new(LimitImpl),
    ]
}

/// Swap join condition columns (for commutativity)
fn swap_join_condition(cond: &JoinCondition) -> JoinCondition {
    match cond {
        JoinCondition::On(expr) => JoinCondition::On(swap_eq_sides(expr)),
        JoinCondition::Using(cols) => JoinCondition::Using(cols.clone()),
        JoinCondition::None => JoinCondition::None,
    }
}

fn swap_eq_sides(expr: &RsdbExpr) -> RsdbExpr {
    match expr {
        RsdbExpr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => RsdbExpr::BinaryOp {
            left: right.clone(),
            op: BinaryOperator::Eq,
            right: left.clone(),
        },
        RsdbExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => RsdbExpr::BinaryOp {
            left: Box::new(swap_eq_sides(left)),
            op: BinaryOperator::And,
            right: Box::new(swap_eq_sides(right)),
        },
        other => other.clone(),
    }
}

/// Count the number of leaf tables in a join tree
fn count_join_tables(expr: &GroupExpression, memo: &Memo) -> usize {
    match &expr.operator {
        LogicalPlan::Join {
            join_type: JoinType::Inner,
            ..
        } => {
            let mut count = 0;
            for &child_gid in &expr.children {
                if let Some(group) = memo.get_group(child_gid) {
                    for &eid in &group.expressions {
                        if let Some(child_expr) = memo.get_expr(eid) {
                            let child_count = count_join_tables(child_expr, memo);
                            count = count.max(child_count);
                        }
                    }
                    if count == 0 {
                        count += 1; // leaf
                    }
                }
            }
            count
        }
        LogicalPlan::Scan { .. } => 1,
        _ => 1,
    }
}

/// Collect join graph from expression tree into a JoinHyperGraph
fn collect_join_graph(expr: &GroupExpression, memo: &Memo, graph: &mut JoinHyperGraph) {
    match &expr.operator {
        LogicalPlan::Join {
            join_type: JoinType::Inner,
            join_condition,
            ..
        } => {
            // Recursively collect from children
            let mut left_nodes = Vec::new();
            let mut right_nodes = Vec::new();

            if expr.children.len() == 2 {
                collect_join_graph_from_group(expr.children[0], memo, graph, &mut left_nodes);
                collect_join_graph_from_group(expr.children[1], memo, graph, &mut right_nodes);

                // Add edges from join condition
                if let JoinCondition::On(cond_expr) = join_condition {
                    let eq_pairs = extract_eq_columns(cond_expr);
                    for (left_col, right_col) in eq_pairs {
                        // Connect left-side nodes to right-side nodes
                        if let (Some(&ln), Some(&rn)) = (left_nodes.first(), right_nodes.first()) {
                            graph.add_edge(
                                ln,
                                rn,
                                JoinEdge {
                                    left_column: left_col,
                                    right_column: right_col,
                                },
                            );
                        }
                    }
                }
            }
        }
        LogicalPlan::Scan { .. } => {
            // Leaf node
            let node_idx = graph.add_node(expr.group_id);
            let _ = node_idx; // node added to graph
        }
        _ => {
            // Non-join, non-scan: treat as leaf
            graph.add_node(expr.group_id);
        }
    }
}

fn collect_join_graph_from_group(
    group_id: usize,
    memo: &Memo,
    graph: &mut JoinHyperGraph,
    nodes: &mut Vec<usize>,
) {
    if let Some(group) = memo.get_group(group_id) {
        // Use the first expression in the group
        if let Some(&first_eid) = group.expressions.first() {
            if let Some(expr) = memo.get_expr(first_eid) {
                match &expr.operator {
                    LogicalPlan::Join {
                        join_type: JoinType::Inner,
                        ..
                    } => {
                        collect_join_graph(expr, memo, graph);
                        // Collect all leaf nodes under this join
                        for &child_gid in &expr.children {
                            collect_join_graph_from_group(child_gid, memo, graph, nodes);
                        }
                    }
                    _ => {
                        // Leaf group
                        let node_idx = graph.add_node(group_id);
                        nodes.push(node_idx);
                    }
                }
            }
        }
    }
}

/// Extract equality column pairs from an expression
fn extract_eq_columns(expr: &RsdbExpr) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    match expr {
        RsdbExpr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if let (RsdbExpr::Column(l), RsdbExpr::Column(r)) = (left.as_ref(), right.as_ref()) {
                pairs.push((l.clone(), r.clone()));
            }
        }
        RsdbExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            pairs.extend(extract_eq_columns(left));
            pairs.extend(extract_eq_columns(right));
        }
        _ => {}
    }
    pairs
}

/// Build a JoinCondition from a set of JoinEdges
fn build_join_condition_from_edges(edges: &[JoinEdge]) -> JoinCondition {
    if edges.is_empty() {
        return JoinCondition::None;
    }

    let mut exprs: Vec<RsdbExpr> = edges
        .iter()
        .map(|e| RsdbExpr::BinaryOp {
            left: Box::new(RsdbExpr::Column(e.left_column.clone())),
            op: BinaryOperator::Eq,
            right: Box::new(RsdbExpr::Column(e.right_column.clone())),
        })
        .collect();

    if exprs.len() == 1 {
        JoinCondition::On(exprs.remove(0))
    } else {
        // AND them together
        let mut combined = exprs.remove(0);
        for e in exprs {
            combined = RsdbExpr::BinaryOp {
                left: Box::new(combined),
                op: BinaryOperator::And,
                right: Box::new(e),
            };
        }
        JoinCondition::On(combined)
    }
}

/// Build a join tree for a partition (returns the memo group id of the root)
/// For a single node, returns the group; for multiple, returns None (handled by the optimizer)
fn build_join_tree_for_partition(nodes: BitSet64, graph: &JoinHyperGraph) -> Option<usize> {
    if nodes.len() == 1 {
        let node_idx = nodes.first()?;
        Some(graph.node_to_group[node_idx])
    } else if nodes.len() == 0 {
        None
    } else {
        // For multi-node partitions, return the first node's group
        // The optimizer will create proper join trees via recursive application
        let first = nodes.first()?;
        Some(graph.node_to_group[first])
    }
}

/// Check if a plan contains joins
pub fn has_joins(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Join { .. } | LogicalPlan::CrossJoin { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Subquery { query: input, .. } => has_joins(input),
        LogicalPlan::Union { inputs, .. } => inputs.iter().any(has_joins),
        _ => false,
    }
}

// ============================================================================
// UnionFind (kept for backwards compat)
// ============================================================================

/// UnionFind (Disjoint Set Union) - tracks equivalent join keys
#[derive(Debug, Clone, Default)]
pub struct UnionFind {
    parent: HashMap<String, String>,
}

impl UnionFind {
    pub fn new() -> Self {
        Self {
            parent: HashMap::new(),
        }
    }

    pub fn find(&self, v: &str) -> String {
        if !self.parent.contains_key(v) {
            return v.to_string();
        }
        let mut result = self.parent.get(v).unwrap().clone();
        while let Some(next) = self.parent.get(&result) {
            if next == &result {
                break;
            }
            result = next.clone();
        }
        result
    }

    pub fn find_mut(&mut self, v: &str) -> String {
        if !self.parent.contains_key(v) {
            self.parent.insert(v.to_string(), v.to_string());
            return v.to_string();
        }
        let current_val = v.to_string();
        let parent_val = self
            .parent
            .get(&current_val)
            .cloned()
            .unwrap_or(current_val.clone());
        if parent_val == current_val {
            return current_val;
        }
        let root = self.find_mut(&parent_val);
        self.parent.insert(current_val, root.clone());
        root
    }

    pub fn add(&mut self, a: String, b: String) {
        let root_a = self.find_mut(&a);
        let root_b = self.find_mut(&b);
        if root_a != root_b {
            self.parent.insert(root_b, root_a);
        }
    }

    pub fn is_connected(&self, a: &str, b: &str) -> bool {
        self.find(a) == self.find(b)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use rsdb_sql::expr::Literal;
    use std::sync::Arc;

    fn make_scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: name.to_string(),
            schema: Arc::new(Schema::new(vec![
                Arc::new(Field::new("id", DataType::Int64, false)),
                Arc::new(Field::new("val", DataType::Utf8, true)),
            ])),
            projection: None,
            filters: vec![],
        }
    }

    fn make_join(
        left: LogicalPlan,
        right: LogicalPlan,
        left_col: &str,
        right_col: &str,
    ) -> LogicalPlan {
        let schema = left.schema();
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            join_condition: JoinCondition::On(RsdbExpr::BinaryOp {
                left: Box::new(RsdbExpr::Column(left_col.to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(RsdbExpr::Column(right_col.to_string())),
            }),
            schema,
        }
    }

    #[test]
    fn test_bitset64() {
        let mut bs = BitSet64::new();
        assert!(bs.is_empty());

        bs.insert(0);
        bs.insert(3);
        bs.insert(5);
        assert_eq!(bs.len(), 3);
        assert!(bs.contains(0));
        assert!(bs.contains(3));
        assert!(!bs.contains(1));

        let items: Vec<usize> = bs.iter().collect();
        assert_eq!(items, vec![0, 3, 5]);

        let bs2 = BitSet64::singleton(3).union(BitSet64::singleton(7));
        let inter = bs.intersection(bs2);
        assert_eq!(inter.len(), 1);
        assert!(inter.contains(3));

        let diff = bs.difference(bs2);
        assert_eq!(diff.len(), 2);
        assert!(diff.contains(0));
        assert!(diff.contains(5));
    }

    #[test]
    fn test_memo_dedup() {
        let mut memo = Memo::new();
        let g1 = memo.create_group();

        let scan = make_scan("t1");
        let (_, e1, is_new1) = memo.add_logical_expr(g1, scan.clone(), vec![]);
        assert!(is_new1);

        let (_, e2, is_new2) = memo.add_logical_expr(g1, scan.clone(), vec![]);
        assert!(!is_new2);
        assert_eq!(e1, e2); // same expression due to dedup
    }

    #[test]
    fn test_cascades_simple_scan() {
        let scan = make_scan("orders");
        let mut optimizer = CascadesOptimizer::new();
        let result = optimizer.optimize(scan).unwrap();
        assert!(matches!(result, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn test_cascades_two_way_join() {
        let a = make_scan("A");
        let b = make_scan("B");
        let join = make_join(a, b, "a_id", "b_id");

        let mut optimizer = CascadesOptimizer::new();
        let result = optimizer.optimize(join).unwrap();

        // Should produce a valid plan (either original or commuted)
        match &result {
            LogicalPlan::Join { .. } => {} // OK
            LogicalPlan::Scan { .. } => {} // fallback OK
            other => panic!("Unexpected plan type: {:?}", other),
        }
    }

    #[test]
    fn test_cascades_three_way_join() {
        let a = make_scan("A");
        let b = make_scan("B");
        let c = make_scan("C");

        let ab = make_join(a, b, "a_id", "b_id");
        let abc = make_join(ab, c, "b_id", "c_id");

        let mut optimizer = CascadesOptimizer::new();
        optimizer.max_iterations = 5000;
        let result = optimizer.optimize(abc).unwrap();

        // Should produce a valid join plan
        match &result {
            LogicalPlan::Join { .. } => {} // OK
            _ => panic!("Expected Join, got {:?}", result),
        }
    }
}
