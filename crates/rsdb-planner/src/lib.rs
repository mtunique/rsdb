//! RSDB Planner - Optimization and Substrait conversion

pub mod cascades;
pub mod cbo;
pub mod fragment_planner;
pub mod memo;
pub mod optimizer;
pub mod predicate_pushdown;
pub mod property;
pub mod stats_collector;
pub mod stats_derivation;
pub mod substrait_consumer;
pub mod substrait_producer;

pub use cascades::{
    has_joins, BitSet64, CascadesOptimizer, CascadesRule, JoinEdge, JoinEnumOnGraphRule, JoinHyperGraph, MinCutBranch, Partition, RuleKind, RuleOutput, Task, UnionFind,
};
pub use cbo::{
    CBOConfig, CBOContext, CBOOptimizer, ColumnStats, Cost, CostModel, InMemoryStatsProvider, PlanStats,
    StatsProvider,
};
pub use fragment_planner::FragmentPlanner;
pub use memo::{
    ExprFingerprint, ExprType, Group, GroupExpression, Memo, PhysicalType, Winner,
};
pub use optimizer::{
    ConstantFolding, FullOptimizer, Optimizer, OptimizerRule, PredicatePushdown, ProjectionPruning,
};
pub use property::{
    Property, Statistics,
};
pub use rsdb_sql::logical_plan::{Partitioning};
pub use stats_collector::{catalog_stats_to_plan_stats, collect_statistics};
pub use substrait_consumer::SubstraitConsumer;
pub use substrait_producer::SubstraitProducer;
