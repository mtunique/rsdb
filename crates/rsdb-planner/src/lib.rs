//! RSDB Planner - Optimization and Substrait conversion

pub mod cascades;
pub mod cbo;
pub mod fragment_planner;
pub mod optimizer;
pub mod stats_collector;
pub mod substrait_consumer;
pub mod substrait_producer;

pub use cascades::{
    has_joins, BitSet64, CascadesOptimizer, CascadesRule, Cost, ExprFingerprint, ExprType, Group,
    GroupExpression, JoinEdge, JoinEnumOnGraphRule, JoinHyperGraph, Memo, MinCutBranch, Partition,
    Partitioning, PhysicalType, Property, RuleKind, RuleOutput, Statistics, Task, UnionFind,
    Winner,
};
pub use cbo::{
    CBOConfig, CBOContext, CBOOptimizer, ColumnStats, CostModel, InMemoryStatsProvider, PlanStats,
    StatsProvider,
};
pub use fragment_planner::FragmentPlanner;
pub use optimizer::{
    ConstantFolding, FullOptimizer, Optimizer, OptimizerRule, PredicatePushdown, ProjectionPruning,
};
pub use stats_collector::{catalog_stats_to_plan_stats, collect_statistics};
pub use substrait_consumer::SubstraitConsumer;
pub use substrait_producer::SubstraitProducer;
