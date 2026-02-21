//! Static topology scheduler for MPP execution

use rsdb_common::{NodeId, Result, RsdbError};
use rsdb_planner::fragment_planner::PlanFragment;
use rsdb_sql::logical_plan::{LogicalPlan, RemoteSource};
use std::collections::HashMap;

/// Pre-calculated assignment of a fragment to workers
#[derive(Debug, Clone)]
pub struct FragmentAssignment {
    pub fragment_id: usize,
    /// Addresses of workers assigned to this fragment
    pub worker_addrs: Vec<String>,
}

/// Static schedule for a query job
#[derive(Debug, Clone)]
pub struct JobSchedule {
    pub job_id: String,
    /// Assignments per fragment ID
    pub assignments: HashMap<usize, FragmentAssignment>,
}

pub struct Scheduler;

impl Scheduler {
    pub fn new() -> Self {
        Self
    }

    /// Pre-determine the worker assignment for all fragments in the DAG.
    pub fn create_job_schedule(
        &self,
        job_id: &str,
        fragments: &[PlanFragment],
        workers: &[rsdb_common::types::WorkerInfo],
    ) -> Result<JobSchedule> {
        if workers.is_empty() {
            return Err(RsdbError::Coordinator("No workers available".to_string()));
        }

        let mut assignments = HashMap::new();
        for (i, frag) in fragments.iter().enumerate() {
            // Assign each fragment to a single worker (round-robin).
            // True shuffle-based MPP would assign Exchange children to all workers,
            // but for correctness without partitioning awareness, one worker per fragment
            // avoids result duplication.
            let worker_idx = i % workers.len();
            let addrs = vec![workers[worker_idx].addr.clone()];

            assignments.insert(frag.id, FragmentAssignment {
                fragment_id: frag.id,
                worker_addrs: addrs,
            });
        }

        Ok(JobSchedule {
            job_id: job_id.to_string(),
            assignments,
        })
    }

    /// Rewrite a fragment's logical plan to include static source addresses.
    /// This converts LogicalPlan::Exchange into LogicalPlan::RemoteExchange.
    pub fn rewrite_plan_for_execution(
        &self,
        plan: &LogicalPlan,
        schedule: &JobSchedule,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Exchange { input, partitioning } => {
                let rewritten_input = self.rewrite_plan_for_execution(input, schedule)?;
                
                let mut sources = Vec::new();
                for assignment in schedule.assignments.values() {
                    for (i, addr) in assignment.worker_addrs.iter().enumerate() {
                        sources.push(RemoteSource {
                            worker_addr: addr.clone(),
                            task_id: format!("{}-{}-{}", schedule.job_id, assignment.fragment_id, i),
                        });
                    }
                }

                Ok(LogicalPlan::RemoteExchange {
                    input: Box::new(rewritten_input),
                    partitioning: partitioning.clone(),
                    sources,
                })
            }
            LogicalPlan::RemoteExchange { input, partitioning, sources } => {
                let rewritten_input = self.rewrite_plan_for_execution(input, schedule)?;
                Ok(LogicalPlan::RemoteExchange {
                    input: Box::new(rewritten_input),
                    partitioning: partitioning.clone(),
                    sources: sources.clone(),
                })
            }
            // Handle other containers recursively
            LogicalPlan::Project { input, expr, schema } => {
                Ok(LogicalPlan::Project {
                    input: Box::new(self.rewrite_plan_for_execution(input, schedule)?),
                    expr: expr.clone(),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Filter { input, predicate } => {
                Ok(LogicalPlan::Filter {
                    input: Box::new(self.rewrite_plan_for_execution(input, schedule)?),
                    predicate: predicate.clone(),
                })
            }
            LogicalPlan::Aggregate { input, group_expr, aggregate_expr, schema } => {
                Ok(LogicalPlan::Aggregate {
                    input: Box::new(self.rewrite_plan_for_execution(input, schedule)?),
                    group_expr: group_expr.clone(),
                    aggregate_expr: aggregate_expr.clone(),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Join { left, right, join_type, join_condition, schema } => {
                Ok(LogicalPlan::Join {
                    left: Box::new(self.rewrite_plan_for_execution(left, schedule)?),
                    right: Box::new(self.rewrite_plan_for_execution(right, schedule)?),
                    join_type: *join_type,
                    join_condition: join_condition.clone(),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::SubqueryAlias { input, alias } => {
                Ok(LogicalPlan::SubqueryAlias {
                    input: Box::new(self.rewrite_plan_for_execution(input, schedule)?),
                    alias: alias.clone(),
                })
            }
            LogicalPlan::Sort { input, expr } => {
                Ok(LogicalPlan::Sort {
                    input: Box::new(self.rewrite_plan_for_execution(input, schedule)?),
                    expr: expr.clone(),
                })
            }
            LogicalPlan::Limit { input, limit, offset } => {
                Ok(LogicalPlan::Limit {
                    input: Box::new(self.rewrite_plan_for_execution(input, schedule)?),
                    limit: *limit,
                    offset: *offset,
                })
            }
            LogicalPlan::CrossJoin { left, right, schema } => {
                Ok(LogicalPlan::CrossJoin {
                    left: Box::new(self.rewrite_plan_for_execution(left, schedule)?),
                    right: Box::new(self.rewrite_plan_for_execution(right, schedule)?),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::Union { inputs, schema } => {
                let new_inputs: Result<Vec<_>> = inputs.iter()
                    .map(|p| self.rewrite_plan_for_execution(p, schedule))
                    .collect();
                Ok(LogicalPlan::Union {
                    inputs: new_inputs?,
                    schema: schema.clone(),
                })
            }
            _ => Ok(plan.clone())
        }
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}
