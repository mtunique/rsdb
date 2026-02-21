//! RSDB Expression

use serde::{Deserialize, Serialize};

/// Subquery types for decorrelation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SubqueryType {
    /// IN subquery: WHERE x IN (SELECT ...)
    In,
    /// NOT IN subquery: WHERE x NOT IN (SELECT ...)
    NotIn,
    /// EXISTS subquery: WHERE EXISTS (SELECT ...)
    Exists,
    /// NOT EXISTS subquery: WHERE NOT EXISTS (SELECT ...)
    NotExists,
    /// Scalar subquery: SELECT (SELECT ...), WHERE y = (SELECT ...)
    Scalar,
}

/// Expression node
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Expr {
    /// Column reference
    Column(String),

    /// Literal value
    Literal(Literal),

    /// Binary operation
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },

    /// Unary operation
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },

    /// Aggregate function
    AggFunc {
        func: AggFunction,
        args: Vec<Expr>,
        distinct: bool,
    },

    /// Alias
    Alias { expr: Box<Expr>, name: String },

    /// Function call
    Function { name: String, args: Vec<Expr> },

    /// Cast
    Cast { expr: Box<Expr>, data_type: String },

    /// Case expression
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },

    /// Subquery - holds subquery query for later processing by optimizer
    Subquery {
        /// Subquery type
        subquery_type: SubqueryType,
        /// The expression being compared (for IN/EXISTS)
        expr: Option<Box<Expr>>,
        /// Outer references - columns from the outer query that this subquery references
        outer_refs: Vec<String>,
        /// The original SQL string of the subquery (for re-parsing in optimizer)
        sql: Option<String>,
    },
}

/// Literal value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Boolean(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Boolean(b) => write!(f, "{}", b),
            Literal::Int(i) => write!(f, "{}", i),
            Literal::Float(fl) => write!(f, "{}", fl),
            Literal::String(s) => write!(f, "'{}'", s),
        }
    }
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Literal::Null, Literal::Null) => true,
            (Literal::Boolean(a), Literal::Boolean(b)) => a == b,
            (Literal::Int(a), Literal::Int(b)) => a == b,
            (Literal::Float(a), Literal::Float(b)) => a.to_bits() == b.to_bits(),
            (Literal::String(a), Literal::String(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Literal {}

impl std::hash::Hash for Literal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Literal::Null => {},
            Literal::Boolean(v) => v.hash(state),
            Literal::Int(v) => v.hash(state),
            Literal::Float(v) => v.to_bits().hash(state),
            Literal::String(v) => v.hash(state),
        }
    }
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Like,
}

impl std::fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = match self {
            BinaryOperator::Eq => "=",
            BinaryOperator::Neq => "<>",
            BinaryOperator::Lt => "<",
            BinaryOperator::Lte => "<=",
            BinaryOperator::Gt => ">",
            BinaryOperator::Gte => ">=",
            BinaryOperator::And => "AND",
            BinaryOperator::Or => "OR",
            BinaryOperator::Plus => "+",
            BinaryOperator::Minus => "-",
            BinaryOperator::Multiply => "*",
            BinaryOperator::Divide => "/",
            BinaryOperator::Modulo => "%",
            BinaryOperator::Like => "LIKE",
        };
        write!(f, "{}", op)
    }
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOperator {
    Not,
    Minus,
    Plus,
}

impl std::fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = match self {
            UnaryOperator::Not => "NOT",
            UnaryOperator::Minus => "-",
            UnaryOperator::Plus => "+",
        };
        write!(f, "{}", op)
    }
}

/// Aggregate functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
}

impl std::fmt::Display for AggFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let func = match self {
            AggFunction::Count => "COUNT",
            AggFunction::Sum => "SUM",
            AggFunction::Avg => "AVG",
            AggFunction::Min => "MIN",
            AggFunction::Max => "MAX",
            AggFunction::CountDistinct => "COUNT(DISTINCT)",
        };
        write!(f, "{}", func)
    }
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column(name) => write!(f, "{}", name),
            Expr::Literal(lit) => write!(f, "{}", lit),
            Expr::BinaryOp { left, op, right } => write!(f, "({} {} {})", left, op, right),
            Expr::UnaryOp { op, expr } => write!(f, "{}({})", op, expr),
            Expr::AggFunc { func, args, distinct } => {
                let args_str = args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ");
                if *distinct && *func != AggFunction::CountDistinct {
                    write!(f, "{}(DISTINCT {})", func, args_str)
                } else {
                    write!(f, "{}({})", func, args_str)
                }
            }
            Expr::Alias { name, .. } => write!(f, "{}", name),
            Expr::Function { name, args } => {
                let args_str = args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ");
                write!(f, "{}({})", name, args_str)
            }
            Expr::Cast { expr, data_type } => write!(f, "CAST({} AS {})", expr, data_type),
            Expr::Case { operand, conditions, results, else_result } => {
                write!(f, "CASE")?;
                if let Some(op) = operand {
                    write!(f, " {}", op)?;
                }
                for (cond, res) in conditions.iter().zip(results.iter()) {
                    write!(f, " WHEN {} THEN {}", cond, res)?;
                }
                if let Some(else_res) = else_result {
                    write!(f, " ELSE {}", else_res)?;
                }
                write!(f, " END")
            }
            Expr::Subquery { subquery_type, .. } => write!(f, "{:?}(SUBQUERY)", subquery_type),
        }
    }
}

impl Expr {
    /// Get all column references in this expression
    pub fn get_columns(&self) -> Vec<String> {
        match self {
            Expr::Column(name) => vec![name.clone()],
            Expr::Literal(_) => vec![],
            Expr::BinaryOp { left, right, .. } => {
                let mut cols = left.get_columns();
                cols.extend(right.get_columns());
                cols
            }
            Expr::UnaryOp { expr, .. } => expr.get_columns(),
            Expr::AggFunc { args, .. } => args.iter().flat_map(|a| a.get_columns()).collect(),
            Expr::Alias { expr, .. } => expr.get_columns(),
            Expr::Function { args, .. } => args.iter().flat_map(|a| a.get_columns()).collect(),
            Expr::Cast { expr, .. } => expr.get_columns(),
            Expr::Case { operand, conditions, results, else_result } => {
                let mut cols = Vec::new();
                if let Some(op) = operand {
                    cols.extend(op.get_columns());
                }
                for e in conditions.iter().chain(results.iter()) {
                    cols.extend(e.get_columns());
                }
                if let Some(e) = else_result {
                    cols.extend(e.get_columns());
                }
                cols
            }
            Expr::Subquery {
                expr, outer_refs, ..
            } => {
                // Return outer_refs as columns (these are correlated references)
                let mut cols = outer_refs.clone();
                // Also get columns from the expression itself
                if let Some(e) = expr {
                    cols.extend(e.get_columns());
                }
                cols
            }
        }
    }

    /// Get all aggregate functions in this expression
    pub fn get_aggregates(&self) -> Vec<Expr> {
        match self {
            Expr::AggFunc { .. } => vec![self.clone()],
            Expr::Literal(_) | Expr::Column(_) => vec![],
            Expr::BinaryOp { left, right, .. } => {
                let mut aggs = left.get_aggregates();
                aggs.extend(right.get_aggregates());
                aggs
            }
            Expr::UnaryOp { expr, .. } => expr.get_aggregates(),
            Expr::Alias { expr, .. } => expr.get_aggregates(),
            Expr::Function { args, .. } => args.iter().flat_map(|a| a.get_aggregates()).collect(),
            Expr::Cast { expr, .. } => expr.get_aggregates(),
            Expr::Case { operand, conditions, results, else_result } => {
                let mut aggs = Vec::new();
                if let Some(op) = operand {
                    aggs.extend(op.get_aggregates());
                }
                for e in conditions.iter().chain(results.iter()) {
                    aggs.extend(e.get_aggregates());
                }
                if let Some(e) = else_result {
                    aggs.extend(e.get_aggregates());
                }
                aggs
            }
            Expr::Subquery { .. } => vec![], // Aggregates inside subqueries belong to those subqueries
        }
    }

    /// Replace aggregate functions with column references
    pub fn replace_aggregates(&self, aggs: &[(Expr, String)]) -> Expr {
        // First check if this expression itself is an aggregate function that matches
        for (agg, name) in aggs {
            if self == agg {
                return Expr::Column(name.clone());
            }
        }

        match self {
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(left.replace_aggregates(aggs)),
                op: *op,
                right: Box::new(right.replace_aggregates(aggs)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(expr.replace_aggregates(aggs)),
            },
            Expr::Alias { expr, name } => Expr::Alias {
                expr: Box::new(expr.replace_aggregates(aggs)),
                name: name.clone(),
            },
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args.iter().map(|a| a.replace_aggregates(aggs)).collect(),
            },
            Expr::Cast { expr, data_type } => Expr::Cast {
                expr: Box::new(expr.replace_aggregates(aggs)),
                data_type: data_type.clone(),
            },
            Expr::Case { operand, conditions, results, else_result } => Expr::Case {
                operand: operand.as_ref().map(|o| Box::new(o.replace_aggregates(aggs))),
                conditions: conditions.iter().map(|c| c.replace_aggregates(aggs)).collect(),
                results: results.iter().map(|r| r.replace_aggregates(aggs)).collect(),
                else_result: else_result.as_ref().map(|e| Box::new(e.replace_aggregates(aggs))),
            },
            _ => self.clone(),
        }
    }
}
