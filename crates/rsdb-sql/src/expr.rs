//! RSDB Expression

use serde::{Deserialize, Serialize};

/// Subquery types for decorrelation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Int(i64),
    Float(f64),
    String(String),
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOperator {
    Not,
    Minus,
    Plus,
}

/// Aggregate functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
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
}
