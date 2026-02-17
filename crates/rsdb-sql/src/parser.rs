//! SQL Parser wrapper

use rsdb_common::{Result, RsdbError};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// SQL Parser wrapper
pub struct SqlParser;

impl SqlParser {
    /// Parse SQL into AST
    pub fn parse(sql: &str) -> Result<Vec<sqlparser::ast::Statement>> {
        let dialect = GenericDialect {};
        Parser::parse_sql(&dialect, sql).map_err(|e| RsdbError::SqlParse(e.to_string()))
    }

    /// Parse a single SQL statement
    pub fn parse_statement(sql: &str) -> Result<sqlparser::ast::Statement> {
        let statements = Self::parse(sql)?;
        statements
            .into_iter()
            .next()
            .ok_or_else(|| RsdbError::SqlParse("Empty SQL".to_string()))
    }
}
