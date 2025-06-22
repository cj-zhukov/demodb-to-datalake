use super::constants::{tables_names::*, MAX_ROWS};

use sqlparser::ast::{Expr, Ident, LimitClause, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use thiserror::Error;

pub const ALL_TABLE_NAMES: &[&str] = &[
    AIRCRAFTS_DATA_TABLE_NAME,
    AIRPORTS_DATA_TABLE_NAME,
    BOARDING_PASSES_TABLE_NAME,
    BOOKINGS_TABLE_NAME,
    FLIGHTS_TABLE_NAME,
    SEATS_TABLE_NAME,
    TICKETS_TABLE_NAME,
    TICKET_FLIGHTS_TABLE_NAME,
];

#[derive(Debug, Error, PartialEq)]
pub enum QueryParserError {
    #[error("SQL parse error")]
    SqlParseError(#[from] ParserError),

    #[error("Invalid query: unsupported table")]
    InvalidTableName,

    #[error("Select query type not found")]
    SelectQueryNotFound,

    #[error("Unsupported query type")]
    UnsupportedQueryType,
}

pub fn prepare_query(query: &str) -> Result<String, QueryParserError> {
    let dialect = GenericDialect {};
    let mut ast = Parser::parse_sql(&dialect, query)?;
    if let Some(Statement::Query(query)) = ast.get_mut(0) {
        // check query contains correct table name
        let valid_table = match &*query.body {
            SetExpr::Select(select) => {
                if let Some(from_table) = select.from.get(0) {
                    if let TableFactor::Table { name, .. } = &from_table.relation {
                        name.0
                            .last()
                            .map(|ident| ALL_TABLE_NAMES.contains(&ident.to_string().as_str()))
                            .unwrap_or(false)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            _ => false,
        };
        if !valid_table {
            return Err(QueryParserError::InvalidTableName);
        }

        if let SetExpr::Select(_select) = &mut *query.body {
            // query contains limit
            if query.limit_clause.is_none() {
                query.limit_clause = Some(LimitClause::LimitOffset {
                    limit: Some(Expr::Value(
                        Value::Number(MAX_ROWS.to_string(), false).into(),
                    )),
                    offset: None,
                    limit_by: vec![],
                })
            };

            Ok(ast[0].to_string())
        } else {
            Err(QueryParserError::SelectQueryNotFound)
        }
    } else {
        Err(QueryParserError::UnsupportedQueryType)
    }
}

pub fn contains_column(expr: &Expr, col_name: &str) -> bool {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            contains_column(left, col_name) || contains_column(right, col_name)
        }
        Expr::Identifier(Ident { value, .. }) => value.eq_ignore_ascii_case(col_name),
        Expr::Nested(inner) => contains_column(inner, col_name),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("select * from aircrafts_data", Ok("SELECT * FROM aircrafts_data LIMIT 10".to_string()))]
    #[case("select * from airports_data", Ok("SELECT * FROM airports_data LIMIT 10".to_string()))]
    #[case("select * from boarding_passes", Ok("SELECT * FROM boarding_passes LIMIT 10".to_string()))]
    #[case("select * from bookings", Ok("SELECT * FROM bookings LIMIT 10".to_string()))]
    #[case("select * from flights", Ok("SELECT * FROM flights LIMIT 10".to_string()))]
    #[case("select * from seats", Ok("SELECT * FROM seats LIMIT 10".to_string()))]
    #[case("select * from tickets", Ok("SELECT * FROM tickets LIMIT 10".to_string()))]
    #[case("select * from ticket_flights", Ok("SELECT * FROM ticket_flights LIMIT 10".to_string()))]
    #[case("select * from foo", Err(QueryParserError::InvalidTableName))]
    #[case(
        "delete from aircrafts_data",
        Err(QueryParserError::UnsupportedQueryType)
    )]
    #[case(
        "update aircrafts_data set data_type = 'foo' where data_type = 'bar'",
        Err(QueryParserError::UnsupportedQueryType)
    )]
    #[case(
        "insert into aircrafts_data(file_name) values('foo')",
        Err(QueryParserError::UnsupportedQueryType)
    )]
    #[case("foo bar baz", Err(QueryParserError::SqlParseError(ParserError::ParserError("Expected: an SQL statement, found: foo at Line: 1, Column: 1".to_string()))))]
    fn prepare_query_test(#[case] input: &str, #[case] expected: Result<String, QueryParserError>) {
        assert_eq!(expected, prepare_query(input));
    }
}
