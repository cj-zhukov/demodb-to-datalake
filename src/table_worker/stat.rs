use async_trait::async_trait;
use datafusion::prelude::*;
use sqlx::PgPool;

use crate::AppError;

#[async_trait]
pub trait TableWorkerStatic {
    async fn query_table(pool: &PgPool, query: &str) -> Result<(), AppError>;
    async fn query_table_to_string(pool: &PgPool, query: &str) -> Result<Vec<String>, AppError>;
    async fn query_table_to_json(pool: &PgPool, query: &str) -> Result<String, AppError>;
    async fn query_table_to_df(pool: &PgPool, query: &str, ctx: &SessionContext) -> Result<DataFrame, AppError>;
}

pub mod helpers {
    use super::*;
    
    pub async fn process_query_table<T: TableWorkerStatic>(pool: &PgPool, query: &str) -> Result<(), AppError> {
        T::query_table(pool, query).await
    }

    pub async fn process_table_to_string<T: TableWorkerStatic>(pool: &PgPool, query: &str) -> Result<Vec<String>, AppError> {
        T::query_table_to_string(pool, query).await
    }

    pub async fn process_table_to_json<T: TableWorkerStatic>(pool: &PgPool, query: &str) -> Result<String, AppError> {
        T::query_table_to_json(pool, query).await
    }

    pub async fn process_table_to_df<T: TableWorkerStatic>(pool: &PgPool, query: &str, ctx: &SessionContext) -> Result<DataFrame, AppError> {
        T::query_table_to_df(pool, query, ctx).await
    }
}
