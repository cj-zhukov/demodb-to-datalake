use async_trait::async_trait;
use datafusion::prelude::*;
use sqlx::PgPool;

use crate::AppError;

#[async_trait]
pub trait TableWorker {
    async fn query_table(&self, pool: &PgPool) -> Result<(), AppError>;
    async fn query_table_to_string(&self, pool: &PgPool) -> Result<Vec<String>, AppError>;
    async fn query_table_to_json(&self, pool: &PgPool) -> Result<String, AppError>;
    async fn query_table_to_df(&self, pool: &PgPool, query: Option<&str>, ctx: &SessionContext) -> Result<DataFrame, AppError>;
}

#[async_trait]
pub trait TableWorkerStatic {
    async fn query_table(pool: &PgPool) -> Result<(), AppError>;
    async fn query_table_to_string(pool: &PgPool) -> Result<Vec<String>, AppError>;
    async fn query_table_to_json(pool: &PgPool) -> Result<String, AppError>;
    async fn query_table_to_df(pool: &PgPool, query: Option<&str>, ctx: &SessionContext) -> Result<DataFrame, AppError>;
}

pub async fn process_query_table<T: TableWorkerStatic>(pool: &PgPool) -> Result<(), AppError> {
    T::query_table(pool).await
}

pub async fn process_table_to_string<T: TableWorkerStatic>(pool: &PgPool) -> Result<Vec<String>, AppError> {
    T::query_table_to_string(pool).await
}

pub async fn process_table_to_json<T: TableWorkerStatic>(pool: &PgPool) -> Result<String, AppError> {
    T::query_table_to_json(pool).await
}

pub async fn process_table_to_df<T: TableWorkerStatic>(pool: &PgPool, query: Option<&str>, ctx: &SessionContext) -> Result<DataFrame, AppError> {
    T::query_table_to_df(pool, query, ctx).await
}
