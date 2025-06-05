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