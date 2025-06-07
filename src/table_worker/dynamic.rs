use async_trait::async_trait;
use datafusion::prelude::*;
use sqlx::PgPool;

use crate::AppError;

#[async_trait]
pub trait TableWorkerDyn {
    async fn query_table(&self, pool: &PgPool, query: &str) -> Result<(), AppError>;
    async fn query_table_to_string(
        &self,
        pool: &PgPool,
        query: &str,
    ) -> Result<Vec<String>, AppError>;
    async fn query_table_to_json(&self, pool: &PgPool, query: &str) -> Result<String, AppError>;
    async fn query_table_to_df(
        &self,
        pool: &PgPool,
        query: &str,
        ctx: &SessionContext,
    ) -> Result<DataFrame, AppError>;
}
