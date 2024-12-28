use demodb_to_datalake::{PostgresDb, Table};
use demodb_to_datalake::MAX_DB_CONS;

use color_eyre::Result;
use datafusion::prelude::*;
use sqlx::PgPool;

pub struct TestApp {
    pub db: PgPool,
    pub table: Table,
}

impl TestApp {
    pub async fn new(db_url: &str, table: Table) -> Result<Self> {
        let db = PostgresDb::builder()
            .with_url(db_url)
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;

        Ok(Self { db: db.as_ref().clone(), table })
    }
}

impl TestApp {
    pub async fn test_aircrafts_data(&self) -> Result<DataFrame> {
        let worker = self.table.to_worker();
        let sql = format!("select * from {} limit 10", self.table.as_ref());
        let res = worker.query_table_to_df(&self.db, Some(&sql)).await?;
        Ok(res)
    }
}