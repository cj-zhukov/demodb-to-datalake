use demodb_to_datalake::{PostgresDb, Table, MAX_DB_CONS};

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
        let sql = format!("select * from {}", self.table.as_ref());
        let res = worker.query_table_to_df(&self.db, Some(&sql)).await?;
        Ok(res)
    }

    pub async fn test_airports_data(&self) -> Result<DataFrame> {
        let worker = self.table.to_worker();
        let sql = format!("select 
                                    airport_code, 
                                    airport_name, 
                                    city, 
                                    json_build_object('x', coordinates[0], 'y', coordinates[1]) as coordinates, 
                                    timezone 
                                from {}", self.table.as_ref());
        let res = worker.query_table_to_df(&self.db, Some(&sql)).await?;
        Ok(res)
    }

    pub async fn test_boarding_passes(&self) -> Result<DataFrame> {
        let worker = self.table.to_worker();
        let sql = format!("select * from {} limit 10", self.table.as_ref());
        let res = worker.query_table_to_df(&self.db, Some(&sql)).await?;
        Ok(res)
    }

    pub async fn test_bookings(&self) -> Result<DataFrame> {
        let worker = self.table.to_worker();
        let sql = format!("select * from {} limit 10", self.table.as_ref());
        let res = worker.query_table_to_df(&self.db, Some(&sql)).await?;
        Ok(res)
    }

    pub async fn test_flights(&self) -> Result<DataFrame> {
        let worker = self.table.to_worker();
        let sql = format!("select * from {} 
                                    where 1 = 1
                                    and actual_departure is not null 
                                    and actual_arrival is not null 
                                    limit 10", self.table.as_ref());
        let res = worker.query_table_to_df(&self.db, Some(&sql)).await?;
        Ok(res)
    }

    pub async fn test_seats(&self) -> Result<DataFrame> {
        let worker = self.table.to_worker();
        let sql = format!("select * from {}", self.table.as_ref());
        let res = worker.query_table_to_df(&self.db, Some(&sql)).await?;
        Ok(res)
    }

    pub async fn test_ticket_flights(&self) -> Result<DataFrame> {
        let worker = self.table.to_worker();
        let sql = format!("select * from {} order by ticket_no limit 10", self.table.as_ref());
        let res = worker.query_table_to_df(&self.db, Some(&sql)).await?;
        Ok(res)
    }

    pub async fn test_tickets(&self) -> Result<DataFrame> {
        let worker = self.table.to_worker();
        let sql = format!("select * from {} order by ticket_no limit 10", self.table.as_ref());
        let res = worker.query_table_to_df(&self.db, Some(&sql)).await?;
        Ok(res)
    }
}