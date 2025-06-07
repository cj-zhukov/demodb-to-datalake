use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};

use color_eyre::Result;
use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

const TABLE: Table = Table::SeatsTable;

mod dyn_trait {
    use super::*;

    #[tokio::test]
    async fn test_seats() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let query = format!("select * from {}", table.as_ref());
        let res = worker.query_table(db.as_ref(), &query).await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_seats_string() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let query = format!("select * from {} order by aircraft_code", table.as_ref());
        let res = worker.query_table_to_string(db.as_ref(), &query).await?;
        let expected = vec![
            "aircraft_code: 319, seat_no: 10A, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 10B, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 10C, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 10D, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 10E, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 10F, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 11A, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 11B, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 11C, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 11D, fare_conditions: Economy",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_seats_json() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let query = format!("select * from {} order by aircraft_code", table.as_ref());
        let res = worker.query_table_to_json(db.as_ref(), &query).await?;
        let expected = "[
            {\"aircraft_code\":\"319\",\"seat_no\":\"10A\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"10B\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"10C\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"10D\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"10E\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"10F\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"11A\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"11B\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"11C\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"11D\",\"fare_conditions\":\"Economy\"}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_seats_df() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let ctx = SessionContext::new();
        let query = format!("select * from {} limit 2000", table.as_ref());
        let res = worker.query_table_to_df(db.as_ref(), &query, &ctx).await?;

        assert_eq!(res.schema().fields().len(), 3); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 1339); // rows count

        let rows = res
            .sort(vec![col("aircraft_code").sort(true, true)])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();
        assert_batches_eq!(
            &[
                "+---------------+---------+-----------------+",
                "| aircraft_code | seat_no | fare_conditions |",
                "+---------------+---------+-----------------+",
                "| 319           | 2C      | Business        |",
                "| 319           | 2D      | Business        |",
                "| 319           | 2F      | Business        |",
                "| 319           | 3A      | Business        |",
                "| 319           | 3C      | Business        |",
                "| 319           | 3D      | Business        |",
                "| 319           | 3F      | Business        |",
                "| 319           | 4A      | Business        |",
                "| 319           | 4C      | Business        |",
                "| 319           | 2A      | Business        |",
                "+---------------+---------+-----------------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}

mod stat_trait {
    use super::*;

    #[tokio::test]
    async fn test_seats() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {}", table.as_ref());
        let res = table.run_query_table(db.as_ref(), &query).await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_seats_string() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by aircraft_code", table.as_ref());
        let res = table.run_query_table_to_string(db.as_ref(), &query).await?;
        let expected = vec![
            "aircraft_code: 319, seat_no: 10A, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 10B, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 10C, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 10D, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 10E, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 10F, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 11A, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 11B, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 11C, fare_conditions: Economy",
            "aircraft_code: 319, seat_no: 11D, fare_conditions: Economy",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_seats_json() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by aircraft_code", table.as_ref());
        let res = table.run_query_table_to_json(db.as_ref(), &query).await?;
        let expected = "[
            {\"aircraft_code\":\"319\",\"seat_no\":\"10A\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"10B\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"10C\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"10D\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"10E\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"10F\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"11A\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"11B\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"11C\",\"fare_conditions\":\"Economy\"},
            {\"aircraft_code\":\"319\",\"seat_no\":\"11D\",\"fare_conditions\":\"Economy\"}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_seats_df() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let ctx = SessionContext::new();
        let query = format!("select * from {} limit 2000", table.as_ref());
        let res = table
            .run_query_table_to_df(db.as_ref(), &query, &ctx)
            .await?;

        assert_eq!(res.schema().fields().len(), 3); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 1339); // rows count

        let rows = res
            .sort(vec![col("aircraft_code").sort(true, true)])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();
        assert_batches_eq!(
            &[
                "+---------------+---------+-----------------+",
                "| aircraft_code | seat_no | fare_conditions |",
                "+---------------+---------+-----------------+",
                "| 319           | 2C      | Business        |",
                "| 319           | 2D      | Business        |",
                "| 319           | 2F      | Business        |",
                "| 319           | 3A      | Business        |",
                "| 319           | 3C      | Business        |",
                "| 319           | 3D      | Business        |",
                "| 319           | 3F      | Business        |",
                "| 319           | 4A      | Business        |",
                "| 319           | 4C      | Business        |",
                "| 319           | 2A      | Business        |",
                "+---------------+---------+-----------------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}
