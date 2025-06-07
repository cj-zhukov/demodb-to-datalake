use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};

use color_eyre::Result;
use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

const TABLE: Table = Table::BookingsTable;

mod dyn_trait {
    use super::*;

    #[tokio::test]
    async fn test_bookings() -> Result<()> {
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
    async fn test_bookings_string() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let query = format!("select * from {} order by book_ref", table.as_ref());
        let res = worker.query_table_to_string(db.as_ref(), &query).await?;
        let expected = vec![
            "book_ref: 000004, book_date: 2016-08-13 12:40:00 UTC, total_amount: 55800.00",
            "book_ref: 00000F, book_date: 2017-07-05 00:12:00 UTC, total_amount: 265700.00",
            "book_ref: 000010, book_date: 2017-01-08 16:45:00 UTC, total_amount: 50900.00",
            "book_ref: 000012, book_date: 2017-07-14 06:02:00 UTC, total_amount: 37900.00",
            "book_ref: 000026, book_date: 2016-08-30 08:08:00 UTC, total_amount: 95600.00",
            "book_ref: 00002D, book_date: 2017-05-20 15:45:00 UTC, total_amount: 114700.00",
            "book_ref: 000034, book_date: 2016-08-08 02:46:00 UTC, total_amount: 49100.00",
            "book_ref: 00003F, book_date: 2016-12-12 12:02:00 UTC, total_amount: 109800.00",
            "book_ref: 000048, book_date: 2016-09-16 22:57:00 UTC, total_amount: 92400.00",
            "book_ref: 00004A, book_date: 2016-10-13 18:57:00 UTC, total_amount: 29000.00",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_bookings_json() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let query = format!("select * from {} order by book_ref", table.as_ref());
        let res = worker.query_table_to_json(db.as_ref(), &query).await?;
        let expected = "[
            {\"book_date\":\"2016-08-13T12:40:00+00:00\",\"book_ref\":\"000004\",\"total_amount\":\"55800.00\"},
            {\"book_date\":\"2017-07-05T00:12:00+00:00\",\"book_ref\":\"00000F\",\"total_amount\":\"265700.00\"},
            {\"book_date\":\"2017-01-08T16:45:00+00:00\",\"book_ref\":\"000010\",\"total_amount\":\"50900.00\"},
            {\"book_date\":\"2017-07-14T06:02:00+00:00\",\"book_ref\":\"000012\",\"total_amount\":\"37900.00\"},
            {\"book_date\":\"2016-08-30T08:08:00+00:00\",\"book_ref\":\"000026\",\"total_amount\":\"95600.00\"},
            {\"book_date\":\"2017-05-20T15:45:00+00:00\",\"book_ref\":\"00002D\",\"total_amount\":\"114700.00\"},
            {\"book_date\":\"2016-08-08T02:46:00+00:00\",\"book_ref\":\"000034\",\"total_amount\":\"49100.00\"},
            {\"book_date\":\"2016-12-12T12:02:00+00:00\",\"book_ref\":\"00003F\",\"total_amount\":\"109800.00\"},
            {\"book_date\":\"2016-09-16T22:57:00+00:00\",\"book_ref\":\"000048\",\"total_amount\":\"92400.00\"},
            {\"book_date\":\"2016-10-13T18:57:00+00:00\",\"book_ref\":\"00004A\",\"total_amount\":\"29000.00\"}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_bookings_df() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let ctx = SessionContext::new();
        let query = format!("select * from {}", table.as_ref());
        let res = worker.query_table_to_df(db.as_ref(), &query, &ctx).await?;

        assert_eq!(res.schema().fields().len(), 3); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

        let rows = res
            .sort(vec![col("book_ref").sort(true, true)])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();
        assert_batches_eq!(
            &[
                "+----------+---------------------------+--------------+",
                "| book_ref | book_date                 | total_amount |",
                "+----------+---------------------------+--------------+",
                "| 000004   | 2016-08-13T12:40:00+00:00 | 55800.00     |",
                "| 00000F   | 2017-07-05T00:12:00+00:00 | 265700.00    |",
                "| 000010   | 2017-01-08T16:45:00+00:00 | 50900.00     |",
                "| 000012   | 2017-07-14T06:02:00+00:00 | 37900.00     |",
                "| 000026   | 2016-08-30T08:08:00+00:00 | 95600.00     |",
                "| 00002D   | 2017-05-20T15:45:00+00:00 | 114700.00    |",
                "| 000034   | 2016-08-08T02:46:00+00:00 | 49100.00     |",
                "| 00003F   | 2016-12-12T12:02:00+00:00 | 109800.00    |",
                "| 000048   | 2016-09-16T22:57:00+00:00 | 92400.00     |",
                "| 00004A   | 2016-10-13T18:57:00+00:00 | 29000.00     |",
                "+----------+---------------------------+--------------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}

mod stat_trait {
    use super::*;

    #[tokio::test]
    async fn test_bookings() -> Result<()> {
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
    async fn test_bookings_string() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by book_ref", table.as_ref());
        let res = table.run_query_table_to_string(db.as_ref(), &query).await?;
        let expected = vec![
            "book_ref: 000004, book_date: 2016-08-13 12:40:00 UTC, total_amount: 55800.00",
            "book_ref: 00000F, book_date: 2017-07-05 00:12:00 UTC, total_amount: 265700.00",
            "book_ref: 000010, book_date: 2017-01-08 16:45:00 UTC, total_amount: 50900.00",
            "book_ref: 000012, book_date: 2017-07-14 06:02:00 UTC, total_amount: 37900.00",
            "book_ref: 000026, book_date: 2016-08-30 08:08:00 UTC, total_amount: 95600.00",
            "book_ref: 00002D, book_date: 2017-05-20 15:45:00 UTC, total_amount: 114700.00",
            "book_ref: 000034, book_date: 2016-08-08 02:46:00 UTC, total_amount: 49100.00",
            "book_ref: 00003F, book_date: 2016-12-12 12:02:00 UTC, total_amount: 109800.00",
            "book_ref: 000048, book_date: 2016-09-16 22:57:00 UTC, total_amount: 92400.00",
            "book_ref: 00004A, book_date: 2016-10-13 18:57:00 UTC, total_amount: 29000.00",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_bookings_json() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by book_ref", table.as_ref());
        let res = table.run_query_table_to_json(db.as_ref(), &query).await?;
        let expected = "[
            {\"book_date\":\"2016-08-13T12:40:00+00:00\",\"book_ref\":\"000004\",\"total_amount\":\"55800.00\"},
            {\"book_date\":\"2017-07-05T00:12:00+00:00\",\"book_ref\":\"00000F\",\"total_amount\":\"265700.00\"},
            {\"book_date\":\"2017-01-08T16:45:00+00:00\",\"book_ref\":\"000010\",\"total_amount\":\"50900.00\"},
            {\"book_date\":\"2017-07-14T06:02:00+00:00\",\"book_ref\":\"000012\",\"total_amount\":\"37900.00\"},
            {\"book_date\":\"2016-08-30T08:08:00+00:00\",\"book_ref\":\"000026\",\"total_amount\":\"95600.00\"},
            {\"book_date\":\"2017-05-20T15:45:00+00:00\",\"book_ref\":\"00002D\",\"total_amount\":\"114700.00\"},
            {\"book_date\":\"2016-08-08T02:46:00+00:00\",\"book_ref\":\"000034\",\"total_amount\":\"49100.00\"},
            {\"book_date\":\"2016-12-12T12:02:00+00:00\",\"book_ref\":\"00003F\",\"total_amount\":\"109800.00\"},
            {\"book_date\":\"2016-09-16T22:57:00+00:00\",\"book_ref\":\"000048\",\"total_amount\":\"92400.00\"},
            {\"book_date\":\"2016-10-13T18:57:00+00:00\",\"book_ref\":\"00004A\",\"total_amount\":\"29000.00\"}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_bookings_df() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let ctx = SessionContext::new();
        let query = format!("select * from {}", table.as_ref());
        let res = table
            .run_query_table_to_df(db.as_ref(), &query, &ctx)
            .await?;

        assert_eq!(res.schema().fields().len(), 3); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

        let rows = res
            .sort(vec![col("book_ref").sort(true, true)])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();
        assert_batches_eq!(
            &[
                "+----------+---------------------------+--------------+",
                "| book_ref | book_date                 | total_amount |",
                "+----------+---------------------------+--------------+",
                "| 000004   | 2016-08-13T12:40:00+00:00 | 55800.00     |",
                "| 00000F   | 2017-07-05T00:12:00+00:00 | 265700.00    |",
                "| 000010   | 2017-01-08T16:45:00+00:00 | 50900.00     |",
                "| 000012   | 2017-07-14T06:02:00+00:00 | 37900.00     |",
                "| 000026   | 2016-08-30T08:08:00+00:00 | 95600.00     |",
                "| 00002D   | 2017-05-20T15:45:00+00:00 | 114700.00    |",
                "| 000034   | 2016-08-08T02:46:00+00:00 | 49100.00     |",
                "| 00003F   | 2016-12-12T12:02:00+00:00 | 109800.00    |",
                "| 000048   | 2016-09-16T22:57:00+00:00 | 92400.00     |",
                "| 00004A   | 2016-10-13T18:57:00+00:00 | 29000.00     |",
                "+----------+---------------------------+--------------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}
