use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};

use color_eyre::Result;
use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

const TABLE: Table = Table::FlightsTable;

mod dyn_trait {
    use super::*;

    #[tokio::test]
    async fn test_flights() -> Result<()> {
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
    async fn test_flights_string() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let query = format!("select * from {} order by flight_id", table.as_ref());
        let res = worker.query_table_to_string(db.as_ref(), &query).await?;
        let expected = vec![
            "flight_id: 1, flight_no: PG0403, scheduled_departure: 2017-06-13 08:25:00 UTC, scheduled_arrival: 2017-06-13 09:20:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-06-13T08:29:00Z), actual_arrival: Some(2017-06-13T09:24:00Z)", 
            "flight_id: 2, flight_no: PG0404, scheduled_departure: 2017-06-13 16:05:00 UTC, scheduled_arrival: 2017-06-13 17:00:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-06-13T16:11:00Z), actual_arrival: Some(2017-06-13T17:06:00Z)", 
            "flight_id: 3, flight_no: PG0405, scheduled_departure: 2017-06-13 06:35:00 UTC, scheduled_arrival: 2017-06-13 07:30:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-06-13T06:38:00Z), actual_arrival: Some(2017-06-13T07:33:00Z)", 
            "flight_id: 4, flight_no: PG0402, scheduled_departure: 2017-02-10 09:25:00 UTC, scheduled_arrival: 2017-02-10 10:20:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-02-10T09:30:00Z), actual_arrival: Some(2017-02-10T10:26:00Z)", 
            "flight_id: 5, flight_no: PG0403, scheduled_departure: 2017-02-10 08:25:00 UTC, scheduled_arrival: 2017-02-10 09:20:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-02-10T08:28:00Z), actual_arrival: Some(2017-02-10T09:22:00Z)", 
            "flight_id: 6, flight_no: PG0403, scheduled_departure: 2016-12-08 08:25:00 UTC, scheduled_arrival: 2016-12-08 09:20:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2016-12-08T08:31:00Z), actual_arrival: Some(2016-12-08T09:25:00Z)", 
            "flight_id: 7, flight_no: PG0404, scheduled_departure: 2017-02-10 16:05:00 UTC, scheduled_arrival: 2017-02-10 17:00:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-02-10T16:07:00Z), actual_arrival: Some(2017-02-10T17:02:00Z)", 
            "flight_id: 8, flight_no: PG0404, scheduled_departure: 2016-12-08 16:05:00 UTC, scheduled_arrival: 2016-12-08 17:00:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2016-12-08T16:10:00Z), actual_arrival: Some(2016-12-08T17:05:00Z)", 
            "flight_id: 9, flight_no: PG0404, scheduled_departure: 2016-11-26 16:05:00 UTC, scheduled_arrival: 2016-11-26 17:00:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2016-11-26T16:09:00Z), actual_arrival: Some(2016-11-26T17:03:00Z)", 
            "flight_id: 10, flight_no: PG0404, scheduled_departure: 2017-01-16 16:05:00 UTC, scheduled_arrival: 2017-01-16 17:00:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-01-16T16:08:00Z), actual_arrival: Some(2017-01-16T17:03:00Z)",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_flights_json() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let query = format!("select * from {} order by flight_id", table.as_ref());
        let res = worker.query_table_to_json(db.as_ref(), &query).await?;
        let expected = "[
            {\"actual_arrival\":\"2017-06-13T09:24:00+00:00\",\"actual_departure\":\"2017-06-13T08:29:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":1,\"flight_no\":\"PG0403\",\"scheduled_arrival\":\"2017-06-13T09:20:00+00:00\",\"scheduled_departure\":\"2017-06-13T08:25:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-06-13T17:06:00+00:00\",\"actual_departure\":\"2017-06-13T16:11:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":2,\"flight_no\":\"PG0404\",\"scheduled_arrival\":\"2017-06-13T17:00:00+00:00\",\"scheduled_departure\":\"2017-06-13T16:05:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-06-13T07:33:00+00:00\",\"actual_departure\":\"2017-06-13T06:38:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":3,\"flight_no\":\"PG0405\",\"scheduled_arrival\":\"2017-06-13T07:30:00+00:00\",\"scheduled_departure\":\"2017-06-13T06:35:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-02-10T10:26:00+00:00\",\"actual_departure\":\"2017-02-10T09:30:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":4,\"flight_no\":\"PG0402\",\"scheduled_arrival\":\"2017-02-10T10:20:00+00:00\",\"scheduled_departure\":\"2017-02-10T09:25:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-02-10T09:22:00+00:00\",\"actual_departure\":\"2017-02-10T08:28:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":5,\"flight_no\":\"PG0403\",\"scheduled_arrival\":\"2017-02-10T09:20:00+00:00\",\"scheduled_departure\":\"2017-02-10T08:25:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2016-12-08T09:25:00+00:00\",\"actual_departure\":\"2016-12-08T08:31:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":6,\"flight_no\":\"PG0403\",\"scheduled_arrival\":\"2016-12-08T09:20:00+00:00\",\"scheduled_departure\":\"2016-12-08T08:25:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-02-10T17:02:00+00:00\",\"actual_departure\":\"2017-02-10T16:07:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":7,\"flight_no\":\"PG0404\",\"scheduled_arrival\":\"2017-02-10T17:00:00+00:00\",\"scheduled_departure\":\"2017-02-10T16:05:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2016-12-08T17:05:00+00:00\",\"actual_departure\":\"2016-12-08T16:10:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":8,\"flight_no\":\"PG0404\",\"scheduled_arrival\":\"2016-12-08T17:00:00+00:00\",\"scheduled_departure\":\"2016-12-08T16:05:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2016-11-26T17:03:00+00:00\",\"actual_departure\":\"2016-11-26T16:09:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":9,\"flight_no\":\"PG0404\",\"scheduled_arrival\":\"2016-11-26T17:00:00+00:00\",\"scheduled_departure\":\"2016-11-26T16:05:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-01-16T17:03:00+00:00\",\"actual_departure\":\"2017-01-16T16:08:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":10,\"flight_no\":\"PG0404\",\"scheduled_arrival\":\"2017-01-16T17:00:00+00:00\",\"scheduled_departure\":\"2017-01-16T16:05:00+00:00\",\"status\":\"Arrived\"}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_flights_df() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let ctx = SessionContext::new();
        let query = format!(
            "select * from {} 
            where actual_departure is not null 
            and actual_arrival is not null",
            table.as_ref()
        );
        let res = worker.query_table_to_df(db.as_ref(), &query, &ctx).await?;

        assert_eq!(res.schema().fields().len(), 10); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

        let rows = res
            .sort(vec![col("flight_id").sort(true, true)])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();
        assert_batches_eq!(
            &[
                "+-----------+-----------+---------------------------+---------------------------+-------------------+-----------------+---------+---------------+---------------------------+---------------------------+",
                "| flight_id | flight_no | scheduled_departure       | scheduled_arrival         | departure_airport | arrival_airport | status  | aircraft_code | actual_departure          | actual_arrival            |",
                "+-----------+-----------+---------------------------+---------------------------+-------------------+-----------------+---------+---------------+---------------------------+---------------------------+",
                "| 1         | PG0403    | 2017-06-13T08:25:00+00:00 | 2017-06-13T09:20:00+00:00 | DME               | LED             | Arrived | 321           | 2017-06-13T08:29:00+00:00 | 2017-06-13T09:24:00+00:00 |",
                "| 2         | PG0404    | 2017-06-13T16:05:00+00:00 | 2017-06-13T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2017-06-13T16:11:00+00:00 | 2017-06-13T17:06:00+00:00 |",
                "| 3         | PG0405    | 2017-06-13T06:35:00+00:00 | 2017-06-13T07:30:00+00:00 | DME               | LED             | Arrived | 321           | 2017-06-13T06:38:00+00:00 | 2017-06-13T07:33:00+00:00 |",
                "| 4         | PG0402    | 2017-02-10T09:25:00+00:00 | 2017-02-10T10:20:00+00:00 | DME               | LED             | Arrived | 321           | 2017-02-10T09:30:00+00:00 | 2017-02-10T10:26:00+00:00 |",
                "| 5         | PG0403    | 2017-02-10T08:25:00+00:00 | 2017-02-10T09:20:00+00:00 | DME               | LED             | Arrived | 321           | 2017-02-10T08:28:00+00:00 | 2017-02-10T09:22:00+00:00 |",
                "| 6         | PG0403    | 2016-12-08T08:25:00+00:00 | 2016-12-08T09:20:00+00:00 | DME               | LED             | Arrived | 321           | 2016-12-08T08:31:00+00:00 | 2016-12-08T09:25:00+00:00 |",
                "| 7         | PG0404    | 2017-02-10T16:05:00+00:00 | 2017-02-10T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2017-02-10T16:07:00+00:00 | 2017-02-10T17:02:00+00:00 |",
                "| 8         | PG0404    | 2016-12-08T16:05:00+00:00 | 2016-12-08T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2016-12-08T16:10:00+00:00 | 2016-12-08T17:05:00+00:00 |",
                "| 9         | PG0404    | 2016-11-26T16:05:00+00:00 | 2016-11-26T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2016-11-26T16:09:00+00:00 | 2016-11-26T17:03:00+00:00 |",
                "| 10        | PG0404    | 2017-01-16T16:05:00+00:00 | 2017-01-16T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2017-01-16T16:08:00+00:00 | 2017-01-16T17:03:00+00:00 |",
                "+-----------+-----------+---------------------------+---------------------------+-------------------+-----------------+---------+---------------+---------------------------+---------------------------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}

mod stat_trait {
    use super::*;

    #[tokio::test]
    async fn test_flights() -> Result<()> {
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
    async fn test_flights_string() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by flight_id", table.as_ref());
        let res = table.run_query_table_to_string(db.as_ref(), &query).await?;
        let expected = vec![
            "flight_id: 1, flight_no: PG0403, scheduled_departure: 2017-06-13 08:25:00 UTC, scheduled_arrival: 2017-06-13 09:20:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-06-13T08:29:00Z), actual_arrival: Some(2017-06-13T09:24:00Z)", 
            "flight_id: 2, flight_no: PG0404, scheduled_departure: 2017-06-13 16:05:00 UTC, scheduled_arrival: 2017-06-13 17:00:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-06-13T16:11:00Z), actual_arrival: Some(2017-06-13T17:06:00Z)", 
            "flight_id: 3, flight_no: PG0405, scheduled_departure: 2017-06-13 06:35:00 UTC, scheduled_arrival: 2017-06-13 07:30:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-06-13T06:38:00Z), actual_arrival: Some(2017-06-13T07:33:00Z)", 
            "flight_id: 4, flight_no: PG0402, scheduled_departure: 2017-02-10 09:25:00 UTC, scheduled_arrival: 2017-02-10 10:20:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-02-10T09:30:00Z), actual_arrival: Some(2017-02-10T10:26:00Z)", 
            "flight_id: 5, flight_no: PG0403, scheduled_departure: 2017-02-10 08:25:00 UTC, scheduled_arrival: 2017-02-10 09:20:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-02-10T08:28:00Z), actual_arrival: Some(2017-02-10T09:22:00Z)", 
            "flight_id: 6, flight_no: PG0403, scheduled_departure: 2016-12-08 08:25:00 UTC, scheduled_arrival: 2016-12-08 09:20:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2016-12-08T08:31:00Z), actual_arrival: Some(2016-12-08T09:25:00Z)", 
            "flight_id: 7, flight_no: PG0404, scheduled_departure: 2017-02-10 16:05:00 UTC, scheduled_arrival: 2017-02-10 17:00:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-02-10T16:07:00Z), actual_arrival: Some(2017-02-10T17:02:00Z)", 
            "flight_id: 8, flight_no: PG0404, scheduled_departure: 2016-12-08 16:05:00 UTC, scheduled_arrival: 2016-12-08 17:00:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2016-12-08T16:10:00Z), actual_arrival: Some(2016-12-08T17:05:00Z)", 
            "flight_id: 9, flight_no: PG0404, scheduled_departure: 2016-11-26 16:05:00 UTC, scheduled_arrival: 2016-11-26 17:00:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2016-11-26T16:09:00Z), actual_arrival: Some(2016-11-26T17:03:00Z)", 
            "flight_id: 10, flight_no: PG0404, scheduled_departure: 2017-01-16 16:05:00 UTC, scheduled_arrival: 2017-01-16 17:00:00 UTC, departure_airport: DME arrival_airport: LED, status: Arrived, aircraft_code: 321, actual_departure: Some(2017-01-16T16:08:00Z), actual_arrival: Some(2017-01-16T17:03:00Z)",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_flights_json() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by flight_id", table.as_ref());
        let res = table.run_query_table_to_json(db.as_ref(), &query).await?;
        let expected = "[
            {\"actual_arrival\":\"2017-06-13T09:24:00+00:00\",\"actual_departure\":\"2017-06-13T08:29:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":1,\"flight_no\":\"PG0403\",\"scheduled_arrival\":\"2017-06-13T09:20:00+00:00\",\"scheduled_departure\":\"2017-06-13T08:25:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-06-13T17:06:00+00:00\",\"actual_departure\":\"2017-06-13T16:11:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":2,\"flight_no\":\"PG0404\",\"scheduled_arrival\":\"2017-06-13T17:00:00+00:00\",\"scheduled_departure\":\"2017-06-13T16:05:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-06-13T07:33:00+00:00\",\"actual_departure\":\"2017-06-13T06:38:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":3,\"flight_no\":\"PG0405\",\"scheduled_arrival\":\"2017-06-13T07:30:00+00:00\",\"scheduled_departure\":\"2017-06-13T06:35:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-02-10T10:26:00+00:00\",\"actual_departure\":\"2017-02-10T09:30:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":4,\"flight_no\":\"PG0402\",\"scheduled_arrival\":\"2017-02-10T10:20:00+00:00\",\"scheduled_departure\":\"2017-02-10T09:25:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-02-10T09:22:00+00:00\",\"actual_departure\":\"2017-02-10T08:28:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":5,\"flight_no\":\"PG0403\",\"scheduled_arrival\":\"2017-02-10T09:20:00+00:00\",\"scheduled_departure\":\"2017-02-10T08:25:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2016-12-08T09:25:00+00:00\",\"actual_departure\":\"2016-12-08T08:31:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":6,\"flight_no\":\"PG0403\",\"scheduled_arrival\":\"2016-12-08T09:20:00+00:00\",\"scheduled_departure\":\"2016-12-08T08:25:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-02-10T17:02:00+00:00\",\"actual_departure\":\"2017-02-10T16:07:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":7,\"flight_no\":\"PG0404\",\"scheduled_arrival\":\"2017-02-10T17:00:00+00:00\",\"scheduled_departure\":\"2017-02-10T16:05:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2016-12-08T17:05:00+00:00\",\"actual_departure\":\"2016-12-08T16:10:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":8,\"flight_no\":\"PG0404\",\"scheduled_arrival\":\"2016-12-08T17:00:00+00:00\",\"scheduled_departure\":\"2016-12-08T16:05:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2016-11-26T17:03:00+00:00\",\"actual_departure\":\"2016-11-26T16:09:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":9,\"flight_no\":\"PG0404\",\"scheduled_arrival\":\"2016-11-26T17:00:00+00:00\",\"scheduled_departure\":\"2016-11-26T16:05:00+00:00\",\"status\":\"Arrived\"},
            {\"actual_arrival\":\"2017-01-16T17:03:00+00:00\",\"actual_departure\":\"2017-01-16T16:08:00+00:00\",\"aircraft_code\":\"321\",\"arrival_airport\":\"LED\",\"departure_airport\":\"DME\",\"flight_id\":10,\"flight_no\":\"PG0404\",\"scheduled_arrival\":\"2017-01-16T17:00:00+00:00\",\"scheduled_departure\":\"2017-01-16T16:05:00+00:00\",\"status\":\"Arrived\"}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_flights_df() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let ctx = SessionContext::new();
        let query = format!(
            "select * from {} 
            where actual_departure is not null 
            and actual_arrival is not null",
            table.as_ref()
        );
        let res = table
            .run_query_table_to_df(db.as_ref(), &query, &ctx)
            .await?;

        assert_eq!(res.schema().fields().len(), 10); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

        let rows = res
            .sort(vec![col("flight_id").sort(true, true)])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();
        assert_batches_eq!(
            &[
                "+-----------+-----------+---------------------------+---------------------------+-------------------+-----------------+---------+---------------+---------------------------+---------------------------+",
                "| flight_id | flight_no | scheduled_departure       | scheduled_arrival         | departure_airport | arrival_airport | status  | aircraft_code | actual_departure          | actual_arrival            |",
                "+-----------+-----------+---------------------------+---------------------------+-------------------+-----------------+---------+---------------+---------------------------+---------------------------+",
                "| 1         | PG0403    | 2017-06-13T08:25:00+00:00 | 2017-06-13T09:20:00+00:00 | DME               | LED             | Arrived | 321           | 2017-06-13T08:29:00+00:00 | 2017-06-13T09:24:00+00:00 |",
                "| 2         | PG0404    | 2017-06-13T16:05:00+00:00 | 2017-06-13T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2017-06-13T16:11:00+00:00 | 2017-06-13T17:06:00+00:00 |",
                "| 3         | PG0405    | 2017-06-13T06:35:00+00:00 | 2017-06-13T07:30:00+00:00 | DME               | LED             | Arrived | 321           | 2017-06-13T06:38:00+00:00 | 2017-06-13T07:33:00+00:00 |",
                "| 4         | PG0402    | 2017-02-10T09:25:00+00:00 | 2017-02-10T10:20:00+00:00 | DME               | LED             | Arrived | 321           | 2017-02-10T09:30:00+00:00 | 2017-02-10T10:26:00+00:00 |",
                "| 5         | PG0403    | 2017-02-10T08:25:00+00:00 | 2017-02-10T09:20:00+00:00 | DME               | LED             | Arrived | 321           | 2017-02-10T08:28:00+00:00 | 2017-02-10T09:22:00+00:00 |",
                "| 6         | PG0403    | 2016-12-08T08:25:00+00:00 | 2016-12-08T09:20:00+00:00 | DME               | LED             | Arrived | 321           | 2016-12-08T08:31:00+00:00 | 2016-12-08T09:25:00+00:00 |",
                "| 7         | PG0404    | 2017-02-10T16:05:00+00:00 | 2017-02-10T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2017-02-10T16:07:00+00:00 | 2017-02-10T17:02:00+00:00 |",
                "| 8         | PG0404    | 2016-12-08T16:05:00+00:00 | 2016-12-08T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2016-12-08T16:10:00+00:00 | 2016-12-08T17:05:00+00:00 |",
                "| 9         | PG0404    | 2016-11-26T16:05:00+00:00 | 2016-11-26T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2016-11-26T16:09:00+00:00 | 2016-11-26T17:03:00+00:00 |",
                "| 10        | PG0404    | 2017-01-16T16:05:00+00:00 | 2017-01-16T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2017-01-16T16:08:00+00:00 | 2017-01-16T17:03:00+00:00 |",
                "+-----------+-----------+---------------------------+---------------------------+-------------------+-----------------+---------+---------------+---------------------------+---------------------------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}
