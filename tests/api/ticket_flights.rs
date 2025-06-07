use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};

use color_eyre::Result;
use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

const TABLE: Table = Table::TicketFlightsTable;

mod dyn_trait {
    use super::*;

    #[tokio::test]
    async fn test_ticket_flights() -> Result<()> {
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
    async fn test_ticket_flights_string() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let query = format!("select * from {} order by ticket_no", table.as_ref());
        let res = worker.query_table_to_string(db.as_ref(), &query).await?;
        let expected = vec![
            "ticket_no: 0005432000284, flight_id: 187662, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000285, flight_id: 187570, fare_conditions: Business, amount: 18500.00", 
            "ticket_no: 0005432000286, flight_id: 187570, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000287, flight_id: 187728, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000288, flight_id: 187728, fare_conditions: Business, amount: 18500.00", 
            "ticket_no: 0005432000289, flight_id: 187754, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000290, flight_id: 187754, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000291, flight_id: 187506, fare_conditions: Business, amount: 18500.00", 
            "ticket_no: 0005432000292, flight_id: 187506, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000293, flight_id: 187625, fare_conditions: Economy, amount: 6200.00",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_ticket_flights_json() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let query = format!("select * from {} order by ticket_no", table.as_ref());
        let res = worker.query_table_to_json(db.as_ref(), &query).await?;
        let expected = "[
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187662,\"ticket_no\":\"0005432000284\"},
            {\"amount\":\"18500.00\",\"fare_conditions\":\"Business\",\"flight_id\":187570,\"ticket_no\":\"0005432000285\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187570,\"ticket_no\":\"0005432000286\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187728,\"ticket_no\":\"0005432000287\"},
            {\"amount\":\"18500.00\",\"fare_conditions\":\"Business\",\"flight_id\":187728,\"ticket_no\":\"0005432000288\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187754,\"ticket_no\":\"0005432000289\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187754,\"ticket_no\":\"0005432000290\"},
            {\"amount\":\"18500.00\",\"fare_conditions\":\"Business\",\"flight_id\":187506,\"ticket_no\":\"0005432000291\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187506,\"ticket_no\":\"0005432000292\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187625,\"ticket_no\":\"0005432000293\"}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_ticket_flights_df() -> Result<()> {
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

        assert_eq!(res.schema().fields().len(), 4); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

        let rows = res
            .sort(vec![col("ticket_no").sort(true, true)])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();
        assert_batches_eq!(
            &[
                "+---------------+-----------+-----------------+-----------+",
                "| ticket_no     | flight_id | fare_conditions | amount    |",
                "+---------------+-----------+-----------------+-----------+",
                "| 0005432003235 | 89752     | Business        | 99800.00  |",
                "| 0005432003470 | 89913     | Business        | 99800.00  |",
                "| 0005432003656 | 90106     | Business        | 99800.00  |",
                "| 0005432079221 | 36094     | Business        | 99800.00  |",
                "| 0005432801137 | 9563      | Business        | 150400.00 |",
                "| 0005432949087 | 164161    | Business        | 105900.00 |",
                "| 0005433557112 | 164098    | Business        | 105900.00 |",
                "| 0005433567794 | 164215    | Business        | 105900.00 |",
                "| 0005434861552 | 65405     | Business        | 49700.00  |",
                "| 0005435834642 | 117026    | Business        | 199300.00 |",
                "+---------------+-----------+-----------------+-----------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}

mod stat_trait {
    use super::*;

    #[tokio::test]
    async fn test_ticket_flights() -> Result<()> {
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
    async fn test_ticket_flights_string() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by ticket_no", table.as_ref());
        let res = table.run_query_table_to_string(db.as_ref(), &query).await?;
        let expected = vec![
            "ticket_no: 0005432000284, flight_id: 187662, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000285, flight_id: 187570, fare_conditions: Business, amount: 18500.00", 
            "ticket_no: 0005432000286, flight_id: 187570, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000287, flight_id: 187728, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000288, flight_id: 187728, fare_conditions: Business, amount: 18500.00", 
            "ticket_no: 0005432000289, flight_id: 187754, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000290, flight_id: 187754, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000291, flight_id: 187506, fare_conditions: Business, amount: 18500.00", 
            "ticket_no: 0005432000292, flight_id: 187506, fare_conditions: Economy, amount: 6200.00", 
            "ticket_no: 0005432000293, flight_id: 187625, fare_conditions: Economy, amount: 6200.00",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_ticket_flights_json() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by ticket_no", table.as_ref());
        let res = table.run_query_table_to_json(db.as_ref(), &query).await?;
        let expected = "[
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187662,\"ticket_no\":\"0005432000284\"},
            {\"amount\":\"18500.00\",\"fare_conditions\":\"Business\",\"flight_id\":187570,\"ticket_no\":\"0005432000285\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187570,\"ticket_no\":\"0005432000286\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187728,\"ticket_no\":\"0005432000287\"},
            {\"amount\":\"18500.00\",\"fare_conditions\":\"Business\",\"flight_id\":187728,\"ticket_no\":\"0005432000288\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187754,\"ticket_no\":\"0005432000289\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187754,\"ticket_no\":\"0005432000290\"},
            {\"amount\":\"18500.00\",\"fare_conditions\":\"Business\",\"flight_id\":187506,\"ticket_no\":\"0005432000291\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187506,\"ticket_no\":\"0005432000292\"},
            {\"amount\":\"6200.00\",\"fare_conditions\":\"Economy\",\"flight_id\":187625,\"ticket_no\":\"0005432000293\"}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_ticket_flights_df() -> Result<()> {
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

        assert_eq!(res.schema().fields().len(), 4); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

        let rows = res
            .sort(vec![col("ticket_no").sort(true, true)])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();
        assert_batches_eq!(
            &[
                "+---------------+-----------+-----------------+-----------+",
                "| ticket_no     | flight_id | fare_conditions | amount    |",
                "+---------------+-----------+-----------------+-----------+",
                "| 0005432003235 | 89752     | Business        | 99800.00  |",
                "| 0005432003470 | 89913     | Business        | 99800.00  |",
                "| 0005432003656 | 90106     | Business        | 99800.00  |",
                "| 0005432079221 | 36094     | Business        | 99800.00  |",
                "| 0005432801137 | 9563      | Business        | 150400.00 |",
                "| 0005432949087 | 164161    | Business        | 105900.00 |",
                "| 0005433557112 | 164098    | Business        | 105900.00 |",
                "| 0005433567794 | 164215    | Business        | 105900.00 |",
                "| 0005434861552 | 65405     | Business        | 49700.00  |",
                "| 0005435834642 | 117026    | Business        | 199300.00 |",
                "+---------------+-----------+-----------------+-----------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}
