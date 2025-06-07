use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};

use color_eyre::Result;
use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

const TABLE: Table = Table::BoardingPassesTable;

mod dyn_trait {
    use super::*;

    #[tokio::test]
    async fn test_boarding_passes() -> Result<()> {
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
    async fn test_boarding_passes_string() -> Result<()> {
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
            "ticket_no: 0005432000284, flight_id: 187662, boarding_no: 16, seat_no: 10C",
            "ticket_no: 0005432000285, flight_id: 187570, boarding_no: 4, seat_no: 2A",
            "ticket_no: 0005432000286, flight_id: 187570, boarding_no: 19, seat_no: 10E",
            "ticket_no: 0005432000287, flight_id: 187728, boarding_no: 10, seat_no: 18A",
            "ticket_no: 0005432000288, flight_id: 187728, boarding_no: 17, seat_no: 2A",
            "ticket_no: 0005432000289, flight_id: 187754, boarding_no: 23, seat_no: 8A",
            "ticket_no: 0005432000290, flight_id: 187754, boarding_no: 1, seat_no: 17A",
            "ticket_no: 0005432000291, flight_id: 187506, boarding_no: 2, seat_no: 3D",
            "ticket_no: 0005432000292, flight_id: 187506, boarding_no: 4, seat_no: 8C",
            "ticket_no: 0005432000293, flight_id: 187625, boarding_no: 20, seat_no: 14A",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_boarding_passes_json() -> Result<()> {
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
            {\"ticket_no\":\"0005432000284\",\"flight_id\":187662,\"boarding_no\":16,\"seat_no\":\"10C\"},
            {\"ticket_no\":\"0005432000285\",\"flight_id\":187570,\"boarding_no\":4,\"seat_no\":\"2A\"},
            {\"ticket_no\":\"0005432000286\",\"flight_id\":187570,\"boarding_no\":19,\"seat_no\":\"10E\"},
            {\"ticket_no\":\"0005432000287\",\"flight_id\":187728,\"boarding_no\":10,\"seat_no\":\"18A\"},
            {\"ticket_no\":\"0005432000288\",\"flight_id\":187728,\"boarding_no\":17,\"seat_no\":\"2A\"},
            {\"ticket_no\":\"0005432000289\",\"flight_id\":187754,\"boarding_no\":23,\"seat_no\":\"8A\"},
            {\"ticket_no\":\"0005432000290\",\"flight_id\":187754,\"boarding_no\":1,\"seat_no\":\"17A\"},
            {\"ticket_no\":\"0005432000291\",\"flight_id\":187506,\"boarding_no\":2,\"seat_no\":\"3D\"},
            {\"ticket_no\":\"0005432000292\",\"flight_id\":187506,\"boarding_no\":4,\"seat_no\":\"8C\"},
            {\"ticket_no\":\"0005432000293\",\"flight_id\":187625,\"boarding_no\":20,\"seat_no\":\"14A\"}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_boarding_passes_df() -> Result<()> {
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
                "+---------------+-----------+-------------+---------+",
                "| ticket_no     | flight_id | boarding_no | seat_no |",
                "+---------------+-----------+-------------+---------+",
                "| 0005432208788 | 198393    | 5           | 28C     |",
                "| 0005433655456 | 198393    | 7           | 31J     |",
                "| 0005435189093 | 198393    | 1           | 27G     |",
                "| 0005435189096 | 198393    | 3           | 18E     |",
                "| 0005435189100 | 198393    | 10          | 30F     |",
                "| 0005435189117 | 198393    | 4           | 31B     |",
                "| 0005435189119 | 198393    | 2           | 2D      |",
                "| 0005435189129 | 198393    | 8           | 30C     |",
                "| 0005435189151 | 198393    | 6           | 32A     |",
                "| 0005435629876 | 198393    | 9           | 30E     |",
                "+---------------+-----------+-------------+---------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}

mod stat_trait {
    use super::*;

    #[tokio::test]
    async fn test_boarding_passes() -> Result<()> {
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
    async fn test_boarding_passes_string() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by ticket_no", table.as_ref());
        let res = table.run_query_table_to_string(db.as_ref(), &query).await?;
        let expected = vec![
            "ticket_no: 0005432000284, flight_id: 187662, boarding_no: 16, seat_no: 10C",
            "ticket_no: 0005432000285, flight_id: 187570, boarding_no: 4, seat_no: 2A",
            "ticket_no: 0005432000286, flight_id: 187570, boarding_no: 19, seat_no: 10E",
            "ticket_no: 0005432000287, flight_id: 187728, boarding_no: 10, seat_no: 18A",
            "ticket_no: 0005432000288, flight_id: 187728, boarding_no: 17, seat_no: 2A",
            "ticket_no: 0005432000289, flight_id: 187754, boarding_no: 23, seat_no: 8A",
            "ticket_no: 0005432000290, flight_id: 187754, boarding_no: 1, seat_no: 17A",
            "ticket_no: 0005432000291, flight_id: 187506, boarding_no: 2, seat_no: 3D",
            "ticket_no: 0005432000292, flight_id: 187506, boarding_no: 4, seat_no: 8C",
            "ticket_no: 0005432000293, flight_id: 187625, boarding_no: 20, seat_no: 14A",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_boarding_passes_json() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by ticket_no", table.as_ref());
        let res = table.run_query_table_to_json(db.as_ref(), &query).await?;
        let expected = "[
            {\"ticket_no\":\"0005432000284\",\"flight_id\":187662,\"boarding_no\":16,\"seat_no\":\"10C\"},
            {\"ticket_no\":\"0005432000285\",\"flight_id\":187570,\"boarding_no\":4,\"seat_no\":\"2A\"},
            {\"ticket_no\":\"0005432000286\",\"flight_id\":187570,\"boarding_no\":19,\"seat_no\":\"10E\"},
            {\"ticket_no\":\"0005432000287\",\"flight_id\":187728,\"boarding_no\":10,\"seat_no\":\"18A\"},
            {\"ticket_no\":\"0005432000288\",\"flight_id\":187728,\"boarding_no\":17,\"seat_no\":\"2A\"},
            {\"ticket_no\":\"0005432000289\",\"flight_id\":187754,\"boarding_no\":23,\"seat_no\":\"8A\"},
            {\"ticket_no\":\"0005432000290\",\"flight_id\":187754,\"boarding_no\":1,\"seat_no\":\"17A\"},
            {\"ticket_no\":\"0005432000291\",\"flight_id\":187506,\"boarding_no\":2,\"seat_no\":\"3D\"},
            {\"ticket_no\":\"0005432000292\",\"flight_id\":187506,\"boarding_no\":4,\"seat_no\":\"8C\"},
            {\"ticket_no\":\"0005432000293\",\"flight_id\":187625,\"boarding_no\":20,\"seat_no\":\"14A\"}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_boarding_passes_df() -> Result<()> {
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
                "+---------------+-----------+-------------+---------+",
                "| ticket_no     | flight_id | boarding_no | seat_no |",
                "+---------------+-----------+-------------+---------+",
                "| 0005432208788 | 198393    | 5           | 28C     |",
                "| 0005433655456 | 198393    | 7           | 31J     |",
                "| 0005435189093 | 198393    | 1           | 27G     |",
                "| 0005435189096 | 198393    | 3           | 18E     |",
                "| 0005435189100 | 198393    | 10          | 30F     |",
                "| 0005435189117 | 198393    | 4           | 31B     |",
                "| 0005435189119 | 198393    | 2           | 2D      |",
                "| 0005435189129 | 198393    | 8           | 30C     |",
                "| 0005435189151 | 198393    | 6           | 32A     |",
                "| 0005435629876 | 198393    | 9           | 30E     |",
                "+---------------+-----------+-------------+---------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}
