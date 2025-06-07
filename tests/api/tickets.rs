use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};

use color_eyre::Result;
use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

const TABLE: Table = Table::TicketsTable;

mod dyn_stat {
    use super::*;

    #[tokio::test]
    async fn test_tickets() -> Result<()> {
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
    async fn test_tickets_string() -> Result<()> {
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
          "ticket_no: 0005432000284, book_ref: 1A40A1, passenger_id: 4030 855525, passenger_name: MIKHAIL SEMENOV, contact_data: {\"phone\":\"+70110137563\"}", 
          "ticket_no: 0005432000285, book_ref: 13736D, passenger_id: 8360 311602, passenger_name: ELENA ZAKHAROVA, contact_data: {\"phone\":\"+70670013989\"}", 
          "ticket_no: 0005432000286, book_ref: DC89BC, passenger_id: 4510 377533, passenger_name: ILYA PAVLOV, contact_data: {\"phone\":\"+70624013335\"}", 
          "ticket_no: 0005432000287, book_ref: CDE08B, passenger_id: 5952 253588, passenger_name: ELENA BELOVA, contact_data: {\"email\":\"e.belova.07121974@postgrespro.ru\",\"phone\":\"+70340423946\"}", 
          "ticket_no: 0005432000288, book_ref: BEFB90, passenger_id: 4313 788533, passenger_name: VYACHESLAV IVANOV, contact_data: {\"email\":\"vyacheslav-ivanov051968@postgrespro.ru\",\"phone\":\"+70417078841\"}", 
          "ticket_no: 0005432000289, book_ref: A903E4, passenger_id: 2742 028983, passenger_name: NATALIYA NESTEROVA, contact_data: {\"phone\":\"+70031478265\"}", 
          "ticket_no: 0005432000290, book_ref: CC77B6, passenger_id: 9873 744760, passenger_name: ALEKSANDRA ARKHIPOVA, contact_data: {\"email\":\"arkhipovaa-1980@postgrespro.ru\",\"phone\":\"+70185914840\"}", 
          "ticket_no: 0005432000291, book_ref: D530F6, passenger_id: 2695 977692, passenger_name: EVGENIY SERGEEV, contact_data: {\"phone\":\"+70007395677\"}", 
          "ticket_no: 0005432000292, book_ref: F26006, passenger_id: 2512 253082, passenger_name: TATYANA ZHUKOVA, contact_data: {\"email\":\"zhukova_tatyana_121964@postgrespro.ru\",\"phone\":\"+70505293692\"}", 
          "ticket_no: 0005432000293, book_ref: 739B4E, passenger_id: 5763 638275, passenger_name: ILYA KRASNOV, contact_data: {\"email\":\"ilya_krasnov_081985@postgrespro.ru\",\"phone\":\"+70669365996\"}",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_tickets_json() -> Result<()> {
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
          {\"ticket_no\":\"0005432000284\",\"book_ref\":\"1A40A1\",\"passenger_id\":\"4030 855525\",\"passenger_name\":\"MIKHAIL SEMENOV\",\"contact_data\":{\"email\":null,\"phone\":\"+70110137563\"}},
          {\"ticket_no\":\"0005432000285\",\"book_ref\":\"13736D\",\"passenger_id\":\"8360 311602\",\"passenger_name\":\"ELENA ZAKHAROVA\",\"contact_data\":{\"email\":null,\"phone\":\"+70670013989\"}},
          {\"ticket_no\":\"0005432000286\",\"book_ref\":\"DC89BC\",\"passenger_id\":\"4510 377533\",\"passenger_name\":\"ILYA PAVLOV\",\"contact_data\":{\"email\":null,\"phone\":\"+70624013335\"}},
          {\"ticket_no\":\"0005432000287\",\"book_ref\":\"CDE08B\",\"passenger_id\":\"5952 253588\",\"passenger_name\":\"ELENA BELOVA\",\"contact_data\":{\"email\":\"e.belova.07121974@postgrespro.ru\",\"phone\":\"+70340423946\"}},
          {\"ticket_no\":\"0005432000288\",\"book_ref\":\"BEFB90\",\"passenger_id\":\"4313 788533\",\"passenger_name\":\"VYACHESLAV IVANOV\",\"contact_data\":{\"email\":\"vyacheslav-ivanov051968@postgrespro.ru\",\"phone\":\"+70417078841\"}},
          {\"ticket_no\":\"0005432000289\",\"book_ref\":\"A903E4\",\"passenger_id\":\"2742 028983\",\"passenger_name\":\"NATALIYA NESTEROVA\",\"contact_data\":{\"email\":null,\"phone\":\"+70031478265\"}},
          {\"ticket_no\":\"0005432000290\",\"book_ref\":\"CC77B6\",\"passenger_id\":\"9873 744760\",\"passenger_name\":\"ALEKSANDRA ARKHIPOVA\",\"contact_data\":{\"email\":\"arkhipovaa-1980@postgrespro.ru\",\"phone\":\"+70185914840\"}},
          {\"ticket_no\":\"0005432000291\",\"book_ref\":\"D530F6\",\"passenger_id\":\"2695 977692\",\"passenger_name\":\"EVGENIY SERGEEV\",\"contact_data\":{\"email\":null,\"phone\":\"+70007395677\"}},
          {\"ticket_no\":\"0005432000292\",\"book_ref\":\"F26006\",\"passenger_id\":\"2512 253082\",\"passenger_name\":\"TATYANA ZHUKOVA\",\"contact_data\":{\"email\":\"zhukova_tatyana_121964@postgrespro.ru\",\"phone\":\"+70505293692\"}},
          {\"ticket_no\":\"0005432000293\",\"book_ref\":\"739B4E\",\"passenger_id\":\"5763 638275\",\"passenger_name\":\"ILYA KRASNOV\",\"contact_data\":{\"email\":\"ilya_krasnov_081985@postgrespro.ru\",\"phone\":\"+70669365996\"}}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_tickets_df() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let worker = table.to_worker();
        let ctx = SessionContext::new();
        let query = format!("select * from {} order by ticket_no", table.as_ref());
        let res = worker.query_table_to_df(db.as_ref(), &query, &ctx).await?;

        assert_eq!(res.schema().fields().len(), 5); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

        let rows = res
            .sort(vec![col("ticket_no").sort(true, true)])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();
        assert_batches_eq!(
            &[
                  "+---------------+----------+--------------+----------------------+---------------------------------------------------------------------------+",
                  "| ticket_no     | book_ref | passenger_id | passenger_name       | contact_data                                                              |",
                  "+---------------+----------+--------------+----------------------+---------------------------------------------------------------------------+",
                r#"| 0005432000284 | 1A40A1   | 4030 855525  | MIKHAIL SEMENOV      | {"email":null,"phone":"+70110137563"}                                     |"#,
                r#"| 0005432000285 | 13736D   | 8360 311602  | ELENA ZAKHAROVA      | {"email":null,"phone":"+70670013989"}                                     |"#,
                r#"| 0005432000286 | DC89BC   | 4510 377533  | ILYA PAVLOV          | {"email":null,"phone":"+70624013335"}                                     |"#,
                r#"| 0005432000287 | CDE08B   | 5952 253588  | ELENA BELOVA         | {"email":"e.belova.07121974@postgrespro.ru","phone":"+70340423946"}       |"#,
                r#"| 0005432000288 | BEFB90   | 4313 788533  | VYACHESLAV IVANOV    | {"email":"vyacheslav-ivanov051968@postgrespro.ru","phone":"+70417078841"} |"#,
                r#"| 0005432000289 | A903E4   | 2742 028983  | NATALIYA NESTEROVA   | {"email":null,"phone":"+70031478265"}                                     |"#,
                r#"| 0005432000290 | CC77B6   | 9873 744760  | ALEKSANDRA ARKHIPOVA | {"email":"arkhipovaa-1980@postgrespro.ru","phone":"+70185914840"}         |"#,
                r#"| 0005432000291 | D530F6   | 2695 977692  | EVGENIY SERGEEV      | {"email":null,"phone":"+70007395677"}                                     |"#,
                r#"| 0005432000292 | F26006   | 2512 253082  | TATYANA ZHUKOVA      | {"email":"zhukova_tatyana_121964@postgrespro.ru","phone":"+70505293692"}  |"#,
                r#"| 0005432000293 | 739B4E   | 5763 638275  | ILYA KRASNOV         | {"email":"ilya_krasnov_081985@postgrespro.ru","phone":"+70669365996"}     |"#,
                  "+---------------+----------+--------------+----------------------+---------------------------------------------------------------------------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}

mod stat_trait {
    use super::*;

    #[tokio::test]
    async fn test_tickets() -> Result<()> {
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
    async fn test_tickets_string() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by ticket_no", table.as_ref());
        let res = table.run_query_table_to_string(db.as_ref(), &query).await?;
        let expected = vec![
          "ticket_no: 0005432000284, book_ref: 1A40A1, passenger_id: 4030 855525, passenger_name: MIKHAIL SEMENOV, contact_data: {\"phone\":\"+70110137563\"}", 
          "ticket_no: 0005432000285, book_ref: 13736D, passenger_id: 8360 311602, passenger_name: ELENA ZAKHAROVA, contact_data: {\"phone\":\"+70670013989\"}", 
          "ticket_no: 0005432000286, book_ref: DC89BC, passenger_id: 4510 377533, passenger_name: ILYA PAVLOV, contact_data: {\"phone\":\"+70624013335\"}", 
          "ticket_no: 0005432000287, book_ref: CDE08B, passenger_id: 5952 253588, passenger_name: ELENA BELOVA, contact_data: {\"email\":\"e.belova.07121974@postgrespro.ru\",\"phone\":\"+70340423946\"}", 
          "ticket_no: 0005432000288, book_ref: BEFB90, passenger_id: 4313 788533, passenger_name: VYACHESLAV IVANOV, contact_data: {\"email\":\"vyacheslav-ivanov051968@postgrespro.ru\",\"phone\":\"+70417078841\"}", 
          "ticket_no: 0005432000289, book_ref: A903E4, passenger_id: 2742 028983, passenger_name: NATALIYA NESTEROVA, contact_data: {\"phone\":\"+70031478265\"}", 
          "ticket_no: 0005432000290, book_ref: CC77B6, passenger_id: 9873 744760, passenger_name: ALEKSANDRA ARKHIPOVA, contact_data: {\"email\":\"arkhipovaa-1980@postgrespro.ru\",\"phone\":\"+70185914840\"}", 
          "ticket_no: 0005432000291, book_ref: D530F6, passenger_id: 2695 977692, passenger_name: EVGENIY SERGEEV, contact_data: {\"phone\":\"+70007395677\"}", 
          "ticket_no: 0005432000292, book_ref: F26006, passenger_id: 2512 253082, passenger_name: TATYANA ZHUKOVA, contact_data: {\"email\":\"zhukova_tatyana_121964@postgrespro.ru\",\"phone\":\"+70505293692\"}", 
          "ticket_no: 0005432000293, book_ref: 739B4E, passenger_id: 5763 638275, passenger_name: ILYA KRASNOV, contact_data: {\"email\":\"ilya_krasnov_081985@postgrespro.ru\",\"phone\":\"+70669365996\"}",
        ];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_tickets_json() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let query = format!("select * from {} order by ticket_no", table.as_ref());
        let res = table.run_query_table_to_json(db.as_ref(), &query).await?;
        let expected = "[
          {\"ticket_no\":\"0005432000284\",\"book_ref\":\"1A40A1\",\"passenger_id\":\"4030 855525\",\"passenger_name\":\"MIKHAIL SEMENOV\",\"contact_data\":{\"email\":null,\"phone\":\"+70110137563\"}},
          {\"ticket_no\":\"0005432000285\",\"book_ref\":\"13736D\",\"passenger_id\":\"8360 311602\",\"passenger_name\":\"ELENA ZAKHAROVA\",\"contact_data\":{\"email\":null,\"phone\":\"+70670013989\"}},
          {\"ticket_no\":\"0005432000286\",\"book_ref\":\"DC89BC\",\"passenger_id\":\"4510 377533\",\"passenger_name\":\"ILYA PAVLOV\",\"contact_data\":{\"email\":null,\"phone\":\"+70624013335\"}},
          {\"ticket_no\":\"0005432000287\",\"book_ref\":\"CDE08B\",\"passenger_id\":\"5952 253588\",\"passenger_name\":\"ELENA BELOVA\",\"contact_data\":{\"email\":\"e.belova.07121974@postgrespro.ru\",\"phone\":\"+70340423946\"}},
          {\"ticket_no\":\"0005432000288\",\"book_ref\":\"BEFB90\",\"passenger_id\":\"4313 788533\",\"passenger_name\":\"VYACHESLAV IVANOV\",\"contact_data\":{\"email\":\"vyacheslav-ivanov051968@postgrespro.ru\",\"phone\":\"+70417078841\"}},
          {\"ticket_no\":\"0005432000289\",\"book_ref\":\"A903E4\",\"passenger_id\":\"2742 028983\",\"passenger_name\":\"NATALIYA NESTEROVA\",\"contact_data\":{\"email\":null,\"phone\":\"+70031478265\"}},
          {\"ticket_no\":\"0005432000290\",\"book_ref\":\"CC77B6\",\"passenger_id\":\"9873 744760\",\"passenger_name\":\"ALEKSANDRA ARKHIPOVA\",\"contact_data\":{\"email\":\"arkhipovaa-1980@postgrespro.ru\",\"phone\":\"+70185914840\"}},
          {\"ticket_no\":\"0005432000291\",\"book_ref\":\"D530F6\",\"passenger_id\":\"2695 977692\",\"passenger_name\":\"EVGENIY SERGEEV\",\"contact_data\":{\"email\":null,\"phone\":\"+70007395677\"}},
          {\"ticket_no\":\"0005432000292\",\"book_ref\":\"F26006\",\"passenger_id\":\"2512 253082\",\"passenger_name\":\"TATYANA ZHUKOVA\",\"contact_data\":{\"email\":\"zhukova_tatyana_121964@postgrespro.ru\",\"phone\":\"+70505293692\"}},
          {\"ticket_no\":\"0005432000293\",\"book_ref\":\"739B4E\",\"passenger_id\":\"5763 638275\",\"passenger_name\":\"ILYA KRASNOV\",\"contact_data\":{\"email\":\"ilya_krasnov_081985@postgrespro.ru\",\"phone\":\"+70669365996\"}}
        ]";
        let res_json: serde_json::Value = serde_json::from_str(&res)?;
        let expected_json: serde_json::Value = serde_json::from_str(expected)?;
        assert_eq!(res_json, expected_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_tickets_df() -> Result<()> {
        let db = PostgresDb::builder()
            .with_url(DATABASE_URL.expose_secret())
            .with_max_cons(MAX_DB_CONS)
            .build()
            .await?;
        let table = TABLE;
        let ctx = SessionContext::new();
        let query = format!("select * from {} order by ticket_no", table.as_ref());
        let res = table
            .run_query_table_to_df(db.as_ref(), &query, &ctx)
            .await?;

        assert_eq!(res.schema().fields().len(), 5); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

        let rows = res
            .sort(vec![col("ticket_no").sort(true, true)])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();
        assert_batches_eq!(
            &[
                  "+---------------+----------+--------------+----------------------+---------------------------------------------------------------------------+",
                  "| ticket_no     | book_ref | passenger_id | passenger_name       | contact_data                                                              |",
                  "+---------------+----------+--------------+----------------------+---------------------------------------------------------------------------+",
                r#"| 0005432000284 | 1A40A1   | 4030 855525  | MIKHAIL SEMENOV      | {"email":null,"phone":"+70110137563"}                                     |"#,
                r#"| 0005432000285 | 13736D   | 8360 311602  | ELENA ZAKHAROVA      | {"email":null,"phone":"+70670013989"}                                     |"#,
                r#"| 0005432000286 | DC89BC   | 4510 377533  | ILYA PAVLOV          | {"email":null,"phone":"+70624013335"}                                     |"#,
                r#"| 0005432000287 | CDE08B   | 5952 253588  | ELENA BELOVA         | {"email":"e.belova.07121974@postgrespro.ru","phone":"+70340423946"}       |"#,
                r#"| 0005432000288 | BEFB90   | 4313 788533  | VYACHESLAV IVANOV    | {"email":"vyacheslav-ivanov051968@postgrespro.ru","phone":"+70417078841"} |"#,
                r#"| 0005432000289 | A903E4   | 2742 028983  | NATALIYA NESTEROVA   | {"email":null,"phone":"+70031478265"}                                     |"#,
                r#"| 0005432000290 | CC77B6   | 9873 744760  | ALEKSANDRA ARKHIPOVA | {"email":"arkhipovaa-1980@postgrespro.ru","phone":"+70185914840"}         |"#,
                r#"| 0005432000291 | D530F6   | 2695 977692  | EVGENIY SERGEEV      | {"email":null,"phone":"+70007395677"}                                     |"#,
                r#"| 0005432000292 | F26006   | 2512 253082  | TATYANA ZHUKOVA      | {"email":"zhukova_tatyana_121964@postgrespro.ru","phone":"+70505293692"}  |"#,
                r#"| 0005432000293 | 739B4E   | 5763 638275  | ILYA KRASNOV         | {"email":"ilya_krasnov_081985@postgrespro.ru","phone":"+70669365996"}     |"#,
                  "+---------------+----------+--------------+----------------------+---------------------------------------------------------------------------+",
            ],
            &rows.collect().await.unwrap()
        );
        Ok(())
    }
}
