use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};

use color_eyre::Result;
use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

#[tokio::test]
async fn test_flights_dyn_df() -> Result<()> {
    let db = PostgresDb::builder()
      .with_url(DATABASE_URL.expose_secret())
      .with_max_cons(MAX_DB_CONS)
      .build()
      .await?;
    let table = Table::TicketsTable;
    let worker = table.to_worker();
    let ctx = SessionContext::new();
    let query = format!("select * from {} order by ticket_no", table.as_ref());
    let res = worker.query_table_to_df(db.as_ref(), &query, &ctx).await?;

    assert_eq!(res.schema().fields().len(), 5); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

    let rows = res.sort(vec![col("ticket_no").sort(true, true)]).unwrap().limit(0, Some(10)).unwrap();
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