use crate::helpers::TestApp;
use demodb_to_datalake::{Table, DATABASE_URL};

use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

#[tokio::test]
async fn test_airports_data() {
    let app = TestApp::new(DATABASE_URL.expose_secret(), Table::AirportsDataTable).await.unwrap();
    let ctx = SessionContext::new();
    let res = app.test_airports_data(&ctx).await.unwrap();

    assert_eq!(res.schema().fields().len(), 5); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 104); // rows count

    let rows = res.sort(vec![col("airport_code").sort(true, true)]).unwrap().limit(0, Some(10)).unwrap();
    assert_batches_eq!(
        &[
              "+--------------+--------------------------------------------------+---------------------------------------------+------------------------------------------------+------------------+",
              "| airport_code | airport_name                                     | city                                        | coordinates                                    | timezone         |",
              "+--------------+--------------------------------------------------+---------------------------------------------+------------------------------------------------+------------------+",
            r#"| AAQ          | {"en":"Anapa Vityazevo Airport","ru":"Витязево"} | {"en":"Anapa","ru":"Анапа"}                 | {"x":37.347301483154,"y":45.002101898193}      | Europe/Moscow    |"#,
            r#"| ABA          | {"en":"Abakan Airport","ru":"Абакан"}            | {"en":"Abakan","ru":"Абакан"}               | {"x":91.38500213623048,"y":53.7400016784668}   | Asia/Krasnoyarsk |"#,
            r#"| AER          | {"en":"Sochi International Airport","ru":"Сочи"} | {"en":"Sochi","ru":"Сочи"}                  | {"x":39.956600189209,"y":43.449901580811}      | Europe/Moscow    |"#,
            r#"| ARH          | {"en":"Talagi Airport","ru":"Талаги"}            | {"en":"Arkhangelsk","ru":"Архангельск"}     | {"x":40.71670150756836,"y":64.60030364990234}  | Europe/Moscow    |"#,
            r#"| ASF          | {"en":"Astrakhan Airport","ru":"Астрахань"}      | {"en":"Astrakhan","ru":"Астрахань"}         | {"x":48.0063018799,"y":46.2832984924}          | Europe/Samara    |"#,
            r#"| BAX          | {"en":"Barnaul Airport","ru":"Барнаул"}          | {"en":"Barnaul","ru":"Барнаул"}             | {"x":83.53849792480469,"y":53.363800048828125} | Asia/Krasnoyarsk |"#,
            r#"| BQS          | {"en":"Ignatyevo Airport","ru":"Игнатьево"}      | {"en":"Blagoveschensk","ru":"Благовещенск"} | {"x":127.41200256347656,"y":50.42539978027344} | Asia/Yakutsk     |"#,
            r#"| BTK          | {"en":"Bratsk Airport","ru":"Братск"}            | {"en":"Bratsk","ru":"Братск"}               | {"x":101.697998046875,"y":56.370601654052734}  | Asia/Irkutsk     |"#,
            r#"| BZK          | {"en":"Bryansk Airport","ru":"Брянск"}           | {"en":"Bryansk","ru":"Брянск"}              | {"x":34.176399231,"y":53.2141990662}           | Europe/Moscow    |"#,
            r#"| CEE          | {"en":"Cherepovets Airport","ru":"Череповец"}    | {"en":"Cherepovets","ru":"Череповец"}       | {"x":38.0158004761,"y":59.273601532}           | Europe/Moscow    |"#,
              "+--------------+--------------------------------------------------+---------------------------------------------+------------------------------------------------+------------------+",
        ],
        &rows.collect().await.unwrap()
    );
}