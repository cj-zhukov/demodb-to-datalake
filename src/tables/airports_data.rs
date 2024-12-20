use crate::{AppError, MAX_ROWS, TableWorker, AIRPORTS_DATA_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
// use geozero::wkb::{FromWkb, WkbDialect};
// use sqlx::postgres::PgTypeInfo;
// use sqlx::Postgres;
// use geozero::wkb::GpkgWkb;
// use geozero::wkt::WktWriter;
// use sqlx::postgres::{PgTypeInfo, PgValueRef};
// use sqlx::{Decode, Postgres};
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use sqlx::types::Json;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{RecordBatch, StringArray};
// use geo::Geometry;
// use geo_types::Geometry;
// use geo_types::Coord;
// use geozero::{wkb, ToWkt, wkt, ToWkb};
// use geo_types::Coord;
// use geozero::wkb::{FromWkb, WkbDialect};
// use geozero::{CoordDimensions, GeomProcessor, GeozeroGeometry};

#[derive(Debug, Default, FromRow, Serialize)]
pub struct AirportsData {
    pub airport_code: String,
    pub airport_name: Option<Json<AirportName>>,
    pub city: Option<Json<City>>,
    // pub coordinates: Option<wkb::Decode<Geometry<f64>>>,
    // pub coordinates: Option<wkb::Decode<Geometry>>,
    // pub coordinates: Option<Point>, 
    pub timezone: Option<String>,
}

// #[derive(Debug, Default)]
// #[derive(Debug, sqlx::Decode, sqlx::Encode)]
// pub struct Point(wkb::Decode<Geometry>);
// pub struct Point {
//     pub x: f64,
//     pub y: f64,
// }

// impl Point {
//     pub fn new() -> Self {
//         Self { x: 0.0, y: 0.0 }
//     }
// }

// impl sqlx::Type<Postgres> for Point {
//     fn type_info() -> PgTypeInfo {
//         PgTypeInfo::with_name("point")
//     }
// }

// impl FromWkb for Point {
//     fn from_wkb<R: std::io::Read>(rdr: &mut R, dialect: WkbDialect) -> geozero::error::Result<Self> {
//         // use std::io::prelude::*;
//         // use std::io::Cursor;
//         // use geo_types::*;
//         // use wkb::*;
//         let mut pt = Point::new();
//         geozero::wkb::process_wkb_type_geom(rdr, &mut pt, dialect)?;
//         let mut bytes_cursor = std::io::Cursor::new(rdr);
//         // let pt = bytes_cursor.read_wkb().unwrap();

//         Ok(pt)
//     }
// }

// impl GeomProcessor for Point {
//     fn dimensions(&self) -> CoordDimensions {
//         CoordDimensions::xyz()
//     }

//     fn coordinate(&mut self, x: f64, y: f64, z: Option<f64>, _m: Option<f64>, _t: Option<f64>, _tm: Option<u64>, _idx: usize) -> geozero::error::Result<()> {
//         self.x = x;
//         self.y = y;
//         // self.z = z.unwrap_or(0.0);
//         let _ = z;
//         Ok(())
//     }
// }

// impl GeozeroGeometry for Point {
//     fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> std::result::Result<(), geozero::error::GeozeroError> {
//         processor.point_begin(0)?;
//         processor.coordinate(self.x, self.y, None, None, None, None, 0)?;
//         processor.point_end(0)
//     }
//     fn dims(&self) -> CoordDimensions {
//         CoordDimensions::xyz()
//     }
// }

// impl From<String> for PointWrapper {     
//     fn from(value: String) -> Self { 
//         let mut data = value;       
//         data.retain(|x| {!['(', ')'].contains(&x)});
//         let c = data.split(",").collect::<Vec<_>>();
//         let (x, y) = (c.get(0).unwrap().parse::<f64>().unwrap(), c.get(1).unwrap().parse::<f64>().unwrap());
//         Self(x, y)
//     } 
// }  

// impl sqlx::Type<sqlx::Postgres> for Point {
//     fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
//         <(f64, f64) as sqlx::Type<sqlx::Postgres>>::type_info()
//         // <Point as sqlx::Type<sqlx::Postgres>>::type_info()
//         // <String as sqlx::Type<sqlx::Postgres>>::type_info()
//     }
// }

// impl<'de> Decode<'de, Postgres> for Point {
//     fn decode(value: PgValueRef) -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>> {
//         let mut blob = <&[u8] as Decode<Postgres>>::decode(value)?;
//         let mut data: Vec<u8> = Vec::new();
//         let mut writer = WktWriter::new(&mut data);
//         dbg!("here");
//         wkb::process_ewkb_geom(&mut blob, &mut writer)
//             .map_err(|e| sqlx::Error::Decode(e.to_string().into()))?;
//         dbg!("here2");
//         // let text = Text(std::str::from_utf8(&data).unwrap().to_string());
//         let mut data = std::str::from_utf8(&data).unwrap().to_string();
//         dbg!("here3");

//         data.retain(|x| { !['(', ')'].contains(&x) });
//         let c = data.split(",").collect::<Vec<_>>();
//         let (x, y) = (c.get(0).unwrap().parse::<f64>().unwrap(), c.get(1).unwrap().parse::<f64>().unwrap());


//         Ok(Self { x, y })
//     }
// }

// impl FromRow<'_, PgRow> for AirportsData {
//     fn from_row(row: &PgRow) -> std::result::Result<AirportsData, sqlx::Error> {
//         let airport_code = row.try_get::<String, _>("airport_code")?;
//         println!("airport_code: {}", airport_code);
//         let airport_name = row.try_get::<Json<AirportName>, _>("airport_name")?;
//         println!("airport_name: {:?}", airport_name);
//         let city = row.try_get::<Json<City>, _>("city")?;
//         println!("city: {:?}", city);
//         // let coordinates = row.try_get::<Point, _>("coordinates")?;
//         // let coordinates = row.try_get::<String, _>("coordinates")?;
//         // let c: String = row.try_get("coordinates")?;
//         let p: Point = row.try_get("coordinates")?;
//         println!("c: {:?}", p);
//         // let coordinates: PointWrapper = PointWrapper::from(c);
//         let timezone = row.try_get::<String, _>("timezone")?;
//         println!("timezone: {}", timezone);

//         // Ok(AirportsData { 
//         //     airport_code, 
//         //     airport_name: Some(airport_name), 
//         //     city: Some(city), 
//         //     , 
//         //     timezone: Some(timezone), 
//         // })
//         Ok(AirportsData::default())
//     }
// }

/*
impl From<String> for UuidWrapper {     
    fn from(value: String) -> Self {         
        Self(Uuid::from_str(value.as_str()).unwrap())     
    } 
}  
impl From<()> for UuidWrapper {     
    fn from(_value: ()) -> Self {         
        Self(Uuid::default())     
    } 
} 
*/

// impl<'r> sqlx::FromRow<'r, PgRow> for Point {
//     fn from_row(row: &'r PgRow) -> std::result::Result<Self, sqlx::Error> {
//         let coordinates = row.try_get::<(f64,f64), _>("coordinates").unwrap();
//             // .map_err(|e| e.into())?;

//         Ok(Point {
//             x: coordinates.0,
//             y: coordinates.1
//         })
//     }
// }

#[derive(Debug, Serialize, Deserialize)]
pub struct AirportName {
    pub en: Option<String>,
    pub ru: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct City {
    pub en: Option<String>,
    pub ru: Option<String>,
}

impl AirportsData {
    pub fn new() -> Self {
        Self::default()
        // Self {
        //     airport_code: String::new(),
        //     airport_name: None,
        //     city: None,
        //     // coordinates: None,
        //     timezone: None, 
        // }
    }

    pub fn table_name() -> String {
        AIRPORTS_DATA_TABLE_NAME.to_string()
    }
}

impl AirportsData {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("airport_code", DataType::Utf8, false),
            Field::new("airport_name", DataType::Utf8, true),
            Field::new("city", DataType::Utf8, true),
            // Field::new("coordinates", DataType::Utf8, true),
            Field::new("timezone", DataType::Utf8, true),
        ])
    }

    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame, AppError> {
        let mut airport_codes = Vec::new();
        let mut airport_names = Vec::new();
        let mut cities= Vec::new();
        // let mut coordinates_all = Vec::new();
        let mut timezones = Vec::new();

        for record in records {
            airport_codes.push(record.airport_code.clone());
            let airport_name = match &mut record.airport_name {
                Some(val) => {
                    Some(serde_json::to_string(&val)?)
                },
                None => None
            };
            airport_names.push(airport_name);
            let city = match &mut record.city {
                Some(val) => {
                    Some(serde_json::to_string(&val)?)
                },
                None => None
            };
            cities.push(city);

            timezones.push(record.timezone.clone());
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(StringArray::from(airport_codes)), 
                Arc::new(StringArray::from(airport_names)),
                Arc::new(StringArray::from(cities)),
                // Arc::new(StringArray::from(coordinates_all)),
                Arc::new(StringArray::from(timezones)),
            ],
        )?;
        let df = ctx.read_batch(batch)?;

        Ok(df)
    }
}

#[async_trait]
impl TableWorker for AirportsData {
    async fn query_table(&self, pool: &PgPool) -> Result<(), AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);

        Ok(())
    }

    async fn query_table_to_string(&self, pool: &PgPool) -> Result<Vec<String>, AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
        let query = sqlx::query(&sql);
        let data: Vec<PgRow> = query.fetch_all(pool).await?;
    
        let rows: Vec<String> = data
            .iter()
            // .map(|row| format!("airport_code: {}, airport_name: {}, city: {}, coordinates: {:?}, timezone: {}", 
            .map(|row| format!("airport_code: {}, airport_name: {}, city: {}, timezone: {}", 
                row.get::<String, _>("airport_code"), 
                row.get::<Value, _>("airport_name"), 
                row.get::<Value, _>("city"),
                // row.get::<Point, _>("coordinates"),
                row.get::<String, _>("timezone"),
            ))
            .collect();
    
        Ok(rows)
    }

    async fn query_table_to_df(&self, pool: &PgPool) -> Result<DataFrame, AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let mut records = query.fetch_all(pool).await?;
        let ctx = SessionContext::new();
        let df = Self::to_df(ctx, &mut records)?;

        Ok(df)
    }

    async fn query_table_to_json(&self, pool: &PgPool) -> Result<String, AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        let res = serde_json::to_string(&data)?;
        
        Ok(res)
    }
}
