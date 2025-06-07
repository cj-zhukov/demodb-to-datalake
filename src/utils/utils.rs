use std::io::Cursor;

use datafusion::{arrow::datatypes::Schema, parquet::arrow::AsyncArrowWriter, prelude::*};
use futures_util::TryStreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};
use tokio_stream::StreamExt;

use crate::AppError;

pub async fn write_df_to_file(df: DataFrame, file_path: &str) -> Result<(), AppError> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None)?;
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch).await?;
    }
    writer.close().await?;

    let mut file = File::create(file_path).await?;
    file.write_all(&buf).await?;

    Ok(())
}

pub async fn read_file_to_df(file_path: &str) -> Result<DataFrame, AppError> {
    let mut buf = vec![];
    let _n = File::open(file_path).await?.read_to_end(&mut buf).await?;
    let stream = ParquetRecordBatchStreamBuilder::new(Cursor::new(buf))
        .await?
        .build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    let ctx = SessionContext::new();
    let df = ctx.read_batches(batches)?;

    Ok(df)
}
