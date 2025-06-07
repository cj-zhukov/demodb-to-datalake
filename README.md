# demodb-to-datalake
demodb-to-datalake is a Rust library for processing data from postgres database into Data Lake.

## Description
demodb-to-datalake reads data from postgres database table into datafusion dataframe and saves it as parquet file.
Demonstration Database demo can be downloaded from here https://postgrespro.com/community/demodb

## Installation
- Download database
```bash
wget /path/to/data/db.sql
```
- install database
```bash
psql -h localhost -f path/to/data/db.sql
```
- Install demodb-to-database crate
- Run examples
```bash
cargo run --example 01-query-table-dyn
cargo run --example 02-query-table-static
```
