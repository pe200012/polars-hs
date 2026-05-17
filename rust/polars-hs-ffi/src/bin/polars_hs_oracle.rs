//! Small Rust Polars oracle for Hspec parity tests.
//!
//! The binary intentionally lives in the pinned FFI crate so parity tests use
//! the same `polars = 0.53.0` dependency as the binding under test.

use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

use polars::prelude::*;

fn csv_read_options(path: PathBuf) -> Result<DataFrame, Box<dyn Error>> {
    Ok(CsvReadOptions::default()
        .with_has_header(false)
        .map_parse_options(|parse_options| {
            parse_options
                .with_separator(b';')
                .with_null_values(Some(NullValues::AllColumnsSingle("NA".into())))
        })
        .try_into_reader_with_file_path(Some(path))?
        .finish()?)
}

fn parquet_read_n_rows(path: PathBuf) -> Result<DataFrame, Box<dyn Error>> {
    let file = File::open(path)?;
    Ok(ParquetReader::new(file).with_slice(Some((0, 2))).finish()?)
}

fn write_canonical_csv(mut dataframe: DataFrame) -> Result<(), Box<dyn Error>> {
    let mut bytes = Vec::new();
    CsvWriter::new(&mut bytes)
        .include_header(true)
        .with_null_value("NULL".into())
        .finish(&mut dataframe)?;
    io::stdout().write_all(&bytes)?;
    Ok(())
}

fn required_arg(
    args: &mut impl Iterator<Item = String>,
    label: &str,
) -> Result<String, Box<dyn Error>> {
    args.next()
        .ok_or_else(|| format!("missing required argument: {label}").into())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let command = required_arg(&mut args, "command")?;
    let path = PathBuf::from(required_arg(&mut args, "path")?);
    let dataframe = match command.as_str() {
        "csv-read-options" => csv_read_options(path)?,
        "parquet-read-n-rows" => parquet_read_n_rows(path)?,
        other => return Err(format!("unknown oracle command: {other}").into()),
    };
    write_canonical_csv(dataframe)
}
