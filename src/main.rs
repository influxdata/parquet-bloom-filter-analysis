use std::{io::Write, path::PathBuf, sync::Arc};

use clap::Parser;
use datafusion::arrow::datatypes::SchemaRef;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Config {
    /// The number of files to generate
    #[clap(short = 'N', long = "n-files", default_value_t = 1)]
    n_files: usize,

    // The directory to output files in
    #[clap(short = 'o', long = "output-dir", default_value = "data/")]
    output_dir: PathBuf,

    /// The number of rows per parquet file generated
    #[clap(short = 'R', long = "rows-per-file", default_value_t = 10_000_000)]
    rows_per_file: usize,

    /// The desired max row grou size for generated parquet files
    #[clap(short = 'g', long = "max-row-group-size", default_value_t = 1_000_000)]
    max_row_group_size: usize,
}

fn main() {
    println!("Parquet Generator");
    let config = Config::parse();

    let schema = create_schema(&config);
    for f in 0..config.n_files {
        let sink = Vec::new();
        let writer = create_writer(sink, Arc::clone(&schema), &config);
    }
}

fn create_schema(config: &Config) -> SchemaRef {
    todo!();
}

fn create_writer<W: Write + Send>(sink: W, schema: SchemaRef, config: &Config) -> ArrowWriter<W> {
    let props = WriterProperties::builder()
        .set_max_row_group_size(config.max_row_group_size)
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();

    ArrowWriter::try_new(sink, schema, Some(props)).expect("create ArrowWriter")
}

struct DataSpec {
    columns: Vec<ColumnSpec>,
}

struct ColumnSpec {
    bloom_filter: bool,
    cardinality: u32,
    base: Option<String>,
}
