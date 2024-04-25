use std::{
    collections::VecDeque,
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    rc::Rc,
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, StringBuilder, StructArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use clap::Parser;
use parquet::{
    arrow::ArrowWriter,
    file::{properties::WriterProperties, writer::TrackedWrite},
    format::FileMetaData,
    schema::types::ColumnPath,
};
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Config {
    /// The data specification file to use
    #[clap(short = 's', long = "spec", default_value = "default-spec.toml")]
    spec: PathBuf,

    /// The number of files to generate
    #[clap(short = 'n', long = "n-files", default_value_t = 1)]
    n_files: usize,

    // The directory to output files in
    #[clap(short = 'o', long = "output-dir", default_value = "data/")]
    output_dir: PathBuf,

    /// The number of rows per parquet file generated
    #[clap(short = 'r', long = "rows-per-file", default_value_t = 10 * 1024 * 1024)]
    rows_per_file: usize,

    /// The desired max row group size for generated parquet files
    #[clap(short = 'g', long = "max-row-group-size", default_value_t = 1024 * 1024)]
    max_row_group_size: usize,

    /// How many duplicate values to produce for each unique combination of column values
    #[clap(short = 'd', long = "dup-factor", default_value_t = 1)]
    duplication_factor: usize,
}

fn main() {
    println!("Parquet Generator");
    let config = Config::parse();
    let _ = std::fs::create_dir_all(&config.output_dir);

    let spec = parse_spec(&config);
    let schema = create_schema(&spec);
    let mut rng = SmallRng::seed_from_u64(0);
    let mut generator = RowGenerator::new(&mut rng, &spec, config.duplication_factor);
    for f in 0..config.n_files {
        let file_path = config.output_dir.join(format!("{f}.parquet"));
        println!("writing to {file_path:?}");
        let sink = File::create_new(file_path).expect("create new parquet file");
        let writer = create_writer(sink, Arc::clone(&schema), &config, &spec);
        let (bytes_written, _meta) = generate_file(&config, writer, &mut generator);
        println!("bytes written: {bytes_written}");
    }
}

fn generate_file<W: Write + Send>(
    config: &Config,
    mut writer: ArrowWriter<W>,
    generator: &mut RowGenerator,
) -> (usize, FileMetaData) {
    let mut row_count = 0;
    while row_count < config.rows_per_file {
        let batch = generator.generate_record_batch(config.rows_per_file);
        if batch.num_rows() == 0 {
            continue;
        }
        row_count += batch.num_rows();
        writer.write(&batch).expect("write batch");
    }
    let metadata = writer.finish().expect("close writer and get meta data");
    let bytes_written = writer.bytes_written();
    (bytes_written, metadata)
}

fn parse_spec(config: &Config) -> DataSpec {
    let mut spec_file = File::open(&config.spec).expect("open spec file");
    let mut buf = String::new();
    spec_file
        .read_to_string(&mut buf)
        .expect("read spec file to string");
    toml::from_str(&buf).expect("parse spec file")
}

fn create_schema(spec: &DataSpec) -> SchemaRef {
    let mut fields = Vec::new();
    for column in &spec.columns {
        fields.push(Field::new(column.name.as_str(), DataType::Utf8, false));
    }
    let schema = Schema::new(fields);
    Arc::new(schema)
}

fn create_writer<W: Write + Send>(
    sink: W,
    schema: SchemaRef,
    config: &Config,
    spec: &DataSpec,
) -> ArrowWriter<TrackedWrite<W>> {
    let mut builder = WriterProperties::builder()
        .set_max_row_group_size(config.max_row_group_size)
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()));

    for column in &spec.columns {
        if let Some(bf) = &column.bloom_filter {
            let path = ColumnPath::from(column.name.as_str());
            builder = builder
                .set_column_bloom_filter_enabled(path.clone(), true)
                .set_column_bloom_filter_fpp(path.clone(), bf.fpp)
                .set_column_bloom_filter_ndv(path, bf.ndv);
        }
    }

    let props = builder.build();

    ArrowWriter::try_new(TrackedWrite::new(sink), schema, Some(props)).expect("create ArrowWriter")
}

#[derive(Debug, Deserialize)]
struct DataSpec {
    columns: Vec<ColumnSpec>,
}

#[derive(Debug, Deserialize, Clone)]
struct ColumnSpec {
    name: String,
    bloom_filter: Option<BloomFilter>,
    cardinality: u32,
    base: Option<String>,
}

impl ColumnSpec {
    fn gen_cardinality_range(&self) -> Vec<u32> {
        (0..self.cardinality).into_iter().collect()
    }
}

#[derive(Debug, Deserialize, Clone)]
struct BloomFilter {
    fpp: f64,
    ndv: u64,
}

#[derive(Debug, Clone)]
struct Row {
    cols: Vec<Rc<str>>,
}

impl From<Vec<Rc<str>>> for Row {
    fn from(cols: Vec<Rc<str>>) -> Self {
        Self { cols }
    }
}

impl From<VecDeque<Rc<str>>> for Row {
    fn from(vd: VecDeque<Rc<str>>) -> Self {
        Self {
            cols: Vec::from(vd),
        }
    }
}

#[derive(Debug)]
struct RowBuilder {
    cols: Vec<StringBuilder>,
}

impl RowBuilder {
    fn new(num_cols: usize) -> Self {
        Self {
            cols: (0..num_cols).map(|_| Default::default()).collect(),
        }
    }

    fn append(&mut self, row: &Row) {
        for (col, b) in row.cols.iter().zip(self.cols.iter_mut()) {
            b.append_value(col);
        }
    }

    fn finish(&mut self) -> StructArray {
        let mut struct_fields = Vec::new();
        for (i, col) in self.cols.iter_mut().enumerate() {
            struct_fields.push((
                // The field name used here does not matter:
                Arc::new(Field::new(format!("col_{i}"), DataType::Utf8, false)),
                Arc::new(col.finish()) as ArrayRef,
            ));
        }
        StructArray::from(struct_fields)
    }
}

impl<'a> Extend<&'a Row> for RowBuilder {
    fn extend<T: IntoIterator<Item = &'a Row>>(&mut self, iter: T) {
        iter.into_iter().for_each(|row| self.append(row))
    }
}

#[derive(Debug)]
struct RowGenerator {
    cols: Vec<ColGenerator>,
    current: usize,
    dup_factor: usize,
}

impl RowGenerator {
    fn new(rng: &mut SmallRng, spec: &DataSpec, dup_factor: usize) -> Self {
        let mut columns: Vec<ColumnSpec> = spec.columns.iter().cloned().collect();
        columns.sort_unstable_by(|a, b| a.cardinality.cmp(&b.cardinality));
        let values = columns
            .iter()
            .map(|c| {
                let mut v = c.gen_cardinality_range();
                let width = ((c.cardinality - 1).checked_ilog10().unwrap_or(0) + 1) as usize;
                v.shuffle(rng);
                v.into_iter()
                    .map(|i| match &c.base {
                        Some(base) => format!("{base}{i:0>width$}"),
                        None => i.to_string(),
                    })
                    .map(Into::into)
                    .collect()
            })
            .collect::<Vec<Vec<ColGenerator>>>();
        let mut iter = values.into_iter().zip(columns).rev().peekable();
        let cols = loop {
            let next = iter.next();
            match (iter.peek_mut(), next) {
                (Some(low), Some(mut high)) => {
                    let chunk_size = (high.1.cardinality / low.1.cardinality) as usize;
                    for l in low.0.iter_mut() {
                        l.children = Some(high.0.drain(0..chunk_size.min(high.0.len())).collect());
                        l.sort_children();
                    }
                }
                (None, Some(mut last)) => {
                    last.0.sort_unstable_by(|a, b| a.value.cmp(&b.value));
                    break last.0;
                }
                (_, None) => unreachable!(),
            }
        };
        Self {
            cols,
            current: 0,
            dup_factor,
        }
    }

    fn generate(&mut self) -> Option<Row> {
        match self.cols[self.current].generate() {
            Some(v) => Some(v.into()),
            None => {
                self.current += 1;
                if self.current == self.cols.len() {
                    self.current = 0;
                    None
                } else {
                    self.cols[self.current].generate().map(Into::into)
                }
            }
        }
    }

    fn generate_record_batch(&mut self, max: usize) -> RecordBatch {
        let mut rows = Vec::new();
        'outer: while let Some(row) = self.generate() {
            for _ in 0..self.dup_factor {
                rows.push(row.clone());
                if rows.len() >= max {
                    break 'outer;
                }
            }
        }
        let mut builder = RowBuilder::new(self.num_cols());
        builder.extend(rows.as_slice());
        RecordBatch::from(&builder.finish())
    }

    fn num_cols(&self) -> usize {
        self.cols.first().map(|c| c.depth()).unwrap_or(0)
    }
}

#[derive(Debug)]
struct ColGenerator {
    value: Rc<str>,
    children: Option<Vec<ColGenerator>>,
    current: usize,
}

impl From<String> for ColGenerator {
    fn from(value: String) -> Self {
        Self {
            value: value.into(),
            children: None,
            current: 0,
        }
    }
}

impl ColGenerator {
    fn sort_children(&mut self) {
        if let Some(ref mut children) = self.children {
            children.sort_unstable_by(|a, b| a.value.cmp(&b.value));
            children.iter_mut().for_each(|c| c.sort_children());
        }
    }

    fn generate(&mut self) -> Option<VecDeque<Rc<str>>> {
        if let Some(ref mut children) = self.children {
            if self.current == children.len() {
                self.current = 0;
                return None;
            }
            match children[self.current].generate() {
                Some(vals) => {
                    self.current += 1;
                    return Some([self.value.clone()].into_iter().chain(vals).collect());
                }
                None => {
                    self.current += 1;
                    if self.current == children.len() {
                        self.current = 0;
                        return None;
                    }
                    let vals = children[self.current].generate()?;
                    Some([self.value.clone()].into_iter().chain(vals).collect())
                }
            }
        } else {
            Some([self.value.clone()].into_iter().collect())
        }
    }

    fn depth(&self) -> usize {
        1 + self
            .children
            .as_ref()
            .and_then(|c| c.first())
            .map(|c| c.depth())
            .unwrap_or(0)
    }
}
