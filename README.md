# parquet-bloom-filter-analysis

Generate parquet files for analysing the effectiveness of bloom filters.

## Usage

This repo provides a CLI for generating parquet data:
```
Parquet Generator
Usage: parquet-gen [OPTIONS]

Options:
  -s, --spec <SPEC>
          The data specification file to use [default: default-spec.toml]
  -n, --n-files <N_FILES>
          The number of files to generate [default: 1]
  -o, --output-dir <OUTPUT_DIR>
          [default: data/]
  -r, --rows-per-file <ROWS_PER_FILE>
          The number of rows per parquet file generated [default: 10485760]
  -g, --max-row-group-size <MAX_ROW_GROUP_SIZE>
          The desired max row group size for generated parquet files [default: 1048576]
  -d, --dup-factor <DUPLICATION_FACTOR>
          How many duplicate values to produce for each unique combination of column values [default: 1]
  -h, --help
          Print help
  -V, --version
          Print version

```

A spec file can be defined, and passed using the `--spec` argument, but the [default](default-spec.toml) will be used if none is provided:
```toml
# Defined columns in the columns list:
[[columns]]
# each column needs a unique name:
name = "t0"
# each column needs a cardinality
cardinality = 100 
# an optional base can be provided, so values produced in this column
# take the form, "first-00", "first-01", ..., up to "first-99"
base = "first-"

[[columns]]
name = "t1"
cardinality = 10000
base = "second-"

[[columns]]
name = "t2"
# write with a bloom filter on this column
bloom_filter = {
  # specify the false positive probability
  fpp = 0.1,
  # specify the nubmer of distinct values for the filter:
  ndv = 100000
}
cardinality = 1000000
base = "third-"
```
