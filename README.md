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

A spec file can be defined, and passed using the `--spec` argument.
See [`default-spec.toml`](default-spec.toml) for an example.

## Example

Running the command
```
parquet-gen -s sample.toml -o sample/ -r 16 -d 2
```

Where `sample.toml` contains the following:
```toml
[[columns]]
name = "region"
cardinality = 2
base = "us-"

[[columns]]
name = "host"
cardinality = 4
base = "prod-"

[[columns]]
name = "container_id"
cardinality = 8
base = "id-"
```

Would produce a parquet file with data arranged like so:
```
+--------+--------+--------------+
| region | host   | container_id |
+--------+--------+--------------+
| us-0   | prod-1 | id-2         |
| us-0   | prod-1 | id-2         |
| us-0   | prod-1 | id-4         |
| us-0   | prod-1 | id-4         |
| us-0   | prod-2 | id-3         |
| us-0   | prod-2 | id-3         |
| us-0   | prod-2 | id-7         |
| us-0   | prod-2 | id-7         |
| us-1   | prod-0 | id-1         |
| us-1   | prod-0 | id-1         |
| us-1   | prod-0 | id-5         |
| us-1   | prod-0 | id-5         |
| us-1   | prod-3 | id-0         |
| us-1   | prod-3 | id-0         |
| us-1   | prod-3 | id-6         |
| us-1   | prod-3 | id-6         |
+--------+--------+--------------+
```

