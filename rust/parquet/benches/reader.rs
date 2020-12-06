// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//#![feature(test)]
extern crate parquet;

use bench_helper::*;

use std::{collections::HashMap, fs::File, path::Path};

use parquet::{
    column::reader::{get_typed_column_reader, ColumnReader},
    data_type::*,
    file::reader::{FileReader, SerializedFileReader},
    schema::{parser::parse_message_type, types::ColumnPath},
};

fn record_reader_10k_collect<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
    let path = Path::new("data/10k-v2.parquet");
    let file = File::open(&path).unwrap();
    let len = file.metadata().unwrap().len();
    let parquet_reader = SerializedFileReader::new(file).unwrap();

    let bench_name = format!("reader_{}", measure_name).to_lowercase();

    let mut group = c.benchmark_group(&bench_name);
    group.throughput(Throughput::Bytes(len as u64));
    group.bench_function("10k-v2", |b| {
        b.iter(|| {
            let projection = parse_message_type("
              message test {
                REQUIRED BYTE_ARRAY binary_field;
                REQUIRED INT32 int32_field;
                REQUIRED INT64 int64_field;
                REQUIRED INT96 int96_field;
                REQUIRED BOOLEAN boolean_field;
                REQUIRED FLOAT float_field;
                REQUIRED DOUBLE double_field;
                REQUIRED FIXED_LEN_BYTE_ARRAY (1024) flba_field;
              }
            ",).unwrap();
            let iter = parquet_reader.get_row_iter(Some(projection)).unwrap();
            let x = iter.collect::<Vec<_>>();
            black_box(x)
        })
    });
}

fn record_reader_stock_simulated_collect<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
    let path = Path::new("data/stock_simulated.parquet");
    let file = File::open(&path).unwrap();
    let len = file.metadata().unwrap().len();
    let parquet_reader = SerializedFileReader::new(file).unwrap();

    let bench_name = format!("reader_{}", measure_name).to_lowercase();

    let mut group = c.benchmark_group(&bench_name);
    group.throughput(Throughput::Bytes(len as u64));
    group.bench_function("stock-simulated-collect", |b| {
        b.iter(|| {
            let iter = parquet_reader.get_row_iter(None).unwrap();
            let _ = iter.collect::<Vec<_>>();
        })
    });
}

fn record_reader_stock_simulated_column<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
    // WARNING THIS BENCH IS INTENDED FOR THIS DATA FILE ONLY
    // COPY OR CHANGE THE DATA FILE MAY NOT WORK AS YOU WISH
    let path = Path::new("data/stock_simulated.parquet");
    let file = File::open(&path).unwrap();
    let len = file.metadata().unwrap().len();
    let parquet_reader = SerializedFileReader::new(file).unwrap();

    let descr = parquet_reader.metadata().file_metadata().schema_descr_ptr();
    let num_row_groups = parquet_reader.num_row_groups();
    let batch_size = 256;

    let bench_name = format!("reader_{}", measure_name).to_lowercase();

    let mut group = c.benchmark_group(&bench_name);
    group.throughput(Throughput::Bytes(len as u64));
    group.bench_function("stock-simulated-column", |b| {
        b.iter(|| {
            let mut current_row_group = 0;

            while current_row_group < num_row_groups {
                let row_group_reader =
                    parquet_reader.get_row_group(current_row_group).unwrap();
                let num_rows = row_group_reader.metadata().num_rows() as usize;

                let mut paths = HashMap::new();
                let row_group_metadata = row_group_reader.metadata();

                for col_index in 0..row_group_reader.num_columns() {
                    let col_meta = row_group_metadata.column(col_index);
                    let col_path = col_meta.column_path().clone();
                    paths.insert(col_path, col_index);
                }

                let mut readers = Vec::new();
                for field in descr.root_schema().get_fields() {
                    let col_path = ColumnPath::new(vec![field.name().to_owned()]);
                    let orig_index = *paths.get(&col_path).unwrap();
                    let col_reader = row_group_reader.get_column_reader(orig_index).unwrap();
                    readers.push(col_reader);
                }

                let mut def_levels = Some(vec![0; batch_size]);
                let mut rep_levels = None::<Vec<i16>>;

                for col_reader in readers.into_iter() {
                    match col_reader {
                        r @ ColumnReader::Int64ColumnReader(..) => {
                            let mut data_collected = Vec::with_capacity(num_rows);
                            let mut val = vec![0; batch_size];
                            let mut typed_reader = get_typed_column_reader::<Int64Type>(r);
                            while let Ok((values_read, _levels_read)) = typed_reader
                                .read_batch(
                                    batch_size,
                                    def_levels.as_mut().map(|x| &mut x[..]),
                                    rep_levels.as_mut().map(|x| &mut x[..]),
                                    &mut val,
                                )
                                {
                                    data_collected.extend_from_slice(&val);
                                    if values_read < batch_size {
                                        break;
                                    }
                                }
                        }
                        r @ ColumnReader::DoubleColumnReader(..) => {
                            let mut data_collected = Vec::with_capacity(num_rows);
                            let mut val = vec![0.0; batch_size];
                            let mut typed_reader = get_typed_column_reader::<DoubleType>(r);
                            while let Ok((values_read, _levels_read)) = typed_reader
                                .read_batch(
                                    batch_size,
                                    def_levels.as_mut().map(|x| &mut x[..]),
                                    rep_levels.as_mut().map(|x| &mut x[..]),
                                    &mut val,
                                )
                                {
                                    data_collected.extend_from_slice(&val);
                                    if values_read < batch_size {
                                        break;
                                    }
                                }
                        }
                        _ => unimplemented!(),
                    }
                }
                current_row_group += 1;
            }
        })
    });
}

bench_group!(benches, record_reader_10k_collect, record_reader_stock_simulated_column,
             record_reader_stock_simulated_collect);
bench_main!(benches);
