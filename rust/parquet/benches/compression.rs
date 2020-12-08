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
#[macro_use]
extern crate lazy_static;


use criterion::*;
use criterion::measurement::*;
//use bench_helper::*;
use std::{
    env,
    collections::HashMap,
    fs::File
};

use parquet::{basic::Compression, compression::*, file::reader::*};

// 10k rows written in page v2 with type:
//
//   message test {
//     required binary binary_field,
//     required int32 int32_field,
//     required int64 int64_field,
//     required boolean boolean_field,
//     required float float_field,
//     required double double_field,
//     required fixed_len_byte_array(1024) flba_field,
//     required int96 int96_field
//   }
//
// filled with random values.
const TEST_FILE: &str = "10k-v2.parquet";

fn get_f_reader() -> SerializedFileReader<File> {
    let mut path_buf = env::current_dir().unwrap();
    path_buf.push("data");
    path_buf.push(TEST_FILE);
    let file = File::open(path_buf.as_path()).unwrap();
    SerializedFileReader::new(file).unwrap()
}

fn get_pages_bytes(col_idx: usize) -> Vec<u8> {
    let mut data: Vec<u8> = Vec::new();
    let f_reader = get_f_reader();
    let rg_reader = f_reader.get_row_group(0).unwrap();
    let mut pg_reader = rg_reader.get_column_page_reader(col_idx).unwrap();
    while let Some(p) = pg_reader.get_next_page().unwrap() {
        data.extend_from_slice(p.buffer().data());
    }
    data
}

lazy_static! {
    static ref COMPRESSION_ALGOS: Vec<Compression> = {
        vec![
            Compression::BROTLI, Compression::GZIP, Compression::SNAPPY,
            Compression::LZ4,
            Compression::ZSTD
        ]
    };

    static ref COL_TYPES: HashMap<&'static str, usize> = {
        let mut hm = HashMap::new();
        hm.insert("binary", 0);
        hm.insert("int32", 1);
        hm.insert("int64", 2);
        hm.insert("boolean", 3);
        hm.insert("float", 4);
        hm.insert("double", 5);
        hm.insert("fixed", 6);
        hm.insert("int96", 7);
        hm
    };

    static ref COL_DATA: Vec<Vec<u8>> = COL_TYPES
        .values()
        .map(|idx| get_pages_bytes(*idx))
        .collect();

    static ref COMPRESSED_PAGES: HashMap<Compression, Vec<Vec<u8>>> = COMPRESSION_ALGOS
        .iter()
        .map(|algo| {
            let data = COL_DATA
                .iter()
                .map(|raw_data| {
                    let mut codec = create_codec(*algo).unwrap().unwrap();
                    let mut v = vec![];
                    codec.compress(&raw_data, &mut v).unwrap();
                    v
                })
                .collect();

            (*algo, data)
        })
        .collect();
}

//fn compress<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
fn compress<M: Measurement>(c: &mut Criterion<M>) {
    let measure_name: &str = "wall_time";
    for algo in COMPRESSION_ALGOS.iter() {
        let bench_name = format!("compress_{:?}::{}", algo, measure_name).to_lowercase();
        let mut group = c.benchmark_group(bench_name);

        for (name, idx) in COL_TYPES.iter() {
            let data = &COL_DATA[*idx];
            group.throughput(Throughput::Bytes(data.len() as u64));
            group.bench_function(*name, |bench| {
                let mut codec = create_codec(*algo).unwrap().unwrap();
                let mut v = Vec::with_capacity(data.len());

                bench.iter(|| {
                    codec.compress(&data, &mut v).unwrap();
                })
            });
        }
    }
}

//fn decompress<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
fn decompress<M: Measurement>(c: &mut Criterion<M>) {
    let measure_name: &str = "wall_time";
    for algo in COMPRESSION_ALGOS.iter() {
        let bench_name = format!("decompress_{:?}::{}", algo, measure_name).to_lowercase();
        let mut group = c.benchmark_group(bench_name);

        for (name, idx) in COL_TYPES.iter() {
            let comp_len = COL_DATA[*idx].len();
            group.throughput(Throughput::Bytes(comp_len as u64));
            group.bench_function(*name, |bench| {
                let mut codec = create_codec(*algo).unwrap().unwrap();
                let comp_data = &COMPRESSED_PAGES.get(algo).unwrap()[*idx];
                let mut v = Vec::with_capacity(comp_len);

                bench.iter(|| {
                    let _ = codec.decompress(&comp_data, &mut v).unwrap();
                });
            });
        }
    }
}

criterion_group!(benches, compress, decompress);
criterion_main!(benches);
