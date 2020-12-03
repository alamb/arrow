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

#![feature(test)]
extern crate parquet;
extern crate rand;

use bench_helper::*;

#[allow(dead_code)]
#[path = "common.rs"]
mod common;
use common::*;

use std::sync::Arc;

use parquet::{
    data_type::*,
    decoding::*,
    encoding::*,
    memory::{BufferPtr, MemTracker},
    schema::types::ColumnDescriptor,
};

/// Actually run the decoder for benchmarking, called from the bench hotloop
fn bench_decoder<T: DataType>(
    decoder: &mut Box<dyn Decoder<T>>,
    input: BufferPtr<u8>,
    output: &mut Vec<T::T>,
    num_values: usize,
    batch_size: usize,
) {
    decoder
        .set_data(input, num_values)
        .expect("set_data() should be OK");

    loop {
        if decoder.get(output).expect("get() should be OK") < batch_size {
            break;
        }
    }
}

// Util function to allow type level programing for benchmarks
fn setup_encoder_bench<M, T>(
    dname: &str,
    mname: &str,
    c: &mut Criterion<M>,
    batch_sizes: &[usize],
    make_encoder: fn(Arc<ColumnDescriptor>, Arc<MemTracker>) -> Box<dyn Encoder<T>>,
) where
    M: Measurement,
    T: DataType + GenRandomValueType<T>,
{
    let phys_type = T::get_physical_type();
    let bench_name = format!("{}_encoder_{:?}::{}", dname, phys_type, mname).to_lowercase();
    let mut group = c.benchmark_group(bench_name);

    for batch_size in batch_sizes {
        let bench_id = BenchmarkId::from_parameter(batch_size);

        // Make the objects we dont care about upfront to avoid benchmarking things like vec::new
        let batch_size = *batch_size as usize;
        let (bytes, values) = T::gen_values(batch_size);
        let mem_tracker = Arc::new(MemTracker::new());
        let col_desc = Arc::new(col_desc(0, T::get_physical_type()));
        let mut encoder = make_encoder(col_desc, mem_tracker);

        group.throughput(Throughput::Bytes(bytes as u64));
        group.bench_with_input(bench_id, &batch_size, |b, _| {
            b.iter(|| {
                encoder.put(&values).expect("put() should be Ok");
                encoder.flush_buffer().expect("flush_buffer() should be OK");
            });
        });
    }

    group.finish();
}

// Util function to allow type level programing for benchmarks
fn setup_decoder_bench<M, T>(
    dname: &str,
    mname: &str,
    c: &mut Criterion<M>,
    num_values: usize,
    batch_sizes: &[usize],
    make_encoder: fn(Arc<ColumnDescriptor>, Arc<MemTracker>) -> Box<dyn Encoder<T>>,
    make_decoder: fn() -> Box<dyn Decoder<T>>,
) where
    M: Measurement,
    T: DataType + GenRandomValueType<T>,
{
    let phys_type = T::get_physical_type();
    let bench_name =
        format!("{}_decoder_{:?}::{}", dname, phys_type, mname).to_lowercase();
    let mut group = c.benchmark_group(bench_name);

    let (_, values) = T::gen_values(num_values);
    let mem_tracker = Arc::new(MemTracker::new());
    let col_desc = Arc::new(col_desc(0, T::get_physical_type()));
    let mut encoder = make_encoder(col_desc, mem_tracker);
    encoder.put(&values).expect("put() should be Ok");
    let buffer = encoder.flush_buffer().expect("flush_buffer() should be OK");

    let mut decoder = make_decoder();

    for batch_size in batch_sizes {
        // Make the objects we dont care about upfront to avoid benchmarking things like vec::new
        let batch_size = *batch_size as usize;
        let mut output = vec![T::T::default(); batch_size];
        let bench_id = BenchmarkId::from_parameter(batch_size);

        group.throughput(Throughput::Bytes((std::mem::size_of::<T::T>() * batch_size) as u64));
        group.bench_with_input(bench_id, &batch_size, |b, &batch_size| {
            b.iter(|| {
                bench_decoder::<T>(
                    &mut decoder,
                    buffer.clone(),
                    &mut output,
                    values.len(),
                    batch_size,
                );
                output.clear();
            });
        });
    }

    group.finish();
}

// Util function to allow type level programing for benchmarks for dicts
// This exists specifically due to Sized on T for dict inner types
fn setup_decoder_dict_bench<M, T>(
    mname: &str,
    c: &mut Criterion<M>,
    num_values: usize,
    batch_sizes: &[usize],
) where
    M: Measurement,
    T: DataType + GenRandomValueType<T>,
{
    let phys_type = T::get_physical_type();
    let bench_name = format!("dict_decoder_{:?}::{}", phys_type, mname).to_lowercase();
    let mut group = c.benchmark_group(bench_name);

    let (_, values) = T::gen_values(num_values);
    let mem_tracker = Arc::new(MemTracker::new());
    let col_desc = Arc::new(col_desc(0, T::get_physical_type()));
    let mut encoder = DictEncoder::<T>::new(col_desc, mem_tracker);
    encoder.put(&values).expect("put() should be Ok");
    let buffer = encoder.flush_buffer().expect("flush_buffer() should be OK");

    let mut inner = Box::new(PlainDecoder::new(0));
    inner.set_data(
        encoder.write_dict().expect("write_dict() should be Ok"),
        encoder.num_entries()
    )
    .expect("set_data() should be Ok");

    let mut decoder = DictDecoder::<T>::new();
    decoder.set_dict(inner).expect("set_dict() should be Ok");

    for batch_size in batch_sizes {
        // Make the objects we dont care about upfront to avoid benchmarking things like vec::new
        let batch_size = *batch_size as usize;
        let mut output = vec![T::T::default(); batch_size];
        let bench_id = BenchmarkId::from_parameter(batch_size);

        group.throughput(Throughput::Bytes((std::mem::size_of::<T::T>() * batch_size) as u64));
        group.bench_with_input(bench_id, &batch_size, |b, &batch_size| {
            b.iter(|| {
                decoder
                    .set_data(buffer.clone(), num_values)
                    .expect("set_data() should be OK");

                loop {
                    if decoder.get(&mut output).expect("get() should be OK") < batch_size {
                        break;
                    }
                }
                output.clear();
            });
        });
    }

    group.finish();
}

/// Abuse the type system to make decoder benchmarks
macro_rules! make_bench {
    (name=$name: literal;
     measure_name=$mname: expr;
     criterion=$crit: expr;
     num_values=$num_vals: expr;
     batch_sizes=$batchs: expr;
     encoder=$mk_encoder: block;
     decoder=$mk_decoder: block;
     types: [$($ty: ty),*]) => {
        $(
            setup_encoder_bench::<M, $ty>($name, $mname, $crit, &$batchs, $mk_encoder);
        )*

        $(
            setup_decoder_bench::<M, $ty>($name, $mname, $crit, $num_vals, &$batchs,
                $mk_encoder, $mk_decoder);
        )*
    };
    (@dict
     measure_name=$mname: expr;
     criterion=$crit: expr;
     num_values=$num_vals: expr;
     batch_sizes=$batchs: expr;
     types: [$($ty: ty),*]) => {
        $(
            setup_encoder_bench::<M, $ty>("dict", $mname, $crit, &$batchs,
                |cd, mt| { Box::new(DictEncoder::<$ty>::new(cd, mt)) }
            );
        )*

        $(
            setup_decoder_dict_bench::<M, $ty>($mname, $crit, $num_vals, &$batchs);
        )*
    };

}

fn decoding<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
    make_bench! {
        name = "plain";
        measure_name = measure_name;
        criterion = c;
        num_values = 1024;
        batch_sizes = [8, 16, 32, 64, 128];
        encoder = { |cd, mt| Box::new(PlainEncoder::new(cd, mt, vec![])) };
        decoder = { || Box::new(PlainDecoder::new(0)) };
        types: [BoolType, Int32Type, Int64Type, Int96Type, FloatType, DoubleType, ByteArrayType]
    };

    make_bench! {
        @dict
        measure_name = measure_name;
        criterion = c;
        num_values = 1024;
        batch_sizes = [8, 16, 32, 64, 128];
        types: [BoolType, Int32Type, Int64Type, Int96Type, FloatType, DoubleType, ByteArrayType]
    };

    make_bench! {
        name = "rle";
        measure_name = measure_name;
        criterion = c;
        num_values = 100;
        batch_sizes = [8, 16, 32, 64, 128];
        encoder = { |_, _| Box::new(RleValueEncoder::new()) };
        decoder = { || Box::new(RleValueDecoder::new()) };
        types: [BoolType]
    };

    make_bench! {
        name = "delta_bin_packed";
        measure_name = measure_name;
        criterion = c;
        num_values = 512;
        batch_sizes = [8, 16, 32, 64, 128];
        encoder = { |_, _| Box::new(DeltaBitPackEncoder::new()) };
        decoder = { || Box::new(DeltaBitPackDecoder::new()) };
        types: [Int32Type, Int64Type]
    };

    make_bench! {
        name = "delta_len_byte_arry";
        measure_name = measure_name;
        criterion = c;
        num_values = 512;
        batch_sizes = [8, 16, 32, 64, 128];
        encoder = { |_, _| Box::new(DeltaLengthByteArrayEncoder::new()) };
        decoder = { || Box::new(DeltaLengthByteArrayDecoder::new()) };
        types: [ByteArrayType]
    };

    make_bench! {
        name = "delta_byte_arry";
        measure_name = measure_name;
        criterion = c;
        num_values = 512;
        batch_sizes = [8, 16, 32, 64, 128];
        encoder = { |_, _| Box::new(DeltaByteArrayEncoder::new()) };
        decoder = { || Box::new(DeltaByteArrayDecoder::new()) };
        types: [ByteArrayType]
    };
}

bench_group!(decoder, decoding);
bench_main!(decoder);
