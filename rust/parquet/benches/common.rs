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

extern crate parquet;
extern crate rand;

use rand::{thread_rng, Rng};
use std::sync::Arc;

use parquet::{
    basic::*,
    data_type::*,
    schema::types::{ColumnDescriptor, ColumnPath, Type as SchemaType},
};

macro_rules! gen_random_ints {
    ($fname:ident, $limit:expr) => {
        pub fn $fname(total: usize) -> (usize, Vec<i32>) {
            let mut values = Vec::with_capacity(total);
            let mut rng = thread_rng();
            for _ in 0..total {
                values.push(rng.gen_range(0, $limit));
            }
            let bytes = values.len() * ::std::mem::size_of::<i32>();
            (bytes, values)
        }
    };
}

gen_random_ints!(gen_10, 10);
gen_random_ints!(gen_100, 100);
gen_random_ints!(gen_1000, 1000);

pub trait GenRandomValueType<T: DataType> {
    fn gen() -> T::T where T::T: Sized;

    fn gen_values(total: usize) -> (usize, Vec<T::T>) {
        let mut vals = Vec::with_capacity(total);
        for _ in 0..total {
            vals.push(Self::gen())
        }
        let bytes = vals.len() * ::std::mem::size_of::<T::T>();
        (bytes, vals)
    }
}

macro_rules! impl_basic_gen {
    ($ty: ty, $val_ty: ty) => {
        impl GenRandomValueType<$ty> for $ty {
            fn gen() -> $val_ty {
                thread_rng().gen()
            }
        }
    }
}

impl_basic_gen!(BoolType, bool);
impl_basic_gen!(Int32Type, i32);
impl_basic_gen!(Int64Type, i64);
impl_basic_gen!(FloatType, f32);
impl_basic_gen!(DoubleType, f64);

impl GenRandomValueType<Int96Type> for Int96Type {
    fn gen() -> Int96 {
        let mut rng = thread_rng();
        let mut val = Int96::new();
        val.set_data(rng.gen(), rng.gen(), rng.gen());
        val
    }
}

impl GenRandomValueType<ByteArrayType> for ByteArrayType {
    fn gen() -> ByteArray {
        let mut rng = thread_rng();
        // Make anything up to 16mb of data
        let size = rng.gen_range(0, 2usize.pow(24) - 1);
        let mut to_ret = Vec::with_capacity(size);

        for _ in 0..to_ret.len() {
            to_ret.push(rng.gen());
        }

        ByteArray::from(to_ret)
    }
}

impl GenRandomValueType<FixedLenByteArrayType> for ByteArray {
    fn gen() -> parquet::data_type::ByteArray {
        let mut rng = thread_rng();
        // Fixed size of 2000
        const SIZE: usize = 2000;
        let mut to_ret = Vec::with_capacity(SIZE);

        for _ in 0..to_ret.len() {
            to_ret.push(rng.gen());
        }

        ByteArray::from(to_ret).into()
    }
}

pub fn gen_test_strs(total: usize) -> (usize, Vec<ByteArray>) {
    let mut words = Vec::new();
    words.push("aaaaaaaaaa");
    words.push("bbbbbbbbbb");
    words.push("cccccccccc");
    words.push("dddddddddd");
    words.push("eeeeeeeeee");
    words.push("ffffffffff");
    words.push("gggggggggg");
    words.push("hhhhhhhhhh");
    words.push("iiiiiiiiii");
    words.push("jjjjjjjjjj");

    let mut rnd = rand::thread_rng();
    let mut values = Vec::new();
    for _ in 0..total {
        let idx = rnd.gen_range(0, 10);
        values.push(ByteArray::from(words[idx]));
    }
    let bytes = values.iter().fold(0, |acc, w| acc + w.len());
    (bytes, values)
}

pub fn col_desc(type_length: i32, primitive_ty: Type) -> ColumnDescriptor {
    let ty = SchemaType::primitive_type_builder("col", primitive_ty)
        .with_length(type_length)
        .build()
        .unwrap();
    ColumnDescriptor::new(Arc::new(ty), 0, 0, ColumnPath::new(vec![]))
}
