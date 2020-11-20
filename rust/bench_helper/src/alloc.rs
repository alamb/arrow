//! Measures rough allocation rate

use criterion::{
    measurement::{Measurement, ValueFormatter},
    Throughput,
};
use stats_alloc::{Region, StatsAlloc};
use std::cell::RefCell;
use std::alloc::System;

#[derive(Copy, Clone, Debug)]
enum Measure {
    Allocations,
    Deallocations,
    Reallocations,
}

impl Measure {
    fn name(&self) -> &'static str {
        match self {
            Self::Allocations => "allocations",
            Self::Deallocations => "deallocations",
            Self::Reallocations => "reallocations",
        }
    }

    fn throughput_name(&self, throughput: &Throughput) -> &'static str {
        match (self, throughput) {
            (Self::Allocations, Throughput::Bytes(_)) => "allocations/byte",
            (Self::Deallocations, Throughput::Bytes(_)) => "deallocations/byte",
            (Self::Reallocations, Throughput::Bytes(_)) => "reallocations/byte",
            (Self::Allocations, Throughput::Elements(_)) => "allocations/element",
            (Self::Deallocations, Throughput::Elements(_)) => "deallocations/element",
            (Self::Reallocations, Throughput::Elements(_)) => "reallocations/element",
        }
    }
}

/// `alloc` implements `criterion::measurement::Measurement` so it can be used in criterion to measure allocs.
/// Create a struct via `Alloc::new()`.
pub struct Alloc {
    region: RefCell<Region<'static, System>>,
    sub_measure: Measure,
}

impl Alloc {
    fn new(alloc: &'static StatsAlloc<System>, sub_measure: Measure) -> Self {
        Self { sub_measure, region: RefCell::from(Region::new(alloc)) }
    }

    pub fn allocations(alloc: &'static StatsAlloc<System>) -> Self {
        Self::new(alloc, Measure::Allocations)
    }

    pub fn dellocations(alloc: &'static StatsAlloc<System>) -> Self {
        Self::new(alloc, Measure::Deallocations)
    }

    pub fn reallocations(alloc: &'static StatsAlloc<System>) -> Self {
        Self::new(alloc, Measure::Reallocations)
    }
}

impl Measurement for Alloc {
    type Intermediate = usize;
    type Value = usize;

    fn start(&self) -> Self::Intermediate {
        let stats = self.region
            .borrow()
            .change();

        match self.sub_measure {
            Measure::Allocations => stats.allocations,
            Measure::Deallocations => stats.deallocations,
            Measure::Reallocations => stats.reallocations,
        }
    }

    fn end(&self, _i: Self::Intermediate) -> Self::Value {
        let stats = self.region
            .borrow()
            .change();
        match self.sub_measure {
            Measure::Allocations => stats.allocations,
            Measure::Deallocations => stats.deallocations,
            Measure::Reallocations => stats.reallocations,
        }
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        v1 + v2
    }

    fn zero(&self) -> Self::Value {
        0
    }

    fn to_f64(&self, value: &Self::Value) -> f64 {
        // HACK: Add a _very_ small amount of error to avoid propagating 0 through benchmarking
        // and thus producing NaNs
        (*value as f64) + 0.0001
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        self
    }
}

impl ValueFormatter for Alloc {
    fn format_value(&self, value: f64) -> String {
        format!("{:.4} {}", value, self.sub_measure.name())
    }

    fn format_throughput(&self, throughput: &Throughput, value: f64) -> String {
        match throughput {
            Throughput::Bytes(b) => format!("{:.4} {}/byte", value / *b as f64, self.sub_measure.name()),
            Throughput::Elements(b) => format!("{:.4} {}/element", value / *b as f64, self.sub_measure.name()),
        }
    }

    fn scale_values(&self, _typical_value: f64, _values: &mut [f64]) -> &'static str {
        self.sub_measure.name()
    }

    fn scale_throughputs(
        &self,
        _typical_value: f64,
        throughput: &Throughput,
        values: &mut [f64],
    ) -> &'static str {
        match throughput {
            Throughput::Bytes(n) => {
                for val in values {
                    *val /= *n as f64;
                }
                self.sub_measure.throughput_name(throughput)
            }
            Throughput::Elements(n) => {
                for val in values {
                    *val /= *n as f64;
                }
                self.sub_measure.throughput_name(throughput)
            }
        }
    }

    fn scale_for_machines(&self, _values: &mut [f64]) -> &'static str {
        self.sub_measure.name()
    }
}
