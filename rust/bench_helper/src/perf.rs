//! Measures the selected perf events using the perf interface of the Linux kernel.
//!

use criterion::{
    measurement::{Measurement, ValueFormatter},
    Throughput,
};
use std::cell::RefCell;

use perfcnt::{
    linux::{PerfCounter, PerfCounterBuilderLinux, HardwareEventType},
    AbstractPerfCounter
};

const PERF_ERR: &str = r#"Unable to bind to perf capabilites

This is probably due to permissions, if this is an issue then setting the perf permissions
in `/proc/sys/kernel/perf_event_paranoid` will enable this benchmark. This can
be done like so

```
echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```

For more details see:
https://www.kernel.org/doc/html/latest/admin-guide/perf-security.html

Otherwise this benchmark measurement will be skipped
"#;

/// `perf` implements `criterion::measurement::Measurement` so it can be used in criterion to measure perf events.
/// Create a struct via `Perf::new()`.
pub struct Perf {
    units: &'static str,
    counter: RefCell<PerfCounter>,
}

impl Perf {
    /// Creates a new criterion measurement plugin that measures perf events.
    pub fn new(units: &'static str, mut builder: PerfCounterBuilderLinux) -> Option<Self> {
        let measure = builder
            .for_pid(std::process::id() as i32)
            .disable()
            .finish()
            .map(RefCell::new)
            .map(|counter| Perf { units, counter });

        match measure {
            Ok(m) => Some(m),
            Err(e) => {
                eprintln!("{}\nReason:{:?}", PERF_ERR, e);
                None
            }
        }
    }

    pub fn hardware(units: &'static str, event: HardwareEventType) -> Option<Self> {
        Self::new(units, PerfCounterBuilderLinux::from_hardware_event(event))
    }
}

impl Measurement for Perf {
    type Intermediate = u64;
    type Value = u64;

    fn start(&self) -> Self::Intermediate {
        self.counter
            .borrow()
            .start()
            .expect("Could not read perf counter");
        0
    }

    fn end(&self, _i: Self::Intermediate) -> Self::Value {
        self.counter
            .borrow()
            .stop()
            .expect("Could not stop perf counter");
        let ret = self
            .counter
            .borrow_mut()
            .read()
            .expect("Could not read perf counter");
        self.counter
            .borrow_mut()
            .reset()
            .expect("Could not reset perf counter");
        ret
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        v1 + v2
    }

    fn zero(&self) -> Self::Value {
        0
    }

    fn to_f64(&self, value: &Self::Value) -> f64 {
        *value as f64
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        self
    }
}

impl ValueFormatter for Perf {
    fn format_value(&self, value: f64) -> String {
        format!("{:.4} {}", value, &self.units)
    }

    fn format_throughput(&self, throughput: &Throughput, value: f64) -> String {
        match throughput {
            Throughput::Bytes(b) => format!("{:.4} {}/byte", value / *b as f64, self.units),
            Throughput::Elements(b) => format!("{:.4} {}/element", value / *b as f64, self.units),
        }
    }

    fn scale_values(&self, _typical_value: f64, _values: &mut [f64]) -> &'static str {
        self.units
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
                "events/byte"
            }
            Throughput::Elements(n) => {
                for val in values {
                    *val /= *n as f64;
                }
                "events/element"
            }
        }
    }

    fn scale_for_machines(&self, _values: &mut [f64]) -> &'static str {
        "events"
    }
}
