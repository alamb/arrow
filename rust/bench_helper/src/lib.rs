//! Contains macros which together define a benchmark harness that can be used
//! in place of the standard benchmark harness. This allows the user to run
//! Criterion.rs benchmarks with `cargo bench`.

// Rexport criterion and deps
pub use criterion::*;
pub use criterion::measurement::Measurement;

pub use stats_alloc;

#[cfg(all(target_arch="x86_64", target_os="linux"))]
pub use perfcnt;

#[cfg(all(target_arch="x86_64", target_os="linux"))]
pub mod perf;

pub mod alloc;

/// Macro used to define a function group for the benchmark harness; see the
/// `bench_main!` macro for more details.
///
/// This is used to define a function group; a collection of functions to call with common
/// Criterion configurations adapted for multiple measurements.
///
/// Accepts two forms which can be seen below.
///
/// Note that the group name given here is not important, it must simply
/// be unique.
/// Note also that this macro is not related to the `Criterion::benchmark_group` function or the
/// `BenchmarkGroup` type.
///
/// # Examples:
///
/// Complete form:
///
/// ```
/// # use bench_helper::*;
/// # fn bench_method1<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
/// # }
/// #
/// # fn bench_method2<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
/// # }
/// #
/// bench_group!{
///     name = benches;
///     // NOTE: This will have its measurement adjusted while running the suite
///     config = Criterion::default();
///     targets = bench_method1, bench_method2
/// }
/// #
/// # fn main() {}
/// ```
///
/// In this form, all of the options are clearly spelled out. This expands to
/// a function named benches, which uses the given config expression to create
/// an instance of the Criterion struct. This is then passed by mutable
/// reference to the targets.
///
/// Compact Form:
///
/// ```
/// # use bench_helper::*;
/// # fn bench_method1<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
/// # }
/// #
/// # fn bench_method2<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
/// # }
/// #
/// bench_group!(benches, bench_method1, bench_method2);
/// #
/// # fn main() {}
/// ```
/// In this form, the first parameter is the name of the group and subsequent
/// parameters are the target methods. The Criterion struct will be created using
/// the `Criterion::default()` function. If you wish to customize the
/// configuration, use the complete form and provide your own configuration
/// function.
#[macro_export]
macro_rules! bench_group {
    (name = $name:ident; config = $config:expr; targets = $( $target:path ),+ $(,)*) => {
        pub fn $name(measure_name: &str, measure: impl $crate::Measurement) {
            let mut criterion: $crate::Criterion<_> = $config
                .noise_threshold(0.03)
                .with_measurement(measure)
                .configure_from_args();
            $(
                $target(measure_name, &mut criterion);
            )+
        }
    };
    ($name:ident, $( $target:path ),+ $(,)*) => {
        bench_group!{
            name = $name;
            config = $crate::Criterion::default();
            targets = $( $target ),+
        }
    }
}

/// Macro which expands to a benchmark harness.
///
/// Currently, using Criterion.rs requires disabling the benchmark harness
/// generated automatically by rustc. This can be done like so:
///
/// We expand on criterion and allow for capturing multiple interesting measurements.
///
/// ```toml
/// [[bench]]
/// name = "my_bench"
/// harness = false
/// ```
///
/// In this case, `my_bench` must be a rust file inside the 'benches' directory,
/// like so:
///
/// `benches/my_bench.rs`
///
/// Since we've disabled the default benchmark harness, we need to add our own:
///
/// ```ignore
/// # use bench_helper::*;
/// # fn bench_method1<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
/// # }
/// #
/// # fn bench_method2<M: Measurement>(measure_name: &str, c: &mut Criterion<M>) {
/// # }
///
/// bench_group!(benches, bench_method1, bench_method2);
/// bench_main!(benches);
/// ```
///
/// The `bench_main` macro expands to a `main` function which runs all of the
/// benchmarks in the given groups. For the following measurements:
///
/// * Wall time
/// * Cache misses (Optional as recorded by linux perf)
/// * Branch misses (Optional as recorded by linux perf)
/// * Normalised CPU Cycles (Optional as recorded by linux perf)
/// * Allocation rate
/// * Reallocation rate
///
/// This macro can be altered to add extra stats if needed
#[macro_export]
macro_rules! bench_main {
    ( $( $group:path ),+ $(,)* ) => {
        use $crate::stats_alloc::{StatsAlloc, Region, INSTRUMENTED_SYSTEM};
        use ::std::alloc::System;

        #[global_allocator]
        static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

        fn main() {
            let wall_time = $crate::measurement::WallTime;
            $( $group("wall_time", wall_time); )+

            let allocs = $crate::alloc::Alloc::allocations(&GLOBAL);
            $( $group("allocs", allocs); )+

            let reallocs = $crate::alloc::Alloc::reallocations(&GLOBAL);
            $( $group("reallocs", reallocs); )+

            if cfg!(all(target_arch="x86_64", target_os="linux")) {
                use $crate::perfcnt::linux::{HardwareEventType as Hardware};
                use $crate::perf::Perf;

                if let Some(cpu_cycles) = Perf::hardware("cycles", Hardware::RefCPUCycles) {
                    $( $group("cpu_cycles", cpu_cycles); )+
                }

                if let Some(stalled_cpu_cycles) = Perf::hardware("stalled_fe_cycles", Hardware::StalledCyclesFrontend) {
                    $( $group("stalled_fe_cycles", stalled_cpu_cycles); )+
                }

                if let Some(stalled_cpu_cycles) = Perf::hardware("stalled_be_cycles", Hardware::StalledCyclesBackend) {
                    $( $group("stalled_be_cycles", stalled_be_cycles); )+
                }

                if let Some(cache_misses) = Perf::hardware("cache misses", Hardware::CacheMisses) {
                    $( $group("cache_misses", cache_misses); )+
                }

                if let Some(branch_misses) = Perf::hardware("branch misses", Hardware::BranchMisses) {
                    $( $group("branch_misses", branch_misses); )+
                }
            }

            $crate::Criterion::default()
                .configure_from_args()
                .final_summary();
        }
    }
}
