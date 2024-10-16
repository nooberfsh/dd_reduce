pub mod min;
pub mod sum;
pub mod util;

pub use crate::min::*;
pub use crate::sum::*;
pub use crate::util::*;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::{Count, CountTotal};
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::TraceReader;
use itertools::Itertools;
use random_fast_rng::{FastRng, Random};
use std::collections::HashMap;
use std::fmt::Display;
use std::time::Instant;
use timely::dataflow::operators::Probe;
use timely::dataflow::ProbeHandle;
use timely::progress::frontier::AntichainRef;

#[macro_export]
macro_rules! sync {
    ($input:expr, $probe:expr, $worker:expr) => {{
        let time = $input.time().clone() + 1;
        $input.advance_to(time);
        $input.flush();
        $worker.step_while(|| $probe.less_than(&time));
    }};
}

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    //count_output();
    //count_shape(td_config);
    //sum_naive();
    //sum_explode();
    //sum_explode_bug()
    //sum_explode_fix()
    //min_naive()
    //count_total_trace_import()
    //bench_hashmap()
    //min_explode()
    //min_naive();
    //min_partition();
    min_optimize();
}

// count 输出的形状
pub fn count_output() {
    timely::execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();
        let mut input = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection();
            input
                .count()
                .inspect(|((k, v), t, r)| println!("key: {k}, count: {v}, time: {t}, diff: {r}"))
                .probe_with(&mut probe);
            handle
        });

        for i in 1..5 {
            for _ in 0..i {
                input.insert("a".to_string());
            }
            sync!(input, probe, worker);
        }
    })
}

fn count_total_trace_import() {
    timely::execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();
        let (mut input, mut trace) = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection();
            let arrange = input.arrange_by_self();
            arrange.stream.probe_with(&mut probe);
            (handle, arrange.trace)
        });
        input.insert("a".to_string());
        sync!(input, probe, worker);
        input.insert("a".to_string());
        sync!(input, probe, worker);
        input.insert("a".to_string());
        sync!(input, probe, worker);

        let time = *input.time();
        println!("current frontier: {time}");
        trace.set_logical_compaction(AntichainRef::new(&[2]));
        trace.set_physical_compaction(AntichainRef::new(&[2]));

        println!("{:?}", trace.get_logical_compaction());
        println!("{:?}", trace.get_physical_compaction());

        worker.dataflow::<u32, _, _>(|scope| {
            let arrange = trace.import(scope);
            arrange.count_total().inspect(|d| println!("{:?}", d));
        });
    })
}

fn bench_hashmap() {
    let round = 100000;
    let batch = 100000;

    let mut map = HashMap::new();
    let mut rng = FastRng::new();
    for i in 0..round {
        let timer = Instant::now();
        for _ in 0..batch {
            let d: u64 = rng.gen();
            let entry = map.entry(d).or_insert(0);
            *entry += 1;
        }
        let elapsed = timer.elapsed();
        println!("round[{i}]: time: {:?}", elapsed);
    }
}

fn dump_trace<Tr, K: Display, V: Display>(trace: &mut Tr)
where
    Tr: TraceReader,
    Tr::Diff: Display,
    Tr::Time: Display,
    for<'a> Tr::Key<'a>: IntoOwned<'a, Owned = K>,
    for<'a> Tr::Val<'a>: IntoOwned<'a, Owned = V>,
{
    let data = trace_to_vec_val_trace(trace);
    for ((k, v), updates) in data {
        let update_desc = updates.iter().map(|(x, y)| format!("({x}, {y})")).join(",");
        println!("trace: [{k}, {v}]:  {}", update_desc)
    }
}
