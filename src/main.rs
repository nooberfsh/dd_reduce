pub mod min_max;
mod sum;
pub mod util;

use std::fmt::Display;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::{Count, CountTotal};
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::TraceReader;
use itertools::Itertools;
use timely::dataflow::ProbeHandle;
use timely::progress::frontier::AntichainRef;
use timely::Config;

pub use crate::min_max::*;
pub use crate::sum::*;
pub use crate::util::*;

#[macro_export]
macro_rules! sync {
    ($input:expr, $probe:expr, $worker:expr) => {{
        let time = $input.time().clone() + 1;
        $input.advance_to(time);
        $input.flush();
        $worker.step_while(|| $probe.less_than(&time));
    }};
}

fn main() {
    //count_output();
    //count_shape(td_config);
    //sum_naive();
    //sum_explode();
    //sum_explode_bug()
    //sum_explode_fix()
    min_explode()
    //min_naive(td_config);
    //min_explode(td_config);
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

// 查看 reduce 之后数据在 trace 中的形状.
pub fn count_shape(config: Config) {
    timely::execute(config, move |worker| {
        let mut probe = ProbeHandle::new();
        let (mut input, mut trace) = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection();
            let arrange = input.count_total().arrange_by_key();
            arrange
                .as_collection(|k: &String, c: &isize| (k.clone(), c.clone()))
                //.inspect(|x| println!("{:?}", x))
                .probe_with(&mut probe);
            (handle, arrange.trace)
        });

        for i in 0..100 {
            println!("************* round {i} *******************");
            input.insert("a".to_string());
            input.insert("a".to_string());

            sync!(input, probe, worker);
            dump_trace(&mut trace);
        }

        let time = *input.time() - 1;
        trace.set_logical_compaction(AntichainRef::new(&[time]));
        trace.set_physical_compaction(AntichainRef::new(&[time]));
        worker.step();
        worker.step();
        worker.step();
        worker.step();
        worker.step();
        println!("|||||||||||| compact |||||||||||||");
        dump_trace(&mut trace);
    })
    .unwrap();
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
