pub mod min_max;
pub mod util;

use std::fmt::Display;
use std::time::Instant;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::operators::CountTotal;
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::trace::TraceReader;
use itertools::Itertools;
use random_fast_rng::{FastRng, Random};
use timely::dataflow::operators::Probe;
use timely::dataflow::ProbeHandle;
use timely::progress::frontier::AntichainRef;
use timely::Config;

use crate::util::trace_to_vec_val_trace;

macro_rules! sync {
    ($input:expr, $probe:expr, $worker:expr) => {{
        let time = $input.time().clone() + 1;
        $input.advance_to(time);
        $input.flush();
        $worker.step_while(|| $probe.less_than(&time));
    }};
}

fn main() {
    let mut td_config = Config::process(1);
    let dd_config = differential_dataflow::Config {
        idle_merge_effort: Some(1000),
    };
    differential_dataflow::configure(&mut td_config.worker, &dd_config);
    //count_shape(td_config);
    //sum_naive(td_config);
    sum_explode(td_config);
}

// 最 naive 的使用方式, 随着单个 key 的 value 数量增加, reduce 会越来约慢, 因为每次计算的数量都在增加.
pub fn sum_naive(config: Config) {
    timely::execute(config, move |worker| {
        let mut probe = ProbeHandle::new();
        let (mut input, mut trace) = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, i64), _>();
            let arrange = input
                .arrange_by_key()
                .reduce_abelian::<_, _, _, ValSpine<_, _, u32, _>>("x", |_key, input, output| {
                    let sum: i64 = input.iter().map(|(v, diff)| (**v) * (*diff as i64)).sum();
                    output.push((sum, 1));
                });
            arrange
                .as_collection(|k: &String, c: &i64| (k.clone(), c.clone()))
                //.inspect(|x| println!("{:?}", x))
                .probe_with(&mut probe);
            (handle, arrange.trace)
        });

        let round = 100;
        let batch = 10000;

        let mut rng = FastRng::new();
        let mut res: i64 = 0;
        for i in 0..round {
            let timer = Instant::now();
            for _ in 0..batch {
                let d: i32 = rng.gen();
                input.update(("a".to_string(), d as i64), 1);
                res += d as i64;
            }
            trace.set_physical_compaction(AntichainRef::new(&[*input.time()]));
            trace.set_logical_compaction(AntichainRef::new(&[*input.time()]));
            sync!(input, probe, worker);
            let elapsed = timer.elapsed();
            println!("round[{i}]: time: {:?}", elapsed);
        }

        println!("sum: {res}");
        dump_trace(&mut trace);
    })
    .unwrap();
}

// 在 sum_naive 中, 随着单 key 的 value 数量不断增加速度会越来约慢, 因为 reduce 维护着所有 distinct value,
// 但是 sum_naive 的计算中的只需要求一个 sum, 不需要区分 distinct value, sum_explode 把 value 通过 `explode` 挪到 (K, V, T, Diff) 中
// Diff, 利用 arrangement 自动合并相同 (K, V, T) 的 Diff 机制, 把 所有 distinct value 合并到少数几个 value,
// 新的 value 的数量取决于需要维护的 Time 数量, 如果 Time 数量始终保持最新的话, 那 value 的数量只会是 一个.
pub fn sum_explode(config: Config) {
    timely::execute(config, move |worker| {
        let mut probe = ProbeHandle::new();
        let (mut input, mut trace) = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, i64), _>();
            let arrange = input
                .explode(|(k, v)| Some((k, v)))
                .arrange_by_self()
                .reduce_abelian::<_, _, _, ValSpine<_, _, u32, _>>("Count", |_k, s, t| {
                    assert_eq!(s.len(), 1);
                    t.push((s[0].1.clone(), 1))
                });
            arrange.stream.probe_with(&mut probe);
            (handle, arrange.trace)
        });

        let round = 1000;
        let batch = 10000;

        let mut rng = FastRng::new();
        let mut res: i64 = 0;
        for i in 0..round {
            let timer = Instant::now();
            for _ in 0..batch {
                let d: i32 = rng.gen();
                input.update(("a".to_string(), d as i64), 1);
                res += d as i64;
            }
            trace.set_physical_compaction(AntichainRef::new(&[*input.time()]));
            trace.set_logical_compaction(AntichainRef::new(&[*input.time()]));
            sync!(input, probe, worker);
            let elapsed = timer.elapsed();
            println!("round[{i}]: time: {:?}", elapsed);
        }

        println!("sum: {res}");
        dump_trace(&mut trace);
    })
    .unwrap();
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
