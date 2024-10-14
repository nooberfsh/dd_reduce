use differential_dataflow::difference::{IsZero, Multiply, Semigroup};
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::trace::TraceReader;
use random_fast_rng::{FastRng, Random};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use timely::dataflow::operators::Probe;
use timely::dataflow::ProbeHandle;
use timely::progress::frontier::AntichainRef;
use timely::Config;

use crate::dump_trace;

pub fn min_naive(config: Config) {
    timely::execute(config, move |worker| {
        let mut probe = ProbeHandle::new();
        let (mut input, mut trace) = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, i64), _>();
            let arrange = input
                .arrange_by_key()
                .reduce_abelian::<_, _, _, ValSpine<_, _, u32, _>>("Count", |_k, s, t| {
                    assert!(s[0].1 > 0);
                    t.push((s[0].0.clone(), 1))
                });
            arrange.stream.probe_with(&mut probe);
            (handle, arrange.trace)
        });

        let round = 10000;
        let batch = 10000;

        let mut rng = FastRng::new();
        let mut res: i64 = 0;
        for i in 0..round {
            let timer = Instant::now();
            for _ in 0..batch {
                let d: i32 = rng.gen();
                input.update(("a".to_string(), d as i64), 1);
                if (d as i64) < res {
                    res = d as i64;
                }
            }
            trace.set_physical_compaction(AntichainRef::new(&[*input.time()]));
            trace.set_logical_compaction(AntichainRef::new(&[*input.time()]));
            crate::sync!(input, probe, worker);

            let elapsed = timer.elapsed();
            println!("round[{i}]: time: {:?}", elapsed);
        }

        println!("min: {res}");
        dump_trace(&mut trace);
    })
    .unwrap();
}

// (K, V, T, Diff) 必须满足 Diff > 0
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Debug, Copy, Serialize, Deserialize)]
struct Min(i64);

impl Multiply<i64> for Min {
    type Output = Self;
    fn multiply(self, rhs: &i64) -> Self::Output {
        assert!(*rhs > 0);
        self
    }
}

impl IsZero for Min {
    fn is_zero(&self) -> bool {
        false
    }
}

impl Semigroup for Min {
    fn plus_equals(&mut self, rhs: &Self) {
        if rhs.0 < self.0 {
            self.0 = rhs.0
        }
    }
}

// NOTE: insert 的数据 diff 部分必须 > 0
pub fn min_explode(config: Config) {
    timely::execute(config, move |worker| {
        let mut probe = ProbeHandle::new();
        let (mut input, mut trace) = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, i64), _>();
            let arrange = input
                .explode(|(k, v)| Some((k, Min(v))))
                .arrange_by_self()
                .reduce_abelian::<_, _, _, ValSpine<_, _, u32, _>>("Count", |_k, s, t| {
                    assert_eq!(s.len(), 1);
                    t.push((s[0].1 .0.clone(), 1))
                });
            arrange.stream.probe_with(&mut probe);
            (handle, arrange.trace)
        });

        let round = 10_000;
        let batch = 10000;

        let mut rng = FastRng::new();
        let mut res: i64 = 0;
        for i in 0..round {
            let timer = Instant::now();
            for _ in 0..batch {
                let d: i32 = rng.gen();
                input.update(("a".to_string(), d as i64), 1);
                if (d as i64) < res {
                    res = d as i64;
                }
            }
            trace.set_physical_compaction(AntichainRef::new(&[*input.time()]));
            trace.set_logical_compaction(AntichainRef::new(&[*input.time()]));
            crate::sync!(input, probe, worker);

            let elapsed = timer.elapsed();
            println!("round[{i}]: time: {:?}", elapsed);
        }

        println!("min: {res}");
        dump_trace(&mut trace);
    })
    .unwrap();
}
