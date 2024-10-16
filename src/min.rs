use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use differential_dataflow::difference::{IsZero, Multiply, Semigroup};
use differential_dataflow::input::Input;
use differential_dataflow::operators::{Count, Reduce};
use random_fast_rng::{FastRng, Random};
use serde::{Deserialize, Serialize};
use timely::dataflow::ProbeHandle;
use timely::{execute, execute_directly, Config};
use timelyext::operators::min_total::MinTotal;

use crate::sync;

const ROUND: usize = 2_000;
const BATCH: usize = 10_000;

// 最 naive 的使用方式, 随着单个 key 的 value 数量增加, reduce 会越来越慢, 因为每次计算的数量都在增加.
pub fn min_naive() {
    execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();
        let mut input = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, u64), _>();
            input
                .reduce(|_key, input, output| {
                    assert!(input[0].1 > 0);
                    output.push((*input[0].0, 1));
                })
                .probe_with(&mut probe);
            handle
        });

        let mut rng = FastRng::new();
        let mut min = u64::MAX;
        for i in 0..ROUND {
            let timer = Instant::now();
            for _ in 0..BATCH {
                let d: u64 = rng.gen();
                input.update(("a".to_string(), d), 1);
                if d < min {
                    min = d;
                }
            }
            sync!(input, probe, worker);
            let elapsed = timer.elapsed();
            println!("round[{i}]: min: {min}, time: {:?}", elapsed);
        }
    })
}

pub fn min_partition() {
    execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();
        let mut input = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, u64), _>();
            input
                .map(|(a, b)| ((a.to_string(), b >> 48), b))
                .reduce(|_key, input, output| {
                    assert!(input[0].1 > 0);
                    output.push((*input[0].0, 1));
                })
                .map(|(a, b)| (a.0, b))
                .reduce(|_key, input, output| {
                    assert!(input[0].1 > 0);
                    output.push((*input[0].0, 1));
                })
                .probe_with(&mut probe);
            handle
        });

        let mut rng = FastRng::new();
        let mut min = u64::MAX;
        for i in 0..ROUND {
            let timer = Instant::now();
            for _ in 0..BATCH {
                let d: u64 = rng.gen();
                input.update(("a".to_string(), d), 1);
                if d < min {
                    min = d;
                }
            }
            sync!(input, probe, worker);
            let elapsed = timer.elapsed();
            println!("round[{i}]: min: {min}, time: {:?}", elapsed);
        }
    })
}

// (K, V, T, Diff) 必须满足 Diff > 0
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Debug, Copy, Serialize, Deserialize)]
struct Min(u64);

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
pub fn min_explode() {
    execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();
        let mut input = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, u64), _>();
            input
                .explode(|(k, v)| Some((k, Min(v))))
                .count()
                .map(|(k, v)| (k, v.0))
                .inspect(|s| println!("{:?}", s))
                .probe_with(&mut probe);
            handle
        });

        let mut rng = FastRng::new();
        let mut min = u64::MAX;
        for i in 0..ROUND {
            let timer = Instant::now();
            for _ in 0..BATCH {
                let d: u64 = rng.gen();
                input.update(("a".to_string(), d), 1);
                if d < min {
                    min = d;
                }
            }
            sync!(input, probe, worker);
            let elapsed = timer.elapsed();
            println!("round[{i}]: min: {min}, time: {:?}", elapsed);
        }
    })
}

pub fn min_optimize() {
    let mut td_config = Config::process(1);
    let dd_config = differential_dataflow::Config {
        idle_merge_effort: Some(1),
    };
    differential_dataflow::configure(&mut td_config.worker, &dd_config);

    execute(td_config, move |worker| {
        let mut probe = ProbeHandle::new();
        let mut input = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, u64), _>();
            input
                .min_total()
                .inspect(|s| println!("{:?}", s))
                .probe_with(&mut probe);
            handle
        });

        let mut rng = FastRng::new();
        let mut min = u64::MAX;
        let mut latency = BTreeSet::new();
        for i in 0..ROUND {
            let timer = Instant::now();
            for _ in 0..BATCH {
                let d: u64 = rng.gen();
                input.update(("a".to_string(), d), 1);
                if d < min {
                    min = d;
                }
            }
            sync!(input, probe, worker);
            let elapsed = timer.elapsed();
            println!("round[{i}]: min: {min}, time: {:?}", elapsed);
            if elapsed >= Duration::from_millis(20) {
                latency.insert(elapsed);
            }
        }
        for l in latency {
            println!("latency: {:?}", l)
        }
    })
    .unwrap();
}
