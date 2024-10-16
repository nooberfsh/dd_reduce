use std::time::Instant;

use differential_dataflow::difference::{IsZero, Multiply, Semigroup};
use differential_dataflow::input::Input;
use differential_dataflow::operators::{Count, Reduce};
use random_fast_rng::{FastRng, Random};
use serde::{Deserialize, Serialize};
use timely::dataflow::ProbeHandle;
use timely::execute_directly;

use crate::sync;

// 最 naive 的使用方式, 随着单个 key 的 value 数量增加, reduce 会越来约慢, 因为每次计算的数量都在增加.
pub fn sum_naive() {
    execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();
        let mut input = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, i64), _>();
            input
                .reduce(|_key, input, output| {
                    let sum: i64 = input.iter().map(|(v, diff)| (**v) * (*diff as i64)).sum();
                    output.push((sum, 1));
                })
                .probe_with(&mut probe);
            handle
        });

        let round = 10000;
        let batch = 10000;

        let mut rng = FastRng::new();
        for i in 0..round {
            let timer = Instant::now();
            for _ in 0..batch {
                let d: i32 = rng.gen();
                input.update(("a".to_string(), d as i64), 1);
            }
            sync!(input, probe, worker);
            let elapsed = timer.elapsed();
            println!("round[{i}]: time: {:?}", elapsed);
        }
    })
}

// 在 sum_naive 中, 随着单 key 的 value 数量不断增加速度会越来约慢, 因为 reduce 维护着所有 distinct value,
// 但是 sum_naive 的计算中的只需要求一个 sum, 不需要区分 distinct value, sum_explode 把 value 通过 `explode` 挪到 (K, V, T, Diff) 中
// Diff, 利用 arrangement 自动合并相同 (K, V, T) 的 Diff 机制, 把 所有 distinct value 合并到少数几个 value,
// 新的 value 的数量取决于需要维护的 Time 数量, 如果 Time 数量始终保持最新的话, 那 value 的数量只会是 一个.
// NOTE: 这里没有无法区 value = 0 / diff = 0 的情况
pub fn sum_explode() {
    execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();
        let mut input = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, i64), _>();
            input
                .explode(|(k, v)| Some((k, v)))
                .count()
                .probe_with(&mut probe);
            handle
        });

        let round = 10000;
        let batch = 10000;

        let mut rng = FastRng::new();
        for i in 0..round {
            let timer = Instant::now();
            for _ in 0..batch {
                let d: i32 = rng.gen();
                input.update(("a".to_string(), d as i64), 1);
            }
            sync!(input, probe, worker);
            let elapsed = timer.elapsed();
            println!("round[{i}]: time: {:?}", elapsed);
        }
    })
}

pub fn sum_explode_bug() {
    execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();
        let mut input = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, i64), _>();
            input
                .explode(|(k, v)| Some((k, v)))
                .count()
                .inspect(|s| println!("{:?}", s))
                .probe_with(&mut probe);
            handle
        });

        input.update(("a".to_string(), 1 as i64), 1);
        input.update(("a".to_string(), -1), 1);

        sync!(input, probe, worker);
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Sum {
    value: i64,
    count: i64,
}

impl IsZero for Sum {
    fn is_zero(&self) -> bool {
        self.count == 0 && self.value == 0
    }
}

impl Semigroup for Sum {
    fn plus_equals(&mut self, rhs: &Self) {
        self.value += rhs.value;
        self.count += rhs.count;
    }
}

impl Multiply<isize> for Sum {
    type Output = Self;
    fn multiply(self, rhs: &isize) -> Self::Output {
        assert_eq!(self.count, 1);
        Sum {
            value: self.value * (*rhs as i64),
            count: *rhs as i64,
        }
    }
}

pub fn sum_explode_fix() {
    execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();
        let mut input = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, i64), _>();
            input
                .explode(|(k, v)| Some((k, Sum { value: v, count: 1 })))
                .count()
                .map(|(k, v)| (k, v.value))
                .inspect(|s| println!("{:?}", s))
                .probe_with(&mut probe);
            handle
        });
        input.update(("a".to_string(), 1 as i64), 1);
        input.update(("a".to_string(), -1), 1);

        sync!(input, probe, worker);

        input.update(("b".to_string(), 1 as i64), 1);
        input.update(("b".to_string(), 2), -1);
        sync!(input, probe, worker);
    })
}
