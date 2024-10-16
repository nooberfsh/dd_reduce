use differential_dataflow::difference::{IsZero, Multiply, Semigroup};
use differential_dataflow::input::Input;
use differential_dataflow::operators::Count;
use serde::{Deserialize, Serialize};
use timely::dataflow::ProbeHandle;
use timely::execute_directly;

use crate::sync;

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
pub fn min_explode() {
    execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();
        let mut input = worker.dataflow::<u32, _, _>(|scope| {
            let (handle, input) = scope.new_collection::<(String, i64), _>();
            input
                .explode(|(k, v)| Some((k, Min(v))))
                .count()
                .map(|(k, v)| (k, v.0))
                .inspect(|s| println!("{:?}", s))
                .probe_with(&mut probe);
            handle
        });
        input.update(("a".to_string(), 1 as i64), 1);
        input.update(("a".to_string(), -1), 1);
        input.update(("a".to_string(), 2), 1);
        sync!(input, probe, worker);
    })
}
