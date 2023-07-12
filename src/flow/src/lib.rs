use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Count;
use timely::progress::timestamp::Refines;

#[test]
fn demo() {
    use abomonation_derive::Abomonation;
    use differential_dataflow::input::InputSession;
    use differential_dataflow::operators::Join;
    #[derive(Debug, Clone, Default, Eq, PartialEq, Hash, Abomonation)]
    /// (System, Event)
    struct MT(usize, usize);
    impl Ord for MT {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.0.cmp(&other.0).then(self.1.cmp(&other.1))
        }
    }
    impl PartialOrd for MT {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.cmp(other).into()
        }
    }
    impl timely::PartialOrder for MT {
        fn less_equal(&self, other: &Self) -> bool {
            self.0 <= other.0 && self.1 <= other.1
        }
    }
    impl timely::order::TotalOrder for MT {}
    impl timely::progress::Timestamp for MT {
        type Summary = MT;
        fn minimum() -> Self {
            Self(0, 0)
        }
    }
    impl timely::progress::PathSummary<MT> for MT {
        fn results_in(&self, src: &MT) -> Option<MT> {
            self.0
                .results_in(&src.0)
                .and_then(|x| self.1.results_in(&src.1).map(|y| MT(x, y)))
            //.and_then(|x| self.1.results_in(&src.1).map(|y| MT(x, y)))
        }

        fn followed_by(&self, other: &Self) -> Option<Self> {
            self.0
                .followed_by(&other.0)
                .and_then(|x| self.1.followed_by(&other.1).map(|y| MT(x, y)))
        }
    }
    impl Refines<()> for MT {
        fn to_inner(other: ()) -> Self {
            Self(0, 0)
        }

        fn to_outer(self) -> () {
            todo!()
        }

        fn summarize(
            path: <Self as timely::progress::Timestamp>::Summary,
        ) -> <() as timely::progress::Timestamp>::Summary {
            todo!()
        }
    }
    impl Lattice for MT {
        fn join(&self, other: &Self) -> Self {
            Self(self.0.max(other.0), self.1.max(other.1))
        }

        fn meet(&self, other: &Self) -> Self {
            Self(self.0.min(other.0), self.1.min(other.1))
        }
    }

    // define a new timely dataflow computation.
    timely::execute_from_args(["w2".to_string()].into_iter(), move |worker| {
        // create an input collection of data.
        let mut input = InputSession::new();

        // define a new computation.
        let probe = worker.dataflow(|scope| {
            // create a new collection from our input.
            let manages = input.to_collection(scope);

            // if (m2, m1) and (m1, p), then output (m1, (m2, p))
            manages
                .map(|(m2, m1)| (m1, m2))
                .join(&manages)
                .count()
                //.inspect(|x| println!("{:?}", x))
                .probe()
        });

        // Read a size for our organization from the arguments.
        let size = 100;

        // Load input (a binary tree).
        input.advance_to(MT(0, 0));
        let mut person = worker.index();
        while person < size {
            input.insert((person / 2, person));
            person += worker.peers();
        }

        // wait for data loading.
        input.advance_to(MT(0, 0));
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }
        println!("{:?}\tdata loaded", worker.timer().elapsed());

        let mut person = 1 + worker.index();
        while person < size {
            input.remove((person / 2, person));
            input.insert((person / 3, person));
            input.advance_to(MT(0, person));
            input.flush();
            while probe.less_than(&input.time()) {
                worker.step();
            }
            println!("{:?}\tstep {} complete", worker.timer().elapsed(), person);
            person += worker.peers();
        }
    })
    .expect("Computation terminated abnormally");
}
