#![feature(test)]

extern crate test;

use std::collections::HashMap;

use test::Bencher;

#[bench]
fn bench_urlencoded(b: &mut Bencher) {
    let s = "db=foo&rp=bar&precision=ns&q=SELECT+*+FROM+cpu+WHERE+time+%3E+now%28%29+-+1h";

    b.iter(|| {
        let n = test::black_box(1000);

        (0..n).fold("foo".to_string(), |_, _| {
            let map = serde_urlencoded::from_str::<HashMap<String, String>>(s).unwrap();
            map.get("db").unwrap().to_string()
        })
    })
}

#[bench]
fn bench_regex(b: &mut Bencher) {
    let s = "db=foo&rp=bar&precision=ns&q=SELECT+*+FROM+cpu+WHERE+time+%3E+now%28%29+-+1h";
    let pattern = regex::Regex::new(r"db=([^&]+)[&]?").unwrap();

    b.iter(|| {
        let n = test::black_box(1000);

        (0..n).fold("foo".to_string(), |_, _| {
            pattern
                .captures(s)
                .unwrap()
                .get(1)
                .unwrap()
                .as_str()
                .to_string()
        })
    })
}
