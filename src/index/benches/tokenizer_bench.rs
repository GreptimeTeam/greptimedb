// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use index::fulltext_index::tokenizer::{EnglishTokenizer, Tokenizer};

fn bench_english_tokenizer(c: &mut Criterion) {
    let tokenizer = EnglishTokenizer;

    let texts = vec![
        ("short", "Hello, world! This is a test."),
        ("medium", "The quick brown fox jumps over the lazy dog. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."),
        ("long", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt."),
        ("mixed_chars", "Hello123 world!!! This-is_a.test@example.com with various: punctuation; and [brackets] {curly} (parentheses) & symbols* + numbers456."),
        ("numbers_heavy", "test123 456test test789 abc123def 999888777 hello42world 123 456 789 mix1ng l3tt3rs 4nd numb3rs"),
        ("punctuation_heavy", "Hello!!! World??? This...is...a...test... With lots of!!! punctuation??? marks!!! And... ellipses???"),
        ("postgres log", "2025-08-01 21:09:28.928 UTC [27] LOG:  checkpoint complete: wrote 0 buffers (0.0%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.001 s, sync=0.001 s, total=0.003 s; sync files=0, longest=0.000 s, average=0.000 s; distance=0 kB, estimate=5 kB; lsn=0/1992868, redo lsn=0/1992868"),
        ("many_short_words", "a b c d e f g h i j k l m n o p q r s t u v w x y z"),
        ("with_unicode", "这是，一个测试。🈶一些 Unicøde 字符比如 café and naïve words."),
    ];

    let mut group = c.benchmark_group("english_tokenizer");

    for (size, text) in texts {
        group.bench_with_input(BenchmarkId::new("tokenize", size), &text, |b, text| {
            b.iter(|| tokenizer.tokenize(text))
        });
    }

    group.finish();

    // Benchmark with repeated tokenization to simulate real-world usage
    let mut repeat_group = c.benchmark_group("english_tokenizer_repeated");

    let sample_text = "The quick brown fox jumps over the lazy dog. This sentence contains most letters of the alphabet.";

    for repeat_count in [10, 100, 1000] {
        repeat_group.bench_with_input(
            BenchmarkId::new("repeated_tokenize", repeat_count),
            &repeat_count,
            |b, &repeat_count| {
                b.iter(|| {
                    for _ in 0..repeat_count {
                        tokenizer.tokenize(sample_text);
                    }
                })
            },
        );
    }

    repeat_group.finish();
}

criterion_group!(benches, bench_english_tokenizer);
criterion_main!(benches);
