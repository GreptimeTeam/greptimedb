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

use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::AsyncRead;
use index::fulltext_index::create::{FulltextIndexCreator, TantivyFulltextIndexCreator};
use index::fulltext_index::tokenizer::{ChineseTokenizer, EnglishTokenizer, Tokenizer};
use index::fulltext_index::{Analyzer, Config};
use puffin::puffin_manager::{PuffinWriter, PutOptions};

const CHINESE_TOKENIZER_TEXTS: &[(&str, &str)] = &[
    ("short", "登录手机号。中国农业银行。"),
    (
        "mixed_log",
        "2025-08-01 21:09:28 用户登录失败 trace_id=abc_123 dynamic_key=mobile_login 中国农业银行接口返回超时。",
    ),
    (
        "product_search",
        "哈基米哦南北绿豆，噢马自立曼波。装电视台，中国中央广播电视台。压不缩，笑不活。",
    ),
    (
        "long_news",
        "中国农业银行发布公告称，手机银行登录服务完成升级。多个地区用户反馈查询速度提升，后台监控显示核心链路延迟下降，异常请求自动重试次数减少。系统继续保留 trace_id、request_id 和 dynamic_key 等字段用于排查问题。",
    ),
];

const CHINESE_INDEX_DOCS: &[&str] = &[
    "登录手机号，中国农业银行手机银行接口返回成功。",
    "用户登录失败，trace_id=abc_123，dynamic_key=mobile_login。",
    "中国中央广播电视台发布新的节目预告。",
    "装电视台的时候遇到压不缩的问题。",
    "哈基米哦南北绿豆，噢马自立曼波。",
    "后台监控显示核心链路延迟下降。",
    "系统保留 request_id 用于排查问题。",
    "中文全文索引需要兼顾召回率和 token 数量。",
];

struct NoopPuffinWriter;

#[async_trait]
impl PuffinWriter for NoopPuffinWriter {
    async fn put_blob<R>(
        &mut self,
        _key: &str,
        _raw_data: R,
        _options: PutOptions,
        _properties: HashMap<String, String>,
    ) -> puffin::error::Result<u64>
    where
        R: AsyncRead + Send,
    {
        unreachable!("tantivy fulltext benchmark only writes directory blobs")
    }

    async fn put_dir(
        &mut self,
        _key: &str,
        _dir: PathBuf,
        _options: PutOptions,
        _properties: HashMap<String, String>,
    ) -> puffin::error::Result<u64> {
        Ok(0)
    }

    fn set_footer_lz4_compressed(&mut self, _lz4_compressed: bool) {}

    async fn finish(self) -> puffin::error::Result<u64> {
        Ok(0)
    }
}

fn bench_english_tokenizer(c: &mut Criterion) {
    let tokenizer = EnglishTokenizer;

    let texts = vec![
        ("short", "Hello, world! This is a test."),
        (
            "medium",
            "The quick brown fox jumps over the lazy dog. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
        ),
        (
            "long",
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt.",
        ),
        (
            "mixed_chars",
            "Hello123 world!!! This-is_a.test@example.com with various: punctuation; and [brackets] {curly} (parentheses) & symbols* + numbers456.",
        ),
        (
            "numbers_heavy",
            "test123 456test test789 abc123def 999888777 hello42world 123 456 789 mix1ng l3tt3rs 4nd numb3rs",
        ),
        (
            "punctuation_heavy",
            "Hello!!! World??? This...is...a...test... With lots of!!! punctuation??? marks!!! And... ellipses???",
        ),
        (
            "postgres log",
            "2025-08-01 21:09:28.928 UTC [27] LOG:  checkpoint complete: wrote 0 buffers (0.0%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.001 s, sync=0.001 s, total=0.003 s; sync files=0, longest=0.000 s, average=0.000 s; distance=0 kB, estimate=5 kB; lsn=0/1992868, redo lsn=0/1992868",
        ),
        (
            "many_short_words",
            "a b c d e f g h i j k l m n o p q r s t u v w x y z",
        ),
        (
            "with_unicode",
            "这是，一个测试。🈶一些 Unicøde 字符比如 café and naïve words.",
        ),
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

fn bench_chinese_tokenizer(c: &mut Criterion) {
    let tokenizer = ChineseTokenizer;
    let mut group = c.benchmark_group("chinese_tokenizer");

    for (name, text) in CHINESE_TOKENIZER_TEXTS {
        group.throughput(Throughput::Bytes(text.len() as u64));
        group.bench_with_input(BenchmarkId::new("tokenize", name), text, |b, text| {
            b.iter(|| black_box(tokenizer.tokenize(black_box(text))))
        });
    }

    group.finish();

    let mut repeat_group = c.benchmark_group("chinese_tokenizer_repeated");
    let sample_text = CHINESE_TOKENIZER_TEXTS
        .iter()
        .find(|(name, _)| *name == "mixed_log")
        .map(|(_, text)| *text)
        .expect("mixed_log sample must exist");

    for repeat_count in [10, 100, 1000] {
        repeat_group.bench_with_input(
            BenchmarkId::new("repeated_tokenize", repeat_count),
            &repeat_count,
            |b, &repeat_count| {
                b.iter(|| {
                    for _ in 0..repeat_count {
                        black_box(tokenizer.tokenize(black_box(sample_text)));
                    }
                })
            },
        );
    }

    repeat_group.finish();
}

fn bench_tantivy_chinese_fulltext_index(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create Tokio runtime");
    let config = Config {
        analyzer: Analyzer::Chinese,
        case_sensitive: false,
    };
    let mut group = c.benchmark_group("tantivy_chinese_fulltext_index");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for doc_count in [32usize, 256usize] {
        group.throughput(Throughput::Elements(doc_count as u64));
        group.bench_with_input(
            BenchmarkId::new("build_commit", doc_count),
            &doc_count,
            |b, &doc_count| {
                b.iter_batched(
                    tempfile::tempdir,
                    |dir| {
                        let dir = dir.expect("failed to create temp dir");
                        runtime.block_on(async {
                            let mut creator =
                                TantivyFulltextIndexCreator::new(dir.path(), config, 64 << 20)
                                    .await
                                    .expect("failed to create tantivy fulltext index");
                            for idx in 0..doc_count {
                                let text = CHINESE_INDEX_DOCS[idx % CHINESE_INDEX_DOCS.len()];
                                creator
                                    .push_text(black_box(text))
                                    .await
                                    .expect("failed to push text");
                            }
                            let mut puffin_writer = NoopPuffinWriter;
                            creator
                                .finish(
                                    &mut puffin_writer,
                                    "tantivy_chinese_fulltext_index",
                                    PutOptions::default(),
                                )
                                .await
                                .expect("failed to commit tantivy fulltext index");
                        });
                        // Return the temp dir so Criterion drops it after timing the routine.
                        dir
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_english_tokenizer,
    bench_chinese_tokenizer,
    bench_tantivy_chinese_fulltext_index
);
criterion_main!(benches);
