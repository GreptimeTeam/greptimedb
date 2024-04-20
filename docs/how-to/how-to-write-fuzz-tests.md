This document introduce how to write fuzz tests in GreptimeDB.

All fuzz test-related resources are located in the `/tests-fuzz` directory.
- Each target(located in the `/tests-fuzz/targets`) stands for an independent fuzz test case.
- Fuzz test fundamental components are located in the `/tests-fuzz/src`.

Figure 1 illustrates that it utilizes a Rng (Random Number Generator) to generate the IR (Intermediate Representation), then employs a DialectTranslator to produce specified dialects for different protocols.

```
                            Rng                                 
                             |                                  
                             |                                  
                             v                                  
                       ExprGenerator                            
                             |                                  
                             |                                  
                             v                                  
               Intermediate representation (IR)                 
                             |                                  
                             |                                  
      +----------------------+----------------------+           
      |                      |                      |           
      v                      v                      v           
MySQLTranslator    PostgreSQLTranslator   OtherDialectTranslator
      |                      |             |           
      |                      |                      |           
      v                      v                      v           
 MySQL Dialect             .....                  .....         
```
(Figure1: Overview of fuzz test components)

## How to add a fuzz test target

1. Create an empty rust source file under the `/tests-fuzz/targets/` directory.

2. Register the fuzz test target in the `/tests-fuzz/Cargo.toml` file.
```toml
...
[[bin]]
name = "{target name}"
path = "targets/{target}.rs"
test = false
bench = false
doc = false
...
```

3. Define the `FuzzInput`

```rust
#[derive(Clone, Debug)]
struct FuzzInput {
    seed: u64,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        Ok(FuzzInput { seed })
    }
}
```

4. Your first fuzz test target

```rust
fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_write(async {
        let Connections { mysql } = init_greptime_connections().await;
            let mut rng = ChaChaRng::seed_from_u64(input.seed);
            let columns = rng.gen_range(2..30);
            ...
            let create_table_generator = CreateTableExprGeneratorBuilder::default()
                .name_generator(Box::new(MappedGenerator::new(
                    WordGenerator,
                    merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
                )))
                .columns(columns)
                .engine("mito")
                .if_not_exists(if_not_exists)
                .build()
                .unwrap();
            let ir = create_table_generator.generate(&mut rng);
            let translator = CreateTableExprTranslator;
            let sql = translator.translate(&expr).unwrap();
            ...
            mysql.execute(&sql).await
    })
});
```
