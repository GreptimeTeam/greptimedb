# Fuzz Test for GreptimeDB

## Setup
1. Install the [fuzz](https://rust-fuzz.github.io/book/cargo-fuzz/setup.html) cli first.
```bash
cargo install cargo-fuzz
```

2. Start GreptimeDB
3. Copy the `.env.example`, which is at project root, to `.env` and change the values on need.

## Run
1. List all fuzz targets
```bash
cargo fuzz list --fuzz-dir tests-fuzz
```

2. Run a fuzz target.
```bash
cargo fuzz run fuzz_create_table --fuzz-dir tests-fuzz
```

## Crash Reproduction
If you want to reproduce a crash, you first need to obtain the Base64 encoded code, which usually appears at the end of a crash report, and store it in a file.

Alternatively, if you already have the crash file, you can skip this step.

```bash
echo "Base64" > .crash
```
Print the `std::fmt::Debug` output for an input.

```bash
cargo fuzz fmt fuzz_target .crash --fuzz-dir tests-fuzz  
```
Rerun the fuzz test with the input.

```bash
cargo fuzz run fuzz_target .crash --fuzz-dir tests-fuzz
```
For more details, visit [cargo fuzz](https://rust-fuzz.github.io/book/cargo-fuzz/tutorial.html) or run the command `cargo fuzz --help`.
