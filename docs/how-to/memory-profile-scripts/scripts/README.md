# Memory Analysis Process
This section will guide you through the process of analyzing memory usage for greptimedb.

1. Get the `jeprof` tool script, see the next section("Getting the `jeprof` tool") for details.

2. After starting `greptimedb`(with env var `MALLOC_CONF=prof:true`), execute the `dump.sh` script with the PID of the `greptimedb` process as an argument. This continuously monitors memory usage and captures profiles when exceeding thresholds (e.g. +20MB within 10 minutes). Outputs `greptime-{timestamp}.gprof` files.

3. With 2-3 gprof files, run `gen_flamegraph.sh` in the same environment to generate flame graphs showing memory allocation call stacks.

4.  **NOTE:** The `gen_flamegraph.sh` script requires `jeprof` and optionally `flamegraph.pl` to be in the current directory. If needed to gen flamegraph now, run the `get_flamegraph_tool.sh` script, which downloads the flame graph generation tool `flamegraph.pl` to the current directory.
    The usage of `gen_flamegraph.sh` is:

    `Usage: ./gen_flamegraph.sh <binary_path> <gprof_directory>`
    where `<binary_path>` is the path to the greptimedb binary, `<gprof_directory>` is the directory containing the gprof files(the directory `dump.sh` is dumping profiles to).
    Example call: `./gen_flamegraph.sh ./greptime .`

    Generating the flame graph might take a few minutes. The generated flame graphs are located in the `<gprof_directory>/flamegraphs` directory. Or if no `flamegraph.pl` is found, it will only contain `.collapse` files which is also fine.
5.  You can send the generated flame graphs(the entire folder of `<gprof_directory>/flamegraphs`) to developers for further analysis.


## Getting the `jeprof` tool
there are three ways to get `jeprof`, list in here from simple to complex, using any one of those methods is ok, as long as it's the same environment as the `greptimedb` will be running on:
1. If you are compiling greptimedb from source, then `jeprof` is already produced during compilation. After running `cargo build`, execute `find_compiled_jeprof.sh`. This will copy `jeprof` to the current directory.
2. Or, if you have the Rust toolchain installed locally, simply follow these commands:
```bash
cargo new get_jeprof
cd get_jeprof
```
Then add this line to `Cargo.toml`:
```toml
[dependencies]
tikv-jemalloc-ctl = { version = "0.6", features = ["use_std", "stats"] }
```
then run:
```bash
cargo build
```
after that the `jeprof` tool is produced. Now run `find_compiled_jeprof.sh` in current directory, it will copy the `jeprof` tool to the current directory.

3. compile jemalloc from source
you can first clone this repo, and checkout to this commit:
```bash
git clone https://github.com/tikv/jemalloc.git
cd jemalloc
git checkout e13ca993e8ccb9ba9847cc330696e02839f328f7
```
then run:
```bash
./configure
make
```
and `jeprof` is in `.bin/` directory. Copy it to the current directory.
