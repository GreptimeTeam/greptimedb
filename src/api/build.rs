fn main() {
    tonic_build::configure()
        .compile(&["greptime/v1/greptime.proto"], &["."])
        .expect("compile proto");
}
