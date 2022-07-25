fn main() {
    tonic_build::configure()
        .compile(&["proto/wal.proto"], &["."])
        .expect("compile wal proto");
}
