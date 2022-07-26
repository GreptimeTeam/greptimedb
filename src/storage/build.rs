fn main() {
    tonic_build::configure()
        .compile(&["proto/wal.proto", "proto/write_batch.proto"], &["."])
        .expect("compile wal proto");
}
