fn main() {
    tonic_build::configure()
        .compile(&["proto/wal.proto"], &["."])
        .expect("compile wal proto");

    protobuf_codegen::Codegen::new()
        .cargo_out_dir("protos")
        .include(".")
        .input("proto/write_batch.proto")
        .run_from_script();
}
