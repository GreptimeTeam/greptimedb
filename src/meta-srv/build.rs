fn main() {
    tonic_build::configure()
        .compile(&["proto/meta_srv.proto", "proto/rpc.proto"], &["."])
        .expect("compile proto");
}
