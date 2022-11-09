use std::path::PathBuf;

fn main() {
    let default_out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .file_descriptor_set_path(default_out_dir.join("greptime_fd.bin"))
        .compile(
            &[
                "greptime/v1/insert.proto",
                "greptime/v1/select.proto",
                "greptime/v1/physical_plan.proto",
                "greptime/v1/greptime.proto",
                "greptime/v1/meta/common.proto",
                "greptime/v1/meta/heartbeat.proto",
                "greptime/v1/meta/route.proto",
                "greptime/v1/meta/store.proto",
                "prometheus/remote/remote.proto",
            ],
            &["."],
        )
        .expect("compile proto");
}
