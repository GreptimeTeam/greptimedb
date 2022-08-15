fn main() {
    tonic_build::configure()
        .compile(
            &[
                "greptime/v1/insert.proto",
                "greptime/v1/select.proto",
                "greptime/v1/physical_plan.proto",
                "greptime/v1/greptime.proto",
                "greptime/v1/meta/common.proto",
                "greptime/v1/meta/meta.proto",
            ],
            &["."],
        )
        .expect("compile proto");
}
