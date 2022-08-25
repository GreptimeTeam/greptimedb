fn main() {
    tonic_build::configure()
        .compile(
            &[
                "greptime/v1/insert.proto",
                "greptime/v1/select.proto",
                "greptime/v1/physical_plan.proto",
                "greptime/v1/greptime.proto",
            ],
            &["."],
        )
        .expect("compile proto");
}
