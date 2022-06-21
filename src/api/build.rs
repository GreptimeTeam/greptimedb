fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(&["greptime/v1/greptime.proto"], &["."])?;
    Ok(())
}
