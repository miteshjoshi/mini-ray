fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile_protos(&["proto/mini_ray.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/mini_ray.proto");
    Ok(())
}
