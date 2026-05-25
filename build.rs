//! Compile the Harmonic searcher protos (Jito-compatible, Searcher role = 3)
//! into Rust gRPC client code for `src/harmonic.rs`. Client-only.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .compile_protos(
            &[
                "proto/auth.proto",
                "proto/searcher.proto",
                "proto/bundle.proto",
                "proto/packet.proto",
                "proto/shared.proto",
            ],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
