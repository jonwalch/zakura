//! Build script for zakurad.
//!
//! Turns Zakura version information into build-time environmental variables,
//! so that it can be compiled into `zakurad`, and used in diagnostics.
//!
//! When compiling the `lightwalletd` gRPC tests, also builds a gRPC client
//! Rust API for `lightwalletd`.

#[path = "build/metadata.rs"]
mod metadata;

use std::{env, path::PathBuf};

/// Process entry point for `zakurad`'s build script.
#[allow(clippy::print_stderr)]
fn main() {
    for (name, value) in metadata::cargo_metadata()
        .expect("Cargo should provide the required build metadata")
        .into_iter()
        .chain(
            metadata::rustc_metadata()
                .expect("rustc -vV should provide the required compiler metadata"),
        )
    {
        metadata::emit_rustc_env(name, &value)
            .expect("Cargo and rustc metadata should be safe Cargo directive values");
    }

    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR")
            .expect("Cargo should provide the package manifest directory"),
    );
    let out_dir = PathBuf::from(
        env::var_os("OUT_DIR").expect("Cargo should provide the build output directory"),
    );

    match metadata::git_metadata(&manifest_dir, &out_dir) {
        Ok(git) => {
            for (name, value) in [
                ("VERGEN_GIT_BRANCH", git.branch.as_str()),
                ("VERGEN_GIT_COMMIT_TIMESTAMP", git.commit_timestamp.as_str()),
                ("VERGEN_GIT_DESCRIBE", git.describe.as_str()),
                ("VERGEN_GIT_SHA", git.sha.as_str()),
            ] {
                metadata::emit_rustc_env(name, value)
                    .expect("Git metadata should be safe Cargo directive values");
            }

            for path in metadata::git_rerun_paths(&git) {
                metadata::emit_rerun_path(&path)
                    .expect("Git metadata paths should be safe Cargo directive values");
            }
        }
        Err(error) => {
            // Packaged sources and some Docker contexts intentionally have no
            // `.git` directory. Git metadata is optional in those builds.
            eprintln!("git metadata unavailable: {error}");
        }
    }

    // Watch the whole package so dirty-source changes refresh `git describe`.
    metadata::emit_rerun_path(&manifest_dir)
        .expect("the package path should be a safe Cargo directive value");

    #[cfg(feature = "lightwalletd-grpc-tests")]
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(
            &["tests/common/lightwalletd/proto/service.proto"],
            &["tests/common/lightwalletd/proto"],
        )
        .expect("Failed to generate lightwalletd gRPC files");
}
