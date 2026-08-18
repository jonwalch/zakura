//! Compatibility and authority tests for Zakura's build metadata collector.

#[allow(dead_code)]
#[path = "../build/metadata.rs"]
mod metadata;

use std::{
    ffi::OsString,
    fs,
    path::Path,
    process::{Command, Output},
};

#[test]
fn cargo_metadata_is_complete_and_deterministic() {
    let variables = [
        ("TARGET", "aarch64-unknown-linux-gnu"),
        ("CARGO_FEATURE_ZETA", "1"),
        ("DEBUG", "false"),
        ("CARGO_FEATURE_ALPHA_BETA", "1"),
        ("OPT_LEVEL", "3"),
    ]
    .map(|(name, value)| (OsString::from(name), OsString::from(value)));

    assert_eq!(
        metadata::cargo_metadata_from(variables).expect("the fixture is complete"),
        [
            ("VERGEN_CARGO_DEBUG", "false"),
            ("VERGEN_CARGO_FEATURES", "alpha_beta,zeta"),
            ("VERGEN_CARGO_OPT_LEVEL", "3"),
            ("VERGEN_CARGO_TARGET_TRIPLE", "aarch64-unknown-linux-gnu"),
        ]
        .map(|(name, value)| (name, value.to_string()))
    );
}

#[test]
fn rustc_metadata_uses_stable_verbose_version_fields() {
    let output = "rustc 1.91.0 (example 2025-10-30)\n\
                  binary: rustc\n\
                  commit-hash: example\n\
                  commit-date: 2025-10-30\n\
                  host: x86_64-unknown-linux-gnu\n\
                  release: 1.91.0\n\
                  LLVM version: 20.1.8\n";

    assert_eq!(
        metadata::rustc_metadata_from(output).expect("the fixture is complete"),
        [
            ("VERGEN_RUSTC_COMMIT_DATE", "2025-10-30"),
            ("VERGEN_RUSTC_SEMVER", "1.91.0"),
        ]
        .map(|(name, value)| (name, value.to_string()))
    );
}

#[test]
fn cargo_directive_values_reject_line_injection() {
    assert!(metadata::validate_directive_value("ordinary-value").is_ok());
    assert!(metadata::validate_directive_value("value\ncargo:warning=injected").is_err());
    assert!(metadata::validate_directive_value("value\rcargo:warning=injected").is_err());
    assert!(metadata::validate_directive_value("value\0suffix").is_err());
}

#[test]
fn git_metadata_preserves_version_semantics() {
    let repository = tempfile::tempdir().expect("temporary repository should be created");
    initialize_repository(repository.path());

    fs::write(repository.path().join("tracked"), "first").expect("fixture should be written");
    git(repository.path(), &["add", "tracked"]);
    git(repository.path(), &["commit", "-m", "first"]);
    git(repository.path(), &["tag", "v1.2.3"]);

    fs::write(repository.path().join("tracked"), "second").expect("fixture should be updated");
    git(repository.path(), &["commit", "-am", "second"]);
    git(repository.path(), &["tag", "zakura-rpc-v9.0.0"]);

    let build_output = repository.path().join("build-output");
    let clean = metadata::git_metadata(repository.path(), &build_output)
        .expect("Git metadata should be available");
    assert_eq!(clean.branch, "main");
    assert_eq!(clean.sha.len(), 7);
    assert_eq!(clean.describe, format!("v1.2.3-1-g{}", clean.sha));
    assert!(clean.commit_timestamp.ends_with(".000000000Z"));

    let rerun_paths = metadata::git_rerun_paths(&clean);
    assert!(rerun_paths.iter().any(|path| path.ends_with("HEAD")));
    assert!(rerun_paths
        .iter()
        .any(|path| path.ends_with("refs/heads/main")));
    assert!(rerun_paths.iter().any(|path| path.ends_with("refs/tags")));

    fs::write(repository.path().join("tracked"), "dirty").expect("fixture should be made dirty");
    let dirty = metadata::git_metadata(repository.path(), &build_output)
        .expect("dirty Git metadata should be available");
    assert_eq!(dirty.describe, format!("v1.2.3-1-g{}-dirty", dirty.sha));

    git(repository.path(), &["checkout", "--detach"]);
    let detached = metadata::git_metadata(repository.path(), &build_output)
        .expect("detached Git metadata should be available");
    assert_eq!(detached.branch, "HEAD");
}

#[test]
fn missing_git_directory_is_nonfatal_to_callers() {
    let source = tempfile::tempdir().expect("temporary source directory should be created");
    let build_output = source.path().join("build-output");

    assert!(metadata::git_metadata(source.path(), &build_output).is_err());
}

#[cfg(unix)]
#[test]
fn configured_fsmonitor_is_not_executed() {
    use std::os::unix::fs::PermissionsExt;

    let repository = tempfile::tempdir().expect("temporary repository should be created");
    initialize_repository(repository.path());
    fs::write(repository.path().join("tracked"), "content").expect("fixture should be written");
    git(repository.path(), &["add", "tracked"]);
    git(repository.path(), &["commit", "-m", "fixture"]);
    git(repository.path(), &["tag", "v1.0.0"]);

    let sentinel = repository.path().join("fsmonitor-ran");
    let helper = repository.path().join("fsmonitor-helper");
    fs::write(
        &helper,
        format!("#!/bin/sh\ntouch '{}'\nexit 1\n", sentinel.display()),
    )
    .expect("helper should be written");
    let mut permissions = fs::metadata(&helper)
        .expect("helper metadata should exist")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&helper, permissions).expect("helper should be executable");
    git(
        repository.path(),
        &[
            "config",
            "core.fsmonitor",
            helper.to_str().expect("UTF-8 path"),
        ],
    );

    metadata::git_metadata(repository.path(), &repository.path().join("build-output"))
        .expect("local fsmonitor configuration should be overridden");
    assert!(!sentinel.exists(), "the configured fsmonitor helper ran");
}

fn initialize_repository(repository: &Path) {
    git(repository, &["init", "-b", "main"]);
    git(repository, &["config", "user.name", "Zakura Test"]);
    git(
        repository,
        &["config", "user.email", "zakura-test@example.com"],
    );
    git(repository, &["config", "commit.gpgSign", "false"]);
    git(repository, &["config", "tag.gpgSign", "false"]);
}

fn git(repository: &Path, arguments: &[&str]) -> Output {
    let output = Command::new("git")
        .current_dir(repository)
        .args(arguments)
        .output()
        .expect("test Git command should run");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
