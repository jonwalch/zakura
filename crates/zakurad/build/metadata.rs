//! Build metadata collection shared by `build.rs` and its integration tests.

use std::{
    env,
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

/// Git metadata compiled into `zakurad` when the source is in a Git checkout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GitMetadata {
    /// The checked-out branch, or `HEAD` for a detached checkout.
    pub(super) branch: String,
    /// The commit timestamp in UTC.
    pub(super) commit_timestamp: String,
    /// Output from the version-tag-constrained `git describe` invocation.
    pub(super) describe: String,
    /// The seven-character abbreviated commit hash used by existing builds.
    pub(super) sha: String,
    /// The checkout-specific Git directory.
    pub(super) git_dir: PathBuf,
    /// The shared Git directory, which owns refs in linked worktrees.
    pub(super) common_git_dir: PathBuf,
}

/// Returns the Cargo metadata consumed by `zakurad` diagnostics.
pub(super) fn cargo_metadata() -> Result<Vec<(&'static str, String)>, String> {
    cargo_metadata_from(env::vars_os())
}

/// Returns Cargo metadata derived from a supplied environment.
pub(super) fn cargo_metadata_from(
    variables: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<Vec<(&'static str, String)>, String> {
    let variables: Vec<_> = variables.into_iter().collect();
    let required = |name: &str| {
        variables
            .iter()
            .find_map(|(key, value)| (key == OsStr::new(name)).then(|| value.clone()))
            .ok_or_else(|| format!("Cargo did not set required build variable {name}"))
            .and_then(|value| {
                value
                    .into_string()
                    .map_err(|_| format!("Cargo build variable {name} is not valid UTF-8"))
            })
    };

    let mut features: Vec<_> = variables
        .iter()
        .filter_map(|(key, _)| key.to_str()?.strip_prefix("CARGO_FEATURE_"))
        .map(str::to_ascii_lowercase)
        .collect();
    features.sort_unstable();
    features.dedup();

    Ok(vec![
        ("VERGEN_CARGO_DEBUG", required("DEBUG")?),
        ("VERGEN_CARGO_FEATURES", features.join(",")),
        ("VERGEN_CARGO_OPT_LEVEL", required("OPT_LEVEL")?),
        ("VERGEN_CARGO_TARGET_TRIPLE", required("TARGET")?),
    ])
}

/// Returns the rustc metadata consumed by `zakurad` diagnostics.
pub(super) fn rustc_metadata() -> Result<Vec<(&'static str, String)>, String> {
    let rustc = env::var_os("RUSTC").unwrap_or_else(|| OsString::from("rustc"));
    let output = Command::new(&rustc)
        .arg("-vV")
        .output()
        .map_err(|error| format!("failed to run {rustc:?} -vV: {error}"))?;

    if !output.status.success() {
        return Err(command_failure("rustc -vV", &output));
    }

    let stdout = String::from_utf8(output.stdout)
        .map_err(|error| format!("rustc -vV output is not valid UTF-8: {error}"))?;
    rustc_metadata_from(&stdout)
}

/// Parses the stable fields used from `rustc -vV` output.
pub(super) fn rustc_metadata_from(output: &str) -> Result<Vec<(&'static str, String)>, String> {
    let field = |name: &str| {
        output
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| format!("rustc -vV output is missing {name}"))
    };

    Ok(vec![
        ("VERGEN_RUSTC_COMMIT_DATE", field("commit-date:")?),
        ("VERGEN_RUSTC_SEMVER", field("release:")?),
    ])
}

/// Collects Git metadata without invoking a shell or configurable Git helpers.
pub(super) fn git_metadata(manifest_dir: &Path, out_dir: &Path) -> Result<GitMetadata, String> {
    fs::create_dir_all(out_dir)
        .map_err(|error| format!("failed to create build output directory: {error}"))?;

    let empty_config = out_dir.join("empty-gitconfig");
    fs::write(&empty_config, b"")
        .map_err(|error| format!("failed to create empty Git configuration: {error}"))?;
    let no_hooks = out_dir.join("no-git-hooks");
    fs::create_dir_all(&no_hooks)
        .map_err(|error| format!("failed to create empty Git hooks directory: {error}"))?;

    let run = |arguments: &[&str]| git_output(manifest_dir, &empty_config, &no_hooks, arguments);

    let git_dir = absolute_path(manifest_dir, &run(&["rev-parse", "--absolute-git-dir"])?);
    let common_git_dir = absolute_path(
        manifest_dir,
        &run(&["rev-parse", "--path-format=absolute", "--git-common-dir"])?,
    );

    Ok(GitMetadata {
        branch: run(&["rev-parse", "--abbrev-ref", "HEAD"])?,
        commit_timestamp: run(&[
            "show",
            "-s",
            "--date=format-local:%Y-%m-%dT%H:%M:%S.000000000Z",
            "--format=%cd",
            "HEAD",
        ])?,
        describe: run(&[
            "describe",
            "--tags",
            "--dirty",
            "--match=v*.*.*",
            "--abbrev=7",
        ])?,
        sha: run(&["rev-parse", "--short=7", "HEAD"])?,
        git_dir,
        common_git_dir,
    })
}

/// Returns files and directories that can change the emitted Git metadata.
pub(super) fn git_rerun_paths(metadata: &GitMetadata) -> Vec<PathBuf> {
    let head = metadata.git_dir.join("HEAD");
    let mut paths = vec![head.clone()];

    if let Ok(contents) = fs::read_to_string(&head) {
        if let Some(reference) = contents.trim().strip_prefix("ref: ") {
            paths.push(metadata.common_git_dir.join(reference));
        }
    }

    for path in [
        metadata.git_dir.join("index"),
        metadata.common_git_dir.join("packed-refs"),
        metadata.common_git_dir.join("refs/tags"),
    ] {
        if path.exists() {
            paths.push(path);
        }
    }

    paths
}

/// Emits one compile-time environment variable for rustc.
#[allow(clippy::print_stdout)]
pub(super) fn emit_rustc_env(name: &str, value: &str) -> Result<(), String> {
    validate_directive_value(name)?;
    validate_directive_value(value)?;
    println!("cargo:rustc-env={name}={value}");
    Ok(())
}

/// Emits one Cargo rebuild path.
#[allow(clippy::print_stdout)]
pub(super) fn emit_rerun_path(path: &Path) -> Result<(), String> {
    let path = path.to_string_lossy();
    validate_directive_value(&path)?;
    println!("cargo:rerun-if-changed={path}");
    Ok(())
}

/// Rejects values that could inject additional Cargo build-script directives.
pub(super) fn validate_directive_value(value: &str) -> Result<(), String> {
    if value
        .chars()
        .any(|character| matches!(character, '\n' | '\r' | '\0'))
    {
        Err("build metadata contains a forbidden control character".to_string())
    } else {
        Ok(())
    }
}

fn git_output(
    manifest_dir: &Path,
    empty_config: &Path,
    no_hooks: &Path,
    arguments: &[&str],
) -> Result<String, String> {
    let mut command = Command::new("git");
    command.current_dir(manifest_dir);

    // Strip all caller-provided Git behavior before setting the small set of
    // deterministic variables needed by these local, read-only commands.
    for (name, _) in env::vars_os() {
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("GIT_")
        {
            command.env_remove(name);
        }
    }

    let hooks_path = format!("core.hooksPath={}", no_hooks.display());
    command
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", empty_config)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("LC_ALL", "C")
        .env("TZ", "UTC0")
        .arg("--no-optional-locks")
        .args(["-c", "core.fsmonitor=false"])
        .args(["-c", hooks_path.as_str()])
        .args(["-c", "diff.external="])
        .args(["-c", "core.abbrev=7"])
        .args(arguments);

    let output = command
        .output()
        .map_err(|error| format!("failed to run git {arguments:?}: {error}"))?;
    if !output.status.success() {
        return Err(command_failure(&format!("git {arguments:?}"), &output));
    }

    let value = String::from_utf8(output.stdout)
        .map_err(|error| format!("git {arguments:?} output is not valid UTF-8: {error}"))?;
    let value = value.trim_end_matches(['\r', '\n'].as_slice());
    if value.is_empty() {
        return Err(format!("git {arguments:?} returned empty output"));
    }
    if value.len() > 4096 {
        return Err(format!(
            "git {arguments:?} returned unexpectedly long output"
        ));
    }
    validate_directive_value(value)?;
    Ok(value.to_string())
}

fn command_failure(command: &str, output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr: String = stderr.trim().chars().take(512).collect();
    format!("{command} failed with status {}: {stderr}", output.status)
}

fn absolute_path(base: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}
