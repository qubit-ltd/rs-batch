// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! README consistency checks for `qubit-batch`.

const CARGO_TOML: &str = include_str!("../../Cargo.toml");
const README_EN: &str = include_str!("../../README.md");
const README_ZH: &str = include_str!("../../README.zh_CN.md");
const PARALLEL_BATCH_EXECUTION_COORDINATOR: &str =
    include_str!("../../src/execute/parallel_batch_execution_coordinator.rs");
const PARALLEL_BATCH_EXECUTOR: &str = include_str!("../../src/execute/impls/parallel_batch_executor.rs");
const PARALLEL_BATCH_EXECUTOR_BUILDER: &str =
    include_str!("../../src/execute/impls/parallel_batch_executor_builder.rs");
const PARALLEL_BATCH_PROCESSOR: &str = include_str!("../../src/process/impls/parallel_batch_processor.rs");

/// Ensures README dependency snippets use the same major.minor line as
/// `[package] version`.
#[test]
fn test_readme_dependency_version_matches_cargo_toml() {
    let cargo_version = extract_package_version(CARGO_TOML).expect("Failed to extract version from Cargo.toml");
    let expected = minor_series(cargo_version).expect("Cargo.toml version must have major.minor");
    let readme_en_version =
        extract_readme_dependency_version(README_EN).expect("Failed to extract version from README.md");
    let readme_zh_version =
        extract_readme_dependency_version(README_ZH).expect("Failed to extract version from README.zh_CN.md");
    assert_eq!(readme_en_version, expected.as_str());
    assert_eq!(readme_zh_version, expected.as_str());
}

/// Ensures both README files document the current executor types.
#[test]
fn test_readme_mentions_current_executor_types() {
    assert!(README_EN.contains("SequentialBatchExecutor"));
    assert!(README_EN.contains("ParallelBatchExecutor"));
    assert!(README_ZH.contains("SequentialBatchExecutor"));
    assert!(README_ZH.contains("ParallelBatchExecutor"));
}

/// Ensures license and repository links use the package-local documentation
/// paths.
#[test]
fn test_readmes_use_local_license_and_repository_links() {
    for readme in [README_EN, README_ZH] {
        assert!(readme.contains("license-Apache%202.0-blue.svg)](LICENSE)"));
        assert!(readme.contains("[LICENSE](LICENSE)"));
        assert!(
            readme
                .contains("Repository: [https://github.com/qubit-ltd/rs-batch](https://github.com/qubit-ltd/rs-batch)",)
                || readme.contains(
                    "仓库地址：[https://github.com/qubit-ltd/rs-batch](https://github.com/qubit-ltd/rs-batch)",
                )
        );
    }
}

/// Ensures parallel implementations use the shared scoped progress guard.
#[test]
fn test_parallel_progress_reporting_uses_scoped_progress_guard() {
    assert!(PARALLEL_BATCH_EXECUTION_COORDINATOR.contains("spawn_auto_reporter"));
    assert!(PARALLEL_BATCH_EXECUTOR.contains("coordinator") && PARALLEL_BATCH_EXECUTOR.contains(".execute"));
    assert!(PARALLEL_BATCH_EXECUTOR_BUILDER.contains("ParallelBatchExecutionCoordinator::new"));
    assert!(PARALLEL_BATCH_PROCESSOR.contains("spawn_auto_reporter"));
    assert!(!PARALLEL_BATCH_EXECUTOR.contains("RunningProgressLoop"));
    assert!(!PARALLEL_BATCH_PROCESSOR.contains("RunningProgressLoop"));
}

/// Returns `major.minor` from a semver string (e.g. `0.5.1` → `0.5`).
fn minor_series(version: &str) -> Option<String> {
    let mut parts = version.split('.');
    let major = parts.next()?;
    let minor = parts.next()?;
    Some(format!("{major}.{minor}"))
}

/// Extracts the first package version entry from Cargo.toml content.
fn extract_package_version(content: &str) -> Option<&str> {
    for line in content.lines() {
        if let Some(value) = line.strip_prefix("version = \"") {
            return value.strip_suffix('"');
        }
    }
    None
}

/// Extracts the `qubit-batch` dependency version from a README file.
fn extract_readme_dependency_version(content: &str) -> Option<&str> {
    for line in content.lines() {
        if let Some(value) = line.trim().strip_prefix("qubit-batch = \"") {
            return value.strip_suffix('"');
        }
    }
    None
}
