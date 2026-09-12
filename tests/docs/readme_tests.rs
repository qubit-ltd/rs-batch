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

/// Keeps navigation before the fixed project footer in both languages.
#[test]
fn test_readme_footer_and_guide_navigation() {
    for (readme, expected) in [
        (README_EN, ["## Testing", "## License", "## Contributing", "## Author"]),
        (README_ZH, ["## 测试", "## 许可证", "## 贡献", "## 作者"]),
    ] {
        let headings: Vec<_> = readme.lines().filter(|line| line.starts_with("## ")).collect();
        assert_eq!(
            &headings[headings.len() - 4..],
            expected,
            "project footer must end the README"
        );
        assert!(
            readme.contains("(doc/user_guide.md)"),
            "English guide must be discoverable"
        );
        assert!(
            readme.contains("(doc/user_guide.zh_CN.md)"),
            "Chinese guide must be discoverable"
        );
    }
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
