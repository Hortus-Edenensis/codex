use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::executable_identity_from_bytes;
use super::managed_codex_remote_sql_build_tag;
use super::parse_codex_version;

#[test]
fn parses_codex_cli_version_output() {
    assert_eq!(
        parse_codex_version("codex 1.2.3\n").expect("version"),
        "1.2.3"
    );
}

#[test]
fn rejects_malformed_codex_cli_version_output() {
    assert!(parse_codex_version("codex\n").is_err());
}

#[test]
fn executable_identity_uses_binary_contents() {
    let old = executable_identity_from_bytes(b"old");
    let same = executable_identity_from_bytes(b"old");
    let new = executable_identity_from_bytes(b"new");

    assert_eq!(old, same);
    assert_ne!(old, new);
}

#[tokio::test]
async fn reads_remote_sql_build_tag_from_release_dir() {
    let temp_dir = TempDir::new().expect("temp dir");
    let release_dir = temp_dir.path().join("current");
    let bin_dir = release_dir.join("bin");
    tokio::fs::create_dir_all(&bin_dir)
        .await
        .expect("create bin dir");
    tokio::fs::write(
        release_dir.join("REMOTE_SQL_BUILD_TAG"),
        "release_version=0.142.5-remote-sql.123+abc123\ngit_sha=abc123\n",
    )
    .await
    .expect("write build tag");

    assert_eq!(
        managed_codex_remote_sql_build_tag(&bin_dir.join("codex"))
            .await
            .expect("build tag"),
        "release_version=0.142.5-remote-sql.123+abc123\ngit_sha=abc123"
    );
}
