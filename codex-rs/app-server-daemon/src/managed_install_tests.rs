use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::ExecutableIdentity;
use super::executable_identity;
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

#[tokio::test]
async fn executable_identity_uses_binary_contents() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let executable = directory.path().join("codex");
    // Span multiple reads, including a partial final buffer, and preserve the
    // digest stored by older clients that hashed the complete file in memory.
    let mut bytes: Vec<u8> = (0..200_003).map(|index| (index % 251) as u8).collect();
    for contents in [&bytes[..], &[][..]] {
        std::fs::write(&executable, contents).expect("write executable");
        assert_eq!(
            executable_identity(&executable).await.expect("identity"),
            ExecutableIdentity {
                digest: *blake3::hash(contents).as_bytes(),
            }
        );
    }
    std::fs::write(&executable, &bytes).expect("write executable");
    let old = executable_identity(&executable).await.expect("identity");
    bytes[100_000] ^= 1;
    std::fs::write(&executable, bytes).expect("replace executable");
    assert_ne!(
        executable_identity(&executable)
            .await
            .expect("new identity"),
        old
    );
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

    for binary in [bin_dir.join("codex"), release_dir.join("codex")] {
        assert_eq!(
            managed_codex_remote_sql_build_tag(&binary)
                .await
                .expect("build tag"),
            "release_version=0.142.5-remote-sql.123+abc123\ngit_sha=abc123"
        );
    }
    tokio::fs::write(release_dir.join("REMOTE_SQL_BUILD_TAG"), " \n")
        .await
        .expect("empty build tag");
    assert!(
        managed_codex_remote_sql_build_tag(&bin_dir.join("codex"))
            .await
            .is_err()
    );
}
