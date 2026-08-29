import argparse
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("deploy_remote_sql_release.py")
WORKFLOW = SCRIPT.parent.parent / "workflows" / "remote-sql-release.yml"
SPEC = importlib.util.spec_from_file_location("deploy_remote_sql_release", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
deploy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(deploy)


def deployment_document(release: str = "0.142.5-copy.56+03efbd5ef6b9") -> dict:
    annotation = "codex.openai.com/remote-sql-release"
    return {
        "metadata": {
            "uid": "uid-1",
            "resourceVersion": "100",
            "annotations": {annotation: release},
        },
        "spec": {
            "replicas": 1,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": {"app": "copy"}},
            "template": {
                "metadata": {"annotations": {annotation: release}},
                "spec": {
                    "containers": [
                        {
                            "name": "workspace",
                            "command": [
                                "sh",
                                "-c",
                                f"set -eu\nexport CODEX_STANDALONE_RELEASE='{release}'\nexec run\n",
                            ],
                        }
                    ]
                },
            },
        },
    }


def common_args(state_file: Path) -> argparse.Namespace:
    return argparse.Namespace(
        namespace="codex-internal",
        deployment="codex-workspace-copy",
        container="workspace",
        release_variable="CODEX_STANDALONE_RELEASE",
        command_index=2,
        release_annotation="codex.openai.com/remote-sql-release",
        release_root="/releases",
        pg_backup_root="/backups",
        state_file=state_file,
        resume_thread_id="known-thread",
        rollout_timeout=60,
    )


class ReleaseScriptTests(unittest.TestCase):
    def test_workflow_stops_at_authenticated_draft_without_self_hosted_jobs(
        self,
    ) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        draft = workflow.split("  draft-release:\n", 1)[1]
        self.assertNotIn("  deploy-copy-workspace:\n", workflow)
        self.assertNotIn("  publish-release:\n", workflow)
        self.assertNotIn("self-hosted", workflow)
        self.assertIn("runs-on: ubuntu-22.04", draft)
        self.assertIn('gh release create "${EXPECTED_TAG}"', draft)
        self.assertIn("--draft", draft)
        self.assertIn('gh release upload "${EXPECTED_TAG}" --clobber', draft)
        self.assertIn("draft Release asset set mismatch", draft)
        self.assertIn("external SSH deployment and verification", draft)

    def test_deployment_shape_requires_one_recreate_replica(self) -> None:
        valid = deployment_document()
        deploy.require_supported_deployment_shape(valid)

        replicas = deployment_document()
        replicas["spec"]["replicas"] = 2
        with self.assertRaisesRegex(deploy.DeployError, "exactly one"):
            deploy.require_supported_deployment_shape(replicas)

        rolling = deployment_document()
        rolling["spec"]["strategy"] = {"type": "RollingUpdate"}
        with self.assertRaisesRegex(deploy.DeployError, "Recreate"):
            deploy.require_supported_deployment_shape(rolling)

    def test_deployment_reads_always_apply_the_shape_gate(self) -> None:
        document = deployment_document()
        document["spec"]["replicas"] = 2
        with tempfile.TemporaryDirectory() as raw:
            args = common_args(Path(raw) / "state.json")
            with mock.patch.object(deploy, "kubectl_json", return_value=document):
                with self.assertRaisesRegex(deploy.DeployError, "exactly one"):
                    deploy.deployment_document(args)

    def test_release_assignment_supports_runtime_build_metadata(self) -> None:
        old = "0.142.5-remote-sql-copy.56+03efbd5ef6b9"
        new = "0.151.0-remote-sql-copy.1+abcdef123456"
        script = f"set -eu\nexport CODEX_STANDALONE_RELEASE='{old}'\nexec run\n"
        self.assertEqual(deploy.release_from_script(script, "CODEX_STANDALONE_RELEASE"), old)
        replaced = deploy.replace_release_in_script(
            script, "CODEX_STANDALONE_RELEASE", old, new
        )
        self.assertEqual(
            deploy.release_from_script(replaced, "CODEX_STANDALONE_RELEASE"), new
        )
        self.assertIn("exec run", replaced)

    def test_release_assignment_requires_exactly_one_literal(self) -> None:
        with self.assertRaises(deploy.DeployError):
            deploy.release_from_script("set -eu\n", "CODEX_STANDALONE_RELEASE")
        duplicate = (
            "export CODEX_STANDALONE_RELEASE=one\n"
            "export CODEX_STANDALONE_RELEASE=two\n"
        )
        with self.assertRaises(deploy.DeployError):
            deploy.release_from_script(duplicate, "CODEX_STANDALONE_RELEASE")

    def test_command_and_annotations_are_one_dry_run_then_one_real_patch(self) -> None:
        old = "0.142.5-copy.56+03efbd5ef6b9"
        new = "0.151.0-remote-sql-copy.1+abcdef123456"
        document = deployment_document(old)
        old_script = document["spec"]["template"]["spec"]["containers"][0]["command"][2]
        state = {
            "oldCommandScript": old_script,
            "newCommandScript": deploy.replace_release_in_script(
                old_script, "CODEX_STANDALONE_RELEASE", old, new
            ),
            "oldAnnotations": deploy.annotation_snapshot(
                document, "codex.openai.com/remote-sql-release"
            ),
            "newReleaseSelector": new,
        }
        with tempfile.TemporaryDirectory() as raw:
            args = common_args(Path(raw) / "state.json")
            with mock.patch.object(deploy, "kubectl") as kubectl:
                deploy.patch_selector(args, document, state, rollback_patch=False)
        self.assertEqual(kubectl.call_count, 2)
        dry_run = kubectl.call_args_list[0].args[1:]
        real = kubectl.call_args_list[1].args[1:]
        self.assertIn("--dry-run=server", dry_run)
        self.assertNotIn("--dry-run=server", real)
        patch_json = dry_run[dry_run.index("--patch") + 1]
        operations = json.loads(patch_json)
        command_path = "/spec/template/spec/containers/0/command/2"
        self.assertIn(
            {"op": "test", "path": command_path, "value": old_script}, operations
        )
        annotation_path = "/metadata/annotations/codex.openai.com~1remote-sql-release"
        self.assertTrue(
            any(op.get("path") == annotation_path and op.get("value") == new for op in operations)
        )

    def test_rollback_restores_absent_annotation_maps(self) -> None:
        key = "codex.openai.com/remote-sql-release"
        document = deployment_document("new")
        document["metadata"]["annotations"] = {key: "new"}
        document["spec"]["template"]["metadata"]["annotations"] = {key: "new"}
        old = {
            "deployment": {"mapPresent": False, "keyPresent": False, "value": None},
            "podTemplate": {"mapPresent": False, "keyPresent": False, "value": None},
        }
        patch = deploy.rollback_annotation_patch(document, key, old, "new")
        self.assertIn({"op": "remove", "path": "/metadata/annotations"}, patch)
        self.assertIn(
            {"op": "remove", "path": "/spec/template/metadata/annotations"}, patch
        )

    def test_activate_rejects_resource_version_drift_before_patch(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            state_path = Path(raw) / "state.json"
            args = common_args(state_path)
            state = {
                "stateVersion": 1,
                "stage": "prepared",
                "deployment": args.deployment,
                "deploymentUid": "uid-1",
                "preparedResourceVersion": "99",
                "container": args.container,
                "releaseVariable": args.release_variable,
                "commandIndex": args.command_index,
                "releaseAnnotation": args.release_annotation,
                "knownResumeThreadId": args.resume_thread_id,
            }
            deploy.atomic_write_json(state_path, state)
            with mock.patch.object(
                deploy, "deployment_document", return_value=deployment_document()
            ), mock.patch.object(deploy, "patch_selector") as patch_selector:
                with self.assertRaisesRegex(deploy.DeployError, "resourceVersion"):
                    deploy.activate(args)
            patch_selector.assert_not_called()

    def test_rollout_command_has_one_rollout_token(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            args = common_args(Path(raw) / "state.json")
            with mock.patch.object(deploy, "kubectl") as kubectl:
                deploy.rollout_status(args)
        parts = kubectl.call_args.args[1:]
        self.assertEqual(parts[:2], ("rollout", "status"))
        self.assertEqual(parts.count("rollout"), 1)

    def test_selected_release_identity_uses_binary_sha_not_directory_name(self) -> None:
        selector = "copy-workspace-0.142.5-build-x86_64-unknown-linux-gnu"
        version = "codex-cli 0.142.5-remote-sql-copy.56+03efbd5ef6b9"
        digest = "a" * 64
        result = mock.Mock(
            stdout=(
                "SELECTED_RELEASE_MATCH=1\n"
                f"BINARY_SHA256={digest}\n"
                f"VERSION_OUTPUT={version}\n"
            )
        )
        args = argparse.Namespace(
            namespace="codex-internal",
            container="workspace",
            release_root="/releases",
        )
        with mock.patch.object(deploy, "remote_exec", return_value=result):
            inspected = deploy.inspect_selected_release(args, "workspace-pod", selector)
        self.assertEqual(inspected["BINARY_SHA256"], digest)
        self.assertEqual(inspected["VERSION_OUTPUT"], version)

    def test_rollback_runtime_comes_from_version_output_not_selector(self) -> None:
        selector = "copy-workspace-0.142.5-build-x86_64-unknown-linux-gnu"
        output = "codex-cli 0.142.5-remote-sql-copy.56+03efbd5ef6b9"
        self.assertNotIn(selector, output)
        self.assertEqual(
            deploy.runtime_version_from_output(output),
            "0.142.5-remote-sql-copy.56+03efbd5ef6b9",
        )
        with self.assertRaisesRegex(deploy.DeployError, "version output"):
            deploy.runtime_version_from_output("codex-cli invalid")

    def test_rollback_verifies_saved_runtime_instead_of_packaging_selector(self) -> None:
        old_selector = "copy-workspace-0.142.5-build-x86_64-unknown-linux-gnu"
        old_runtime = "0.142.5-remote-sql-copy.56+03efbd5ef6b9"
        document = deployment_document(old_selector)
        with tempfile.TemporaryDirectory() as raw:
            args = common_args(Path(raw) / "state.json")
            state = {
                "stateVersion": 1,
                "stage": "activated",
                "deployment": args.deployment,
                "deploymentUid": "uid-1",
                "container": args.container,
                "releaseVariable": args.release_variable,
                "commandIndex": args.command_index,
                "releaseAnnotation": args.release_annotation,
                "knownResumeThreadId": args.resume_thread_id,
                "oldReleaseSelector": old_selector,
                "newReleaseSelector": "0.151.0-remote-sql-copy.2+abcdef123456",
                "oldAnnotations": deploy.annotation_snapshot(
                    document, args.release_annotation
                ),
                "oldBinarySha256": "a" * 64,
                "oldVersionOutput": f"codex-cli {old_runtime}",
                "releaseDirectory": "/releases/new",
                "postgresBackup": {"directory": "/backups/new"},
            }
            deploy.atomic_write_json(args.state_file, state)
            with mock.patch.object(
                deploy, "deployment_document", return_value=document
            ), mock.patch.object(
                deploy, "select_ready_pod", return_value="old-pod"
            ), mock.patch.object(
                deploy, "verify_selected_release"
            ) as verify_release, mock.patch.object(
                deploy, "verify_daemon_running"
            ) as verify_daemon, mock.patch.object(
                deploy, "verify_known_resume"
            ), mock.patch("sys.stdout", new=io.StringIO()):
                deploy.rollback(args)
            verify_release.assert_called_once_with(
                args, "old-pod", old_selector, "a" * 64, old_runtime
            )
            verify_daemon.assert_called_once_with(args, "old-pod", old_runtime)

    def test_migration_manifest_locks_sha256_and_derives_sqlx_sha384(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            lines = []
            expected = []
            for version, relative in enumerate(deploy.MIGRATION_PATHS, 1):
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                content = f"-- migration {version}\nSELECT {version};\n".encode()
                path.write_bytes(content)
                lines.append(f"{hashlib.sha256(content).hexdigest()}  {relative}\n")
                expected.append((version, hashlib.sha384(content).hexdigest()))
            manifest = root / ".github" / "remote-sql-migrations.sha256"
            manifest.parent.mkdir()
            manifest.write_text("".join(lines), encoding="utf-8")
            self.assertEqual(
                deploy.expected_sqlx_migrations(root, manifest), expected
            )

    def test_live_sqlx_migrations_may_be_a_locked_release_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            live = root / "live.txt"
            expected = root / "expected.txt"
            live.write_text("1:aaa\n2:bbb\n", encoding="utf-8")
            expected.write_text("1:aaa\n2:bbb\n3:ccc\n", encoding="utf-8")
            deploy.require_sqlx_migration_prefix(live, expected)

    def test_live_sqlx_migration_prefix_rejects_drift_and_extra_versions(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            live = root / "live.txt"
            expected = root / "expected.txt"
            expected.write_text("1:aaa\n2:bbb\n", encoding="utf-8")
            for content in ("", "1:aaa\n2:changed\n", "1:aaa\n2:bbb\n3:ccc\n"):
                live.write_text(content, encoding="utf-8")
                with self.assertRaises(deploy.DeployError):
                    deploy.require_sqlx_migration_prefix(live, expected)

    def test_archive_rejects_traversal_and_extra_members(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            archive = root / "bad.tar.gz"
            with tarfile.open(archive, "w:gz") as output:
                info = tarfile.TarInfo("../codex")
                payload = b"binary"
                info.size = len(payload)
                output.addfile(info, io.BytesIO(payload))
            with self.assertRaises(deploy.DeployError):
                deploy.extract_release_archive(archive, root / "out")

    def test_parse_sha256s_rejects_duplicate_asset(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "SHA256SUMS.txt"
            digest = "a" * 64
            path.write_text(f"{digest}  codex\n{digest}  codex\n", encoding="utf-8")
            with self.assertRaises(deploy.DeployError):
                deploy.parse_sha256s(path)

    def test_release_download_rejects_any_extra_draft_asset(self) -> None:
        args = argparse.Namespace(
            github_token_env="GITHUB_TOKEN",
            repository="owner/repository",
            release_id=42,
            release_tag="v0.151.0-remote-sql-copy.1",
            archive_name="release.tar.gz",
            sums_name="SHA256SUMS.txt",
            provenance_name="PROVENANCE.txt",
        )
        names = [
            args.archive_name,
            args.sums_name,
            args.provenance_name,
            "unexpected.txt",
        ]
        release = {
            "id": args.release_id,
            "draft": True,
            "tag_name": args.release_tag,
            "assets": [
                {"id": index, "name": name}
                for index, name in enumerate(names, 1)
            ],
        }
        with tempfile.TemporaryDirectory() as raw:
            with mock.patch.dict(deploy.os.environ, {"GITHUB_TOKEN": "test-token"}), mock.patch.object(
                deploy, "github_json", return_value=release
            ), mock.patch.object(deploy, "download_url") as download:
                with self.assertRaisesRegex(deploy.DeployError, "asset set"):
                    deploy.release_assets(args, Path(raw))
        download.assert_not_called()

    def test_release_download_uses_release_id_endpoint(self) -> None:
        args = argparse.Namespace(
            github_token_env="GITHUB_TOKEN",
            repository="owner/repository",
            release_id=42,
            release_tag="v0.151.0-remote-sql-copy.1",
            archive_name="release.tar.gz",
            sums_name="SHA256SUMS.txt",
            provenance_name="PROVENANCE.txt",
        )
        release = {
            "id": args.release_id,
            "draft": True,
            "tag_name": args.release_tag,
            "assets": [
                {"id": 1, "name": args.archive_name},
                {"id": 2, "name": args.sums_name},
                {"id": 3, "name": args.provenance_name},
            ],
        }
        with tempfile.TemporaryDirectory() as raw:
            with mock.patch.dict(deploy.os.environ, {"GITHUB_TOKEN": "test-token"}), mock.patch.object(
                deploy, "github_json", return_value=release
            ) as github_json, mock.patch.object(deploy, "download_url"):
                deploy.release_assets(args, Path(raw))
        github_json.assert_called_once_with(
            "https://api.github.com/repos/owner/repository/releases/42",
            "test-token",
        )

    def test_release_download_rejects_mismatched_release_id(self) -> None:
        args = argparse.Namespace(
            github_token_env="GITHUB_TOKEN",
            repository="owner/repository",
            release_id=42,
            release_tag="v0.151.0-remote-sql-copy.1",
            archive_name="release.tar.gz",
            sums_name="SHA256SUMS.txt",
            provenance_name="PROVENANCE.txt",
        )
        release = {
            "id": 43,
            "draft": True,
            "tag_name": args.release_tag,
            "assets": [],
        }
        with tempfile.TemporaryDirectory() as raw:
            with mock.patch.dict(deploy.os.environ, {"GITHUB_TOKEN": "test-token"}), mock.patch.object(
                deploy, "github_json", return_value=release
            ), mock.patch.object(deploy, "download_url") as download:
                with self.assertRaisesRegex(deploy.DeployError, "Release id"):
                    deploy.release_assets(args, Path(raw))
        download.assert_not_called()

    def test_backup_uses_postgres_pod_and_stages_verified_files(self) -> None:
        args = argparse.Namespace(
            namespace="codex-internal",
            container="workspace",
            release_selector="0.151.0-remote-sql-copy.1+150745544e68",
            run_id="123",
            run_attempt="1",
            postgres_pod="codex-postgres-0",
            postgres_container="postgres",
            pg_backup_root="/backups",
            pg_backup_timeout=60,
        )
        checksum = "a" * 96

        def materialize(
            _args: argparse.Namespace,
            _pod: str,
            _container: str,
            source: str,
            destination: Path,
        ) -> None:
            if source.endswith(".list"):
                destination.write_text("restore-entry\n", encoding="utf-8")
            elif source.endswith(".sha256"):
                destination.write_text("b" * 64 + "\n", encoding="utf-8")
            else:
                destination.write_text(f"1:{checksum}\n", encoding="utf-8")

        safe = mock.Mock(
            stdout=(
                "BACKUP_DIR=/backups/release\n"
                + "BACKUP_SHA256="
                + "b" * 64
                + "\nBACKUP_BYTES=11\nRESTORE_LIST_ENTRIES=1\n"
            )
        )
        def remote_result(
            _args: argparse.Namespace,
            _pod: str,
            script: str,
            script_args: list[str],
            **_kwargs: object,
        ) -> mock.Mock:
            if "BACKUP_STAGE=" in script:
                return mock.Mock(stdout=f"BACKUP_STAGE={script_args[2]}\n")
            return safe

        with mock.patch.object(deploy, "postgres_exec") as postgres_exec, mock.patch.object(
            deploy, "copy_from_pod", side_effect=materialize
        ) as copy_from, mock.patch.object(deploy, "copy_to_pod") as copy_to, mock.patch.object(
            deploy, "remote_exec", side_effect=remote_result
        ) as remote_exec, mock.patch.object(deploy, "stream_pod_file") as stream:
            result = deploy.backup_postgres(
                args,
                "workspace-pod",
                {"metadata": {}},
                {"backupFormatVersion": 1},
                [(1, checksum)],
            )
        self.assertEqual(result["BACKUP_BYTES"], "11")
        self.assertEqual(postgres_exec.call_count, 2)
        self.assertEqual(copy_from.call_count, 3)
        self.assertEqual(copy_to.call_count, 6)
        self.assertEqual(remote_exec.call_count, 2)
        stream.assert_called_once()

    def test_stream_pod_file_pipes_without_runner_staging(self) -> None:
        args = argparse.Namespace(
            namespace="codex-internal",
            pg_backup_timeout=60,
        )
        source_stdout = mock.Mock()
        source_stderr = mock.Mock()
        source_stderr.read.return_value = b""
        source = mock.Mock(
            stdout=source_stdout,
            stderr=source_stderr,
            returncode=0,
        )
        source.wait.return_value = 0
        destination = mock.Mock(returncode=0)
        destination.communicate.return_value = (b"", b"")
        with mock.patch.object(
            deploy.subprocess, "Popen", side_effect=[source, destination]
        ) as popen:
            deploy.stream_pod_file(
                args,
                "postgres-pod",
                "postgres",
                "/tmp/source.dump",
                "workspace-pod",
                "workspace",
                "/backups/.staging/postgres.dump",
            )
        self.assertEqual(popen.call_count, 2)
        self.assertIs(
            popen.call_args_list[1].kwargs["stdin"],
            source_stdout,
        )
        destination.communicate.assert_called_once_with(timeout=60)
        source_stdout.close.assert_called_once()

    def test_parse_args_accepts_positive_release_id_for_prepare(self) -> None:
        argv = [
            "deploy_remote_sql_release.py",
            "prepare",
            "--namespace",
            "codex-internal",
            "--deployment",
            "codex-workspace-copy",
            "--container",
            "workspace",
            "--release-root",
            "/releases",
            "--pg-backup-root",
            "/backups",
            "--state-file",
            "/tmp/state.json",
            "--resume-thread-id",
            "known-thread",
            "--repository",
            "owner/repository",
            "--release-tag",
            "v0.151.0-remote-sql-copy.1",
            "--release-id",
            "42",
            "--postgres-pod",
            "codex-postgres-0",
            "--runtime-version",
            "0.151.0-remote-sql-copy.1+150745544e68",
            "--source-sha",
            "150745544e6841702655579c78e21803f9b2927b",
            "--release-selector",
            "copy-workspace-0.151.0",
            "--archive-name",
            "release.tar.gz",
            "--run-id",
            "123",
            "--run-attempt",
            "1",
        ]
        with mock.patch.object(sys, "argv", argv):
            args = deploy.parse_args()
        self.assertEqual(args.release_id, 42)

    def test_parse_args_rejects_non_positive_release_id_for_prepare(self) -> None:
        base_argv = [
            "deploy_remote_sql_release.py",
            "prepare",
            "--namespace",
            "codex-internal",
            "--deployment",
            "codex-workspace-copy",
            "--container",
            "workspace",
            "--release-root",
            "/releases",
            "--pg-backup-root",
            "/backups",
            "--state-file",
            "/tmp/state.json",
            "--resume-thread-id",
            "known-thread",
            "--repository",
            "owner/repository",
            "--release-tag",
            "v0.151.0-remote-sql-copy.1",
            "--postgres-pod",
            "codex-postgres-0",
            "--runtime-version",
            "0.151.0-remote-sql-copy.1+150745544e68",
            "--source-sha",
            "150745544e6841702655579c78e21803f9b2927b",
            "--release-selector",
            "copy-workspace-0.151.0",
            "--archive-name",
            "release.tar.gz",
            "--run-id",
            "123",
            "--run-attempt",
            "1",
        ]
        for invalid in ("0", "-1", "abc"):
            with self.subTest(release_id=invalid):
                argv = [*base_argv, "--release-id", invalid]
                with mock.patch.object(sys, "argv", argv):
                    with self.assertRaises(SystemExit):
                        deploy.parse_args()


if __name__ == "__main__":
    unittest.main()
