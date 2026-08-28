import argparse
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
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
    def test_publish_redownloads_and_revalidates_exact_assets(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        publish = workflow.split("  publish-release:\n", 1)[1]
        self.assertIn('gh release download "${EXPECTED_TAG}"', publish)
        self.assertIn("SHA256SUMS.txt entry set mismatch", publish)
        self.assertIn("archive codex SHA-256 mismatch", publish)
        self.assertIn("bundled and standalone provenance differ", publish)
        self.assertLess(
            publish.index('after_assets="$(asset_fingerprint)"'),
            publish.index('gh release edit "${EXPECTED_TAG}"'),
        )

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


if __name__ == "__main__":
    unittest.main()
