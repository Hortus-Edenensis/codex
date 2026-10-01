#!/usr/bin/env python3
"""Check release gating and execute the workflow's version selection without building."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import tomllib


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/remote-sql-release.yml"


def run_block(workflow, step_name):
    step = workflow.split(f"      - name: {step_name}\n", 1)[1]
    step = step.split("\n      - ", 1)[0]
    if "        run: |\n" not in step:
        return step.split("        run: ", 1)[1].splitlines()[0] + "\n"
    body = step.split("        run: |\n", 1)[1]
    lines = []
    for line in body.splitlines():
        if line and not line.startswith("          "):
            break
        lines.append(line)
    return textwrap.dedent("\n".join(lines)) + "\n"


def main():
    assert not (ROOT / ".github/workflows/remote-sql-build.yml").exists()
    workflow = WORKFLOW.read_text()
    assert "tags:\n      - 'v*-patch-*'" in workflow
    assert "branches:" not in workflow
    assert "deploy-copy-workspace" not in workflow
    assert "smoke-copy-workspace" not in workflow
    assert "--ignore-rust-version" not in workflow
    assert "continue-on-error" not in workflow
    assert "STABLE_GIT_COMMIT: ${{ github.sha }}" in workflow
    release = workflow.split("\n  release:\n", 1)[1]
    assert "needs: linux-cargo-build" in release
    assert (
        "github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v')"
        in release
    )
    assert "contents: write" in release
    assert "gh release create" in release and "gh release upload" in release
    assert "--clobber" not in release
    pg_test = run_block(workflow, "Test legacy PostgreSQL memory migration")
    assert (
        "psql -X --set ON_ERROR_STOP=1 --file postgres-thread-store/tests/legacy_generated_memories.sql"
        in pg_test
    )
    assert "image: postgres:17-alpine" in workflow
    assert "PGHOST: 127.0.0.1" in workflow
    assert "PGPORT: ${{ job.services.postgres.ports[5432] }}" in workflow
    assert "PGDATABASE: codex_release_test" in workflow
    assert "secrets." not in workflow
    assert workflow.index(
        "name: Test legacy PostgreSQL memory migration"
    ) < workflow.index("name: Build remote SQL targets")
    tests = run_block(workflow, "Test remote SQL targets")
    assert "just test --cargo-profile release" in tests
    for package in [
        "codex-postgres-thread-store",
        "codex-api",
        "codex-model-provider",
        "codex-responses-api-proxy",
        "codex-app-server-daemon",
    ]:
        assert f"-p {package}" in tests
    assert workflow.index("name: Test remote SQL targets") < workflow.index(
        "name: Build remote SQL targets"
    )
    caller_tests = run_block(workflow, "Test memory and thread callers")
    assert "just test --cargo-profile release --lib" in caller_tests
    for package in [
        "codex-core",
        "codex-state",
        "codex-thread-store",
        "codex-app-server",
    ]:
        assert f"-p {package}" in caller_tests
    assert "--lib" not in tests
    assert (
        workflow.index("name: Build remote SQL targets")
        < workflow.index("name: Test memory and thread callers")
        < workflow.index("name: Stage release artifact")
    )
    build = run_block(workflow, "Build remote SQL targets")
    assert "-p codex-cli -p codex-code-mode-host" in build
    assert "--bin codex --bin codex-code-mode-host" in build
    stage = run_block(workflow, "Stage release artifact")
    assert "build_codex_package.py" in stage
    assert '--package-version "${RELEASE_VERSION}"' in stage
    assert "REMOTE_SQL_BUILD_TAG" in stage and "SHA256SUMS" in stage
    assert 'test "${version##* }" = "${RELEASE_VERSION}"' in stage
    assert "codex-code-mode-host --help" in stage
    for step in re.findall(r"^      - name: (.+)$", workflow, re.MULTILINE):
        subprocess.run(
            ["bash", "-n"], input=run_block(workflow, step), text=True, check=True
        )

    version_script = run_block(workflow, "Embed release version").split(
        "cargo metadata", 1
    )[0]
    with tempfile.TemporaryDirectory() as temp:
        output = Path(temp) / "output"
        workspace = Path(temp) / "codex-rs"
        workspace.mkdir()
        (Path(temp) / "scripts").symlink_to(ROOT / "scripts", target_is_directory=True)
        cargo_path = workspace / "Cargo.toml"
        source = (ROOT / "codex-rs/Cargo.toml").read_text()
        original = tomllib.loads(source)
        env = {
            **os.environ,
            "CODEX_REPO_ROOT": str(ROOT),
            "CODEX_REMOTE_SQL_DEFAULT_VERSION_BASE": "0.159.3",
            "GITHUB_OUTPUT": str(output),
            "GITHUB_RUN_NUMBER": "42",
            "GITHUB_SHA": "a" * 40,
        }
        cases = [
            ("tag", "v0.159.3-patch-1", "", "0.159.3-patch-1"),
            ("tag", "v0.159.3-patch-1", "0.159.3-patch-1", "0.159.3-patch-1"),
            ("tag", "prefix-v0.159.3-patch-1", "", None),
            ("tag", "v0.159.3-patch-1-extra", "", None),
            ("tag", "v0.159.3-patch-1", "0.159.3-patch-2", None),
            ("tag", "v00.159.3-patch-1", "", None),
            ("branch", "release", "bad/version", None),
            ("branch", "release", "0.159.3-patch-2", "0.159.3-patch-2"),
            ("branch", "release", "", "0.159.3-remote-sql.42+aaaaaaaaaaaa"),
        ]
        for ref_type, ref, requested, expected in cases:
            output.write_text("")
            cargo_path.write_text(source)
            result = subprocess.run(
                ["bash", "-c", version_script],
                cwd=workspace,
                env={
                    **env,
                    "GITHUB_REF_TYPE": ref_type,
                    "GITHUB_REF_NAME": ref,
                    "INPUT_RELEASE_VERSION": requested,
                },
                capture_output=True,
                text=True,
            )
            assert (result.returncode == 0) == (expected is not None), (
                ref,
                result.stderr,
            )
            if expected is not None:
                assert output.read_text() == f"release_version={expected}\n"
                actual = tomllib.loads(cargo_path.read_text())
                assert actual["workspace"]["package"]["version"] == expected
                actual["workspace"]["package"]["version"] = original["workspace"][
                    "package"
                ]["version"]
                assert actual == original
            else:
                assert cargo_path.read_text() == source
        for base in ["0.0.0", "0.159.3"]:
            cargo_path.write_text(
                f'[workspace.package]\nversion = "{base}" # preserved\n[workspace.dependencies]\nexample = "{base}"\n'
            )
            subprocess.run(
                ["bash", "-c", version_script],
                cwd=workspace,
                env={
                    **env,
                    "GITHUB_REF_TYPE": "tag",
                    "GITHUB_REF_NAME": "v0.159.3-patch-1",
                    "INPUT_RELEASE_VERSION": "",
                },
                check=True,
                capture_output=True,
                text=True,
            )
            assert (
                cargo_path.read_text()
                == f'[workspace.package]\nversion = "0.159.3-patch-1" # preserved\n[workspace.dependencies]\nexample = "{base}"\n'
            )
        # Use prebuilt stand-ins to exercise the official package layout without a build or download.
        binary = Path(temp) / "stand-in"
        binary.write_text("#!/bin/sh\nexit 0\n")
        binary.chmod(0o755)
        archive_path = Path(temp) / "package.tar.gz"
        command = [
            sys.executable,
            str(ROOT / "scripts/build_codex_package.py"),
            "--target",
            "x86_64-unknown-linux-gnu",
            "--package-version",
            "0.159.3-patch-1",
            "--package-dir",
            str(Path(temp) / "package"),
            "--archive-output",
            str(archive_path),
        ]
        for flag in [
            "--entrypoint-bin",
            "--code-mode-host-bin",
            "--bwrap-bin",
            "--rg-bin",
            "--zsh-bin",
        ]:
            command.extend([flag, str(binary)])
        subprocess.run(command, env=env, check=True, capture_output=True, text=True)
        with tarfile.open(archive_path) as archive:
            assert {
                "bin/codex",
                "bin/codex-code-mode-host",
                "codex-path/rg",
                "codex-resources/bwrap",
            } <= set(archive.getnames())
            manifest = json.load(archive.extractfile("codex-package.json"))
            assert manifest["version"] == "0.159.3-patch-1"
            assert manifest["entrypoint"] == "bin/codex"
    print(
        "release workflow checks passed: 9 version cases with real Cargo.toml + 2 base versions, isolated PostgreSQL gate, test/build/release gates, shell syntax, native package layout"
    )


if __name__ == "__main__":
    main()
