#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import time
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request
from urllib.request import urlopen


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,199}$")
DATABASE_URL_RE = re.compile(r"postgres(?:ql)?://[^\s'\"]+", re.IGNORECASE)
STATE_VERSION = 1
MIGRATION_PATHS = [
    "codex-rs/postgres-thread-store/migrations/0001_remote_sql_threads.sql",
    "codex-rs/postgres-thread-store/migrations/0002_normalize_thread_source_projection.sql",
    "codex-rs/postgres-thread-store/migrations/0003_remote_control_enrollments.sql",
    "codex-rs/postgres-thread-store/migrations/0004_thread_goals_full_state.sql",
    "codex-rs/postgres-thread-store/migrations/0005_threads_memory_mode.sql",
    "codex-rs/postgres-thread-store/migrations/0006_normalize_thread_history_mode.sql",
    "codex-rs/postgres-thread-store/migrations/0007_generated_memory_pipeline.sql",
]


class DeployError(RuntimeError):
    pass


def redact(text: str) -> str:
    return DATABASE_URL_RE.sub("[REDACTED_DATABASE_URL]", text)


def run(
    command: list[str],
    *,
    input_text: str | None = None,
    timeout: int = 300,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            command,
            input=input_text,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise DeployError(f"command failed to run: {redact(str(exc))}") from exc
    if check and result.returncode != 0:
        detail = redact((result.stderr or result.stdout).strip())
        raise DeployError(
            f"command exited with {result.returncode}: {detail or 'no diagnostic output'}"
        )
    return result


def kubectl(args: argparse.Namespace, *parts: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return run(["kubectl", "-n", args.namespace, *parts], **kwargs)


def kubectl_json(args: argparse.Namespace, *parts: str) -> dict[str, Any]:
    result = kubectl(args, *parts, "-o", "json")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise DeployError("kubectl returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise DeployError("kubectl JSON response was not an object")
    return payload


def require_safe_name(label: str, value: str) -> str:
    if not SAFE_NAME_RE.fullmatch(value):
        raise DeployError(f"{label} is not a safe release identifier")
    return value


def positive_int_arg(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def deployment_document(args: argparse.Namespace) -> dict[str, Any]:
    deployment = kubectl_json(args, "get", "deployment", args.deployment)
    require_supported_deployment_shape(deployment)
    return deployment


def require_supported_deployment_shape(deployment: dict[str, Any]) -> None:
    spec = deployment.get("spec")
    if not isinstance(spec, dict):
        raise DeployError("deployment spec is missing")
    if spec.get("replicas") != 1:
        raise DeployError("remote SQL release requires exactly one Deployment replica")
    strategy = spec.get("strategy")
    if not isinstance(strategy, dict) or strategy.get("type") != "Recreate":
        raise DeployError("remote SQL release requires Deployment strategy.type Recreate")


def container_index_and_script(
    deployment: dict[str, Any], container_name: str, command_index: int
) -> tuple[int, str]:
    containers = (
        deployment.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers", [])
    )
    if not isinstance(containers, list):
        raise DeployError("deployment containers are missing")
    for container_index, container in enumerate(containers):
        if not isinstance(container, dict) or container.get("name") != container_name:
            continue
        command = container.get("command")
        if not isinstance(command, list) or command_index >= len(command):
            raise DeployError(
                f"container {container_name} does not have command[{command_index}]"
            )
        script = command[command_index]
        if not isinstance(script, str):
            raise DeployError(f"container command[{command_index}] is not a string")
        return container_index, script
    raise DeployError(f"deployment does not contain container {container_name}")


def release_assignment_pattern(variable_name: str) -> re.Pattern[str]:
    return re.compile(
        rf"(?m)^(?P<prefix>[ \t]*export[ \t]+{re.escape(variable_name)}=)"
        r"(?P<quote>['\"]?)(?P<value>[A-Za-z0-9][A-Za-z0-9._+-]{0,199})(?P=quote)[ \t]*$"
    )


def release_from_script(script: str, variable_name: str) -> str:
    matches = list(release_assignment_pattern(variable_name).finditer(script))
    if len(matches) != 1:
        raise DeployError(
            f"startup script must contain exactly one literal export for {variable_name}"
        )
    return matches[0].group("value")


def replace_release_in_script(
    script: str, variable_name: str, expected_release: str, replacement_release: str
) -> str:
    pattern = release_assignment_pattern(variable_name)
    matches = list(pattern.finditer(script))
    if len(matches) != 1 or matches[0].group("value") != expected_release:
        raise DeployError("startup release assignment changed concurrently")
    match = matches[0]
    quote = match.group("quote")
    replacement = f"{match.group('prefix')}{quote}{replacement_release}{quote}"
    return script[: match.start()] + replacement + script[match.end() :]


def deployment_selector(deployment: dict[str, Any]) -> str:
    selector = deployment.get("spec", {}).get("selector", {})
    expressions = selector.get("matchExpressions", [])
    if expressions:
        raise DeployError("deployment matchExpressions are not supported by this release gate")
    labels = selector.get("matchLabels")
    if not isinstance(labels, dict) or not labels:
        raise DeployError("deployment has no matchLabels selector")
    pairs: list[str] = []
    for key, value in sorted(labels.items()):
        if not isinstance(key, str) or not isinstance(value, str):
            raise DeployError("deployment selector labels must be strings")
        pairs.append(f"{key}={value}")
    return ",".join(pairs)


def pod_release_value(
    pod: dict[str, Any],
    container_name: str,
    command_index: int,
    variable_name: str,
) -> str | None:
    containers = pod.get("spec", {}).get("containers", [])
    if not isinstance(containers, list):
        return None
    for container in containers:
        if not isinstance(container, dict) or container.get("name") != container_name:
            continue
        command = container.get("command")
        if not isinstance(command, list) or command_index >= len(command):
            return None
        script = command[command_index]
        if not isinstance(script, str):
            return None
        try:
            return release_from_script(script, variable_name)
        except DeployError:
            return None
    return None


def pod_is_ready(pod: dict[str, Any], container_name: str) -> bool:
    if pod.get("metadata", {}).get("deletionTimestamp") is not None:
        return False
    if pod.get("status", {}).get("phase") != "Running":
        return False
    statuses = pod.get("status", {}).get("containerStatuses", [])
    if not isinstance(statuses, list):
        return False
    return any(
        isinstance(status, dict)
        and status.get("name") == container_name
        and status.get("ready") is True
        for status in statuses
    )


def select_ready_pod(
    args: argparse.Namespace,
    deployment: dict[str, Any],
    expected_release: str,
    *,
    timeout: int = 300,
) -> str:
    selector = deployment_selector(deployment)
    deadline = time.monotonic() + timeout
    last_candidates: list[str] = []
    while True:
        pods = kubectl_json(args, "get", "pods", "-l", selector).get("items", [])
        if not isinstance(pods, list):
            raise DeployError("pod list response omitted items")
        candidates: list[str] = []
        for pod in pods:
            if not isinstance(pod, dict) or not pod_is_ready(pod, args.container):
                continue
            if (
                pod_release_value(
                    pod,
                    args.container,
                    args.command_index,
                    args.release_variable,
                )
                != expected_release
            ):
                continue
            name = pod.get("metadata", {}).get("name")
            if isinstance(name, str):
                candidates.append(name)
        last_candidates = sorted(candidates)
        if len(last_candidates) == 1:
            return last_candidates[0]
        if time.monotonic() >= deadline:
            raise DeployError(
                "expected exactly one ready pod for release selector "
                f"{expected_release}; found {last_candidates}"
            )
        time.sleep(5)


def github_json(url: str, token: str) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "codex-remote-sql-release",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urlopen(request, timeout=60) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise DeployError(f"GitHub API returned HTTP {exc.code}") from exc
    if not isinstance(payload, dict):
        raise DeployError("GitHub API response was not an object")
    return payload


def download_url(url: str, token: str, destination: Path) -> None:
    request = Request(
        url,
        headers={
            "Accept": "application/octet-stream",
            "Authorization": f"Bearer {token}",
            "User-Agent": "codex-remote-sql-release",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urlopen(request, timeout=1800) as response, destination.open("wb") as output:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)
    except HTTPError as exc:
        destination.unlink(missing_ok=True)
        raise DeployError(f"release asset download returned HTTP {exc.code}") from exc


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_sha256s(path: Path) -> dict[str, str]:
    entries: dict[str, str] = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            raise DeployError(f"invalid SHA256SUMS line {line_number}")
        digest, name = parts
        name = name.lstrip("*")
        if not SHA256_RE.fullmatch(digest) or not SAFE_NAME_RE.fullmatch(name):
            raise DeployError(f"invalid SHA256SUMS entry on line {line_number}")
        if name in entries:
            raise DeployError(f"duplicate SHA256SUMS entry for {name}")
        entries[name] = digest
    return entries


def verify_checksum(path: Path, expected: str) -> None:
    actual = sha256_file(path)
    if actual != expected:
        raise DeployError(f"SHA-256 mismatch for {path.name}")


def expected_sqlx_migrations(
    repository_root: Path, manifest_path: Path
) -> list[tuple[int, str]]:
    lines = manifest_path.read_text(encoding="utf-8").splitlines()
    if len(lines) != len(MIGRATION_PATHS):
        raise DeployError("remote SQL migration manifest must contain exactly seven entries")
    checksums: list[tuple[int, str]] = []
    for version, (line, expected_path) in enumerate(zip(lines, MIGRATION_PATHS), 1):
        parts = line.split(maxsplit=1)
        if len(parts) != 2 or parts[1].lstrip("*") != expected_path:
            raise DeployError(f"migration manifest entry {version} has the wrong path")
        sha256 = parts[0]
        if not SHA256_RE.fullmatch(sha256):
            raise DeployError(f"migration manifest entry {version} has an invalid SHA-256")
        path = repository_root / expected_path
        if not path.is_file():
            raise DeployError(f"migration source is missing: {expected_path}")
        verify_checksum(path, sha256)
        digest = hashlib.sha384(path.read_bytes()).hexdigest()
        checksums.append((version, digest))
    return checksums


def parse_provenance(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if separator != "=" or not key or not value:
            raise DeployError(f"invalid PROVENANCE.txt line {line_number}")
        if key in values:
            raise DeployError(f"duplicate provenance key {key}")
        values[key] = value
    return values


def extract_release_archive(archive_path: Path, output_dir: Path) -> tuple[Path, Path]:
    expected = {"codex", "PROVENANCE.txt"}
    seen: set[str] = set()
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive.getmembers():
            if member.name not in expected or not member.isfile() or member.name in seen:
                raise DeployError(f"unexpected archive member {member.name!r}")
            source = archive.extractfile(member)
            if source is None:
                raise DeployError(f"failed to read archive member {member.name}")
            destination = output_dir / member.name
            with destination.open("wb") as output:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk:
                        break
                    output.write(chunk)
            seen.add(member.name)
    if seen != expected:
        raise DeployError(f"release archive members were incomplete: {sorted(seen)}")
    binary = output_dir / "codex"
    binary.chmod(0o755)
    return binary, output_dir / "PROVENANCE.txt"


def release_assets(args: argparse.Namespace, directory: Path) -> dict[str, Path]:
    token = os.environ.get(args.github_token_env)
    if not token:
        raise DeployError(f"{args.github_token_env} is not set")
    api_root = "https://api.github.com"
    release = github_json(
        f"{api_root}/repos/{args.repository}/releases/{args.release_id}", token
    )
    if release.get("id") != args.release_id:
        raise DeployError("GitHub Release id did not match the requested release")
    if release.get("draft") is not True:
        raise DeployError("deployment requires an authenticated draft GitHub Release")
    if release.get("tag_name") != args.release_tag:
        raise DeployError("GitHub Release tag did not match the requested release")
    assets = release.get("assets", [])
    if not isinstance(assets, list):
        raise DeployError("GitHub Release assets were missing")
    by_name: dict[str, dict[str, Any]] = {}
    for asset in assets:
        if isinstance(asset, dict) and isinstance(asset.get("name"), str):
            if asset["name"] in by_name:
                raise DeployError(f"draft GitHub Release has duplicate asset {asset['name']}")
            by_name[asset["name"]] = asset
    required = [args.archive_name, args.sums_name, args.provenance_name]
    if set(by_name) != set(required):
        raise DeployError(
            "draft GitHub Release asset set does not exactly match the release bundle"
        )
    paths: dict[str, Path] = {}
    for name in required:
        asset = by_name.get(name)
        asset_id = asset.get("id") if asset is not None else None
        if not isinstance(asset_id, int):
            raise DeployError(f"draft GitHub Release is missing asset {name}")
        destination = directory / name
        download_url(
            f"{api_root}/repos/{args.repository}/releases/assets/{asset_id}",
            token,
            destination,
        )
        paths[name] = destination
    return paths


def remote_exec(
    args: argparse.Namespace,
    pod: str,
    script: str,
    script_args: list[str],
    *,
    timeout: int = 600,
) -> subprocess.CompletedProcess[str]:
    return kubectl(
        args,
        "exec",
        "-i",
        pod,
        "-c",
        args.container,
        "--",
        "sh",
        "-s",
        "--",
        *script_args,
        input_text=script,
        timeout=timeout,
    )


def copy_to_pod(
    args: argparse.Namespace,
    pod: str,
    source: Path,
    destination: str,
    *,
    timeout: int = 1800,
) -> None:
    kubectl(
        args,
        "cp",
        str(source),
        f"{pod}:{destination}",
        "-c",
        args.container,
        timeout=timeout,
    )


def copy_from_pod(
    args: argparse.Namespace,
    pod: str,
    container: str,
    source: str,
    destination: Path,
) -> None:
    kubectl(
        args,
        "cp",
        f"{pod}:{source}",
        str(destination),
        "-c",
        container,
        timeout=args.pg_backup_timeout,
    )


def postgres_exec(
    args: argparse.Namespace,
    script: str,
    script_args: list[str],
    *,
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    return kubectl(
        args,
        "exec",
        "-i",
        args.postgres_pod,
        "-c",
        args.postgres_container,
        "--",
        "sh",
        "-s",
        "--",
        *script_args,
        input_text=script,
        timeout=timeout,
    )


def stream_pod_file(
    args: argparse.Namespace,
    source_pod: str,
    source_container: str,
    source_path: str,
    destination_pod: str,
    destination_container: str,
    destination_path: str,
) -> None:
    source_command = [
        "kubectl",
        "-n",
        args.namespace,
        "exec",
        source_pod,
        "-c",
        source_container,
        "--",
        "cat",
        source_path,
    ]
    destination_script = r'''set -eu
umask 077
destination="$1"
[ ! -e "${destination}" ]
cat > "${destination}"
[ -s "${destination}" ]
'''
    destination_command = [
        "kubectl",
        "-n",
        args.namespace,
        "exec",
        "-i",
        destination_pod,
        "-c",
        destination_container,
        "--",
        "sh",
        "-c",
        destination_script,
        "sh",
        destination_path,
    ]
    try:
        source = subprocess.Popen(
            source_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as exc:
        raise DeployError("failed to start PostgreSQL backup source stream") from exc
    if source.stdout is None:
        source.terminate()
        source.wait(timeout=5)
        raise DeployError("source Pod stream did not expose stdout")
    try:
        destination = subprocess.Popen(
            destination_command,
            stdin=source.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as exc:
        source.stdout.close()
        source.terminate()
        source.wait(timeout=5)
        raise DeployError("failed to start PostgreSQL backup destination stream") from exc
    source.stdout.close()
    try:
        destination_stdout, destination_stderr = destination.communicate(
            timeout=args.pg_backup_timeout
        )
        source_returncode = source.wait(timeout=60)
        source_stderr = source.stderr.read() if source.stderr is not None else b""
    except subprocess.TimeoutExpired as exc:
        for process in (destination, source):
            process.terminate()
        for process in (destination, source):
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        raise DeployError("PostgreSQL backup stream timed out") from exc
    if destination.returncode != 0 or source_returncode != 0:
        diagnostic = destination_stderr or source_stderr or destination_stdout
        raise DeployError(
            "PostgreSQL backup stream failed: "
            + redact(diagnostic.decode("utf-8", errors="replace").strip())
        )


def parse_safe_output(text: str, required: set[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if separator == "=" and key in required:
            values[key] = value
    missing = required - values.keys()
    if missing:
        raise DeployError(f"remote command omitted safe outputs: {sorted(missing)}")
    return values


def install_immutable_release(
    args: argparse.Namespace,
    pod: str,
    binary: Path,
    provenance: Path,
    sums: Path,
    binary_sha256: str,
) -> str:
    remote_base = f"/tmp/codex-release-{os.getpid()}-{int(time.time())}"
    remote_binary = f"{remote_base}.bin"
    remote_provenance = f"{remote_base}.provenance"
    remote_sums = f"{remote_base}.sums"
    copy_to_pod(args, pod, binary, remote_binary)
    copy_to_pod(args, pod, provenance, remote_provenance)
    copy_to_pod(args, pod, sums, remote_sums)
    script = r'''set -eu
umask 022
release_root="$1"
release_selector="$2"
expected_sha="$3"
source_binary="$4"
source_provenance="$5"
source_sums="$6"
destination="${release_root}/${release_selector}"
stage="${release_root}/.staging-${release_selector}-$$"
cleanup() {
  rm -f "${source_binary}" "${source_provenance}" "${source_sums}"
  rm -rf "${stage}"
}
trap cleanup EXIT HUP INT TERM
mkdir -p "${release_root}"
actual_sha="$(sha256sum "${source_binary}" | awk '{print $1}')"
[ "${actual_sha}" = "${expected_sha}" ]
if [ -e "${destination}" ]; then
  [ -f "${destination}/bin/codex" ]
  installed_sha="$(sha256sum "${destination}/bin/codex" | awk '{print $1}')"
  [ "${installed_sha}" = "${expected_sha}" ]
  cmp -s "${source_provenance}" "${destination}/PROVENANCE.txt"
  cmp -s "${source_sums}" "${destination}/SHA256SUMS.txt"
  printf 'RELEASE_DIR=%s\n' "${destination}"
  exit 0
fi
mkdir -p "${stage}/bin"
install -m 0755 "${source_binary}" "${stage}/bin/codex"
install -m 0644 "${source_provenance}" "${stage}/PROVENANCE.txt"
install -m 0644 "${source_sums}" "${stage}/SHA256SUMS.txt"
installed_sha="$(sha256sum "${stage}/bin/codex" | awk '{print $1}')"
[ "${installed_sha}" = "${expected_sha}" ]
mv "${stage}" "${destination}"
printf 'RELEASE_DIR=%s\n' "${destination}"
'''
    result = remote_exec(
        args,
        pod,
        script,
        [
            args.release_root,
            args.release_selector,
            binary_sha256,
            remote_binary,
            remote_provenance,
            remote_sums,
        ],
        timeout=1800,
    )
    values = parse_safe_output(result.stdout, {"RELEASE_DIR"})
    return values["RELEASE_DIR"]


def inspect_selected_release(
    args: argparse.Namespace, pod: str, release_selector: str
) -> dict[str, str]:
    script = r'''set -eu
release_root="$1"
release_selector="$2"
binary="${release_root}/${release_selector}/bin/codex"
selected=/home/codex/.codex/packages/standalone/current/bin/codex
[ -x "${binary}" ]
[ -x "${selected}" ]
binary_sha="$(sha256sum "${binary}" | awk '{print $1}')"
selected_sha="$(sha256sum "${selected}" | awk '{print $1}')"
[ "${binary_sha}" = "${selected_sha}" ]
version_output="$("${binary}" --version)"
case "${version_output}" in *"${release_selector}"*) ;; *) exit 1 ;; esac
printf 'BINARY_SHA256=%s\n' "${binary_sha}"
printf 'VERSION_OUTPUT=%s\n' "${version_output}"
'''
    result = remote_exec(
        args,
        pod,
        script,
        [args.release_root, release_selector],
        timeout=120,
    )
    values = parse_safe_output(result.stdout, {"BINARY_SHA256", "VERSION_OUTPUT"})
    if not SHA256_RE.fullmatch(values["BINARY_SHA256"]):
        raise DeployError("selected release returned an invalid SHA-256")
    if release_selector not in values["VERSION_OUTPUT"]:
        raise DeployError("selected release returned an unexpected version")
    return values


def backup_postgres(
    args: argparse.Namespace,
    pod: str,
    deployment: dict[str, Any],
    backup_metadata: dict[str, Any],
    sqlx_checksums: list[tuple[int, str]],
) -> dict[str, str]:
    backup_name = require_safe_name(
        "backup name",
        f"{args.release_selector}-run{args.run_id}-attempt{args.run_attempt}",
    )
    destination = f"{args.pg_backup_root}/{backup_name}"
    stage = f"{args.pg_backup_root}/.staging-{backup_name}-{os.getpid()}"
    postgres_base = f"/tmp/codex-pg-backup-{os.getpid()}-{int(time.time())}"
    postgres_paths = {
        "postgres.dump": f"{postgres_base}.dump",
        "pg_restore.list": f"{postgres_base}.list",
        "live-sqlx-migrations.txt": f"{postgres_base}.migrations",
        "source-postgres-dump.sha256": f"{postgres_base}.sha256",
    }
    create_script = r'''set -eu
umask 077
dump="$1"
restore_list="$2"
live_migrations="$3"
dump_sha="$4"
cleanup() {
  rm -f "${dump}" "${restore_list}" "${live_migrations}" "${dump_sha}"
}
fail() {
  printf 'POSTGRES_BACKUP_FAILED=%s\n' "$1" >&2
  exit 1
}
trap cleanup EXIT HUP INT TERM
command -v pg_dump >/dev/null 2>&1 || fail missing_pg_dump
command -v pg_restore >/dev/null 2>&1 || fail missing_pg_restore
command -v psql >/dev/null 2>&1 || fail missing_psql
[ -n "${POSTGRES_USER:-}" ] || fail missing_postgres_user
[ -n "${POSTGRES_PASSWORD:-}" ] || fail missing_postgres_password
[ -n "${POSTGRES_DB:-}" ] || fail missing_postgres_database
PGPASSWORD="${POSTGRES_PASSWORD}" pg_dump \
  -h 127.0.0.1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
  --format=custom --compress=6 --no-owner --no-privileges \
  --file="${dump}" >/dev/null || fail pg_dump
pg_restore --list "${dump}" > "${restore_list}" || fail pg_restore_list
PGPASSWORD="${POSTGRES_PASSWORD}" psql \
  -h 127.0.0.1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
  -XAtq -v ON_ERROR_STOP=1 \
  -c "SELECT version::text || ':' || encode(checksum, 'hex') FROM _sqlx_migrations WHERE version BETWEEN 1 AND 7 AND success ORDER BY version" \
  > "${live_migrations}" || fail sqlx_migrations
[ -s "${dump}" ] || fail empty_dump
[ -s "${restore_list}" ] || fail empty_restore_list
[ -s "${live_migrations}" ] || fail empty_sqlx_migrations
sha256sum "${dump}" | awk '{print $1}' > "${dump_sha}" || fail dump_sha256
[ "$(wc -l < "${dump_sha}" | tr -d ' ')" = 1 ] || fail dump_sha256_lines
trap - EXIT HUP INT TERM
printf 'POSTGRES_BACKUP_READY=1\n'
'''
    cleanup_postgres_script = r'''set -eu
rm -f "$1" "$2" "$3" "$4"
'''
    initialize_stage_script = r'''set -eu
umask 077
backup_root="$1"
destination="$2"
stage="$3"
[ ! -e "${destination}" ]
[ ! -e "${stage}" ]
mkdir -p "${backup_root}"
chmod 0700 "${backup_root}"
mkdir "${stage}"
chmod 0700 "${stage}"
printf 'BACKUP_STAGE=%s\n' "${stage}"
'''
    cleanup_stage_script = r'''set -eu
stage="$1"
case "${stage}" in */.staging-*) rm -rf "${stage}" ;; *) exit 1 ;; esac
'''
    finalize_script = r'''set -eu
umask 077
stage="$1"
destination="$2"
[ -d "${stage}" ]
[ ! -e "${destination}" ]
for name in \
  backup-metadata.json \
  deployment.json \
  expected-sqlx-migrations.txt \
  live-sqlx-migrations.txt \
  pg_restore.list \
  postgres.dump \
  source-postgres-dump.sha256; do
  [ -s "${stage}/${name}" ]
done
cmp -s "${stage}/expected-sqlx-migrations.txt" "${stage}/live-sqlx-migrations.txt"
expected_dump_sha="$(cat "${stage}/source-postgres-dump.sha256")"
case "${expected_dump_sha}" in *[!0-9a-f]*|'') exit 1 ;; esac
[ "${#expected_dump_sha}" = 64 ]
actual_dump_sha="$(sha256sum "${stage}/postgres.dump" | awk '{print $1}')"
[ "${actual_dump_sha}" = "${expected_dump_sha}" ]
(
  cd "${stage}"
  sha256sum \
    backup-metadata.json \
    deployment.json \
    expected-sqlx-migrations.txt \
    live-sqlx-migrations.txt \
    pg_restore.list \
    postgres.dump \
    source-postgres-dump.sha256 > SHA256SUMS.txt
  sha256sum --check --strict SHA256SUMS.txt >/dev/null
)
dump_bytes="$(wc -c < "${stage}/postgres.dump" | tr -d ' ')"
list_entries="$(wc -l < "${stage}/pg_restore.list" | tr -d ' ')"
mv "${stage}" "${destination}"
chmod 0700 "${destination}"
find "${destination}" -type f -exec chmod 0600 {} +
printf 'BACKUP_DIR=%s\n' "${destination}"
printf 'BACKUP_SHA256=%s\n' "${actual_dump_sha}"
printf 'BACKUP_BYTES=%s\n' "${dump_bytes}"
printf 'RESTORE_LIST_ENTRIES=%s\n' "${list_entries}"
'''
    with tempfile.TemporaryDirectory(prefix="codex-remote-sql-backup-") as raw_dir:
        directory = Path(raw_dir)
        local_paths = {
            "deployment.json": directory / "deployment.json",
            "backup-metadata.json": directory / "backup-metadata.json",
            "expected-sqlx-migrations.txt": directory
            / "expected-sqlx-migrations.txt",
            "live-sqlx-migrations.txt": directory / "live-sqlx-migrations.txt",
            "pg_restore.list": directory / "pg_restore.list",
            "source-postgres-dump.sha256": directory
            / "source-postgres-dump.sha256",
        }
        local_paths["deployment.json"].write_text(
            json.dumps(deployment, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        local_paths["backup-metadata.json"].write_text(
            json.dumps(backup_metadata, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        local_paths["expected-sqlx-migrations.txt"].write_text(
            "".join(f"{version}:{checksum}\n" for version, checksum in sqlx_checksums),
            encoding="utf-8",
        )
        postgres_exec(
            args,
            create_script,
            list(postgres_paths.values()),
            timeout=args.pg_backup_timeout,
        )
        try:
            for name in (
                "pg_restore.list",
                "live-sqlx-migrations.txt",
                "source-postgres-dump.sha256",
            ):
                copy_from_pod(
                    args,
                    args.postgres_pod,
                    args.postgres_container,
                    postgres_paths[name],
                    local_paths[name],
                )
            if (
                local_paths["live-sqlx-migrations.txt"].read_bytes()
                != local_paths["expected-sqlx-migrations.txt"].read_bytes()
            ):
                raise DeployError("live SQLx migrations did not match release sources")
            source_dump_sha = local_paths[
                "source-postgres-dump.sha256"
            ].read_text(encoding="utf-8").strip()
            if not SHA256_RE.fullmatch(source_dump_sha):
                raise DeployError("PostgreSQL source dump returned an invalid SHA-256")
            if local_paths["pg_restore.list"].stat().st_size <= 0:
                raise DeployError("PostgreSQL restore list was empty")
            stage_result = remote_exec(
                args,
                pod,
                initialize_stage_script,
                [args.pg_backup_root, destination, stage],
                timeout=120,
            )
            stage_values = parse_safe_output(stage_result.stdout, {"BACKUP_STAGE"})
            if stage_values["BACKUP_STAGE"] != stage:
                raise DeployError("workspace returned an unexpected backup stage")
            try:
                for name, source in local_paths.items():
                    copy_to_pod(
                        args,
                        pod,
                        source,
                        f"{stage}/{name}",
                        timeout=args.pg_backup_timeout,
                    )
                stream_pod_file(
                    args,
                    args.postgres_pod,
                    args.postgres_container,
                    postgres_paths["postgres.dump"],
                    pod,
                    args.container,
                    f"{stage}/postgres.dump",
                )
                result = remote_exec(
                    args,
                    pod,
                    finalize_script,
                    [stage, destination],
                    timeout=args.pg_backup_timeout,
                )
            except Exception:
                try:
                    remote_exec(
                        args,
                        pod,
                        cleanup_stage_script,
                        [stage],
                        timeout=120,
                    )
                except DeployError:
                    pass
                raise
        finally:
            postgres_exec(
                args,
                cleanup_postgres_script,
                list(postgres_paths.values()),
                timeout=120,
            )
    values = parse_safe_output(
        result.stdout,
        {"BACKUP_DIR", "BACKUP_SHA256", "BACKUP_BYTES", "RESTORE_LIST_ENTRIES"},
    )
    if not SHA256_RE.fullmatch(values["BACKUP_SHA256"]):
        raise DeployError("PostgreSQL backup returned an invalid SHA-256")
    for key in ("BACKUP_BYTES", "RESTORE_LIST_ENTRIES"):
        if not values[key].isdigit() or int(values[key]) <= 0:
            raise DeployError(f"PostgreSQL backup returned invalid {key}")
    return values


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    os.replace(temporary, path)


def read_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DeployError(f"failed to read deployment state {path}") from exc
    if not isinstance(payload, dict) or payload.get("stateVersion") != STATE_VERSION:
        raise DeployError("unsupported deployment state")
    return payload


def require_state_matches_args(args: argparse.Namespace, state: dict[str, Any]) -> None:
    expected = {
        "deployment": args.deployment,
        "container": args.container,
        "releaseVariable": args.release_variable,
        "commandIndex": args.command_index,
        "releaseAnnotation": args.release_annotation,
        "knownResumeThreadId": args.resume_thread_id,
    }
    for key, value in expected.items():
        if state.get(key) != value:
            raise DeployError(f"deployment state does not match --{key}")


def json_pointer_escape(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def annotation_snapshot(deployment: dict[str, Any], key: str) -> dict[str, Any]:
    locations = {
        "deployment": deployment.get("metadata", {}),
        "podTemplate": deployment.get("spec", {}).get("template", {}).get("metadata", {}),
    }
    snapshot: dict[str, Any] = {}
    for name, metadata in locations.items():
        if not isinstance(metadata, dict):
            raise DeployError(f"{name} metadata is missing")
        annotations = metadata.get("annotations")
        if annotations is None:
            snapshot[name] = {
                "mapPresent": False,
                "keyPresent": False,
                "value": None,
            }
            continue
        if not isinstance(annotations, dict):
            raise DeployError(f"{name} annotations are not an object")
        value = annotations.get(key)
        if value is not None and not isinstance(value, str):
            raise DeployError(f"{name} release annotation is not a string")
        snapshot[name] = {
            "mapPresent": True,
            "keyPresent": key in annotations,
            "value": value,
        }
    return snapshot


def require_annotation_snapshot(
    deployment: dict[str, Any], key: str, expected: dict[str, Any]
) -> None:
    if annotation_snapshot(deployment, key) != expected:
        raise DeployError("release annotations changed concurrently")


def annotation_patch_paths(key: str) -> dict[str, tuple[str, str]]:
    escaped = json_pointer_escape(key)
    return {
        "deployment": ("/metadata/annotations", f"/metadata/annotations/{escaped}"),
        "podTemplate": (
            "/spec/template/metadata/annotations",
            f"/spec/template/metadata/annotations/{escaped}",
        ),
    }


def activation_annotation_patch(
    key: str, old_snapshot: dict[str, Any], new_value: str
) -> list[dict[str, Any]]:
    patch: list[dict[str, Any]] = []
    for name, (map_path, key_path) in annotation_patch_paths(key).items():
        old = old_snapshot.get(name)
        if not isinstance(old, dict):
            raise DeployError("saved annotation snapshot is incomplete")
        if old.get("keyPresent") is True:
            old_value = old.get("value")
            if not isinstance(old_value, str):
                raise DeployError("saved release annotation is invalid")
            patch.extend(
                [
                    {"op": "test", "path": key_path, "value": old_value},
                    {"op": "replace", "path": key_path, "value": new_value},
                ]
            )
        elif old.get("mapPresent") is True:
            patch.append({"op": "add", "path": key_path, "value": new_value})
        else:
            patch.append({"op": "add", "path": map_path, "value": {key: new_value}})
    return patch


def rollback_annotation_patch(
    deployment: dict[str, Any],
    key: str,
    old_snapshot: dict[str, Any],
    new_value: str,
) -> list[dict[str, Any]]:
    current = annotation_snapshot(deployment, key)
    patch: list[dict[str, Any]] = []
    paths = annotation_patch_paths(key)
    metadata_locations = {
        "deployment": deployment.get("metadata", {}),
        "podTemplate": deployment.get("spec", {}).get("template", {}).get("metadata", {}),
    }
    for name, (map_path, key_path) in paths.items():
        saved = old_snapshot.get(name)
        now = current.get(name)
        if not isinstance(saved, dict) or not isinstance(now, dict):
            raise DeployError("saved annotation snapshot is incomplete")
        if now.get("keyPresent") is not True or now.get("value") != new_value:
            raise DeployError("release annotations no longer select the new release")
        patch.append({"op": "test", "path": key_path, "value": new_value})
        if saved.get("keyPresent") is True:
            old_value = saved.get("value")
            if not isinstance(old_value, str):
                raise DeployError("saved release annotation is invalid")
            patch.append({"op": "replace", "path": key_path, "value": old_value})
        elif saved.get("mapPresent") is True:
            patch.append({"op": "remove", "path": key_path})
        else:
            metadata = metadata_locations[name]
            annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
            if annotations != {key: new_value}:
                raise DeployError(
                    "cannot restore an absent annotation map after concurrent annotation changes"
                )
            patch.append({"op": "remove", "path": map_path})
    return patch


def prepare(args: argparse.Namespace) -> None:
    require_safe_name("release selector", args.release_selector)
    deployment = deployment_document(args)
    metadata = deployment.get("metadata", {})
    uid = metadata.get("uid")
    resource_version = metadata.get("resourceVersion")
    if not isinstance(uid, str) or not isinstance(resource_version, str):
        raise DeployError("deployment identity metadata is missing")
    _, old_command_script = container_index_and_script(
        deployment, args.container, args.command_index
    )
    old_release = release_from_script(old_command_script, args.release_variable)
    if old_release == args.release_selector:
        raise DeployError("deployment already selects the requested release")
    new_command_script = replace_release_in_script(
        old_command_script,
        args.release_variable,
        old_release,
        args.release_selector,
    )
    old_annotations = annotation_snapshot(deployment, args.release_annotation)
    for location in old_annotations.values():
        if location.get("keyPresent") is True and location.get("value") != old_release:
            raise DeployError(
                "release annotations disagree with the startup script selector"
            )
    old_pod = select_ready_pod(args, deployment, old_release)
    old_release_info = inspect_selected_release(args, old_pod, old_release)
    sqlx_checksums = expected_sqlx_migrations(
        args.repository_root.resolve(), args.migration_manifest.resolve()
    )
    with tempfile.TemporaryDirectory(prefix="codex-remote-sql-release-") as raw_dir:
        directory = Path(raw_dir)
        assets = release_assets(args, directory)
        sums = parse_sha256s(assets[args.sums_name])
        required_entries = {args.archive_name, args.provenance_name, "codex"}
        missing = required_entries - sums.keys()
        if missing:
            raise DeployError(f"SHA256SUMS.txt is missing {sorted(missing)}")
        verify_checksum(assets[args.archive_name], sums[args.archive_name])
        verify_checksum(assets[args.provenance_name], sums[args.provenance_name])
        binary, bundled_provenance = extract_release_archive(
            assets[args.archive_name], directory
        )
        if bundled_provenance.read_bytes() != assets[args.provenance_name].read_bytes():
            raise DeployError("bundled and standalone provenance files differ")
        verify_checksum(binary, sums["codex"])
        provenance = parse_provenance(assets[args.provenance_name])
        expected_provenance = {
            "release_tag": args.release_tag,
            "runtime_version": args.runtime_version,
            "source_sha": args.source_sha,
            "source_sha12": args.source_sha[:12],
            "target": "x86_64-unknown-linux-gnu",
        }
        for key, expected in expected_provenance.items():
            if provenance.get(key) != expected:
                raise DeployError(f"provenance mismatch for {key}")
        release_dir = install_immutable_release(
            args,
            old_pod,
            binary,
            assets[args.provenance_name],
            assets[args.sums_name],
            sums["codex"],
        )
    deployment_annotations = metadata.get("annotations")
    deployment_revision = (
        deployment_annotations.get("deployment.kubernetes.io/revision")
        if isinstance(deployment_annotations, dict)
        else None
    )
    backup_metadata = {
        "backupFormatVersion": 1,
        "deployment": args.deployment,
        "deploymentUid": uid,
        "deploymentResourceVersion": resource_version,
        "deploymentRevision": deployment_revision,
        "container": args.container,
        "postgresPod": args.postgres_pod,
        "postgresContainer": args.postgres_container,
        "commandIndex": args.command_index,
        "releaseVariable": args.release_variable,
        "releaseAnnotation": args.release_annotation,
        "oldReleaseSelector": old_release,
        "oldBinarySha256": old_release_info["BINARY_SHA256"],
        "oldVersionOutput": old_release_info["VERSION_OUTPUT"],
        "newReleaseSelector": args.release_selector,
        "newBinarySha256": sums["codex"],
        "releaseTag": args.release_tag,
        "sourceSha": args.source_sha,
        "runId": args.run_id,
        "runAttempt": args.run_attempt,
        "knownResumeThreadId": args.resume_thread_id,
    }
    backup = backup_postgres(
        args,
        old_pod,
        deployment,
        backup_metadata,
        sqlx_checksums,
    )
    state = {
        "stateVersion": STATE_VERSION,
        "stage": "prepared",
        "repository": args.repository,
        "releaseTag": args.release_tag,
        "runtimeVersion": args.runtime_version,
        "sourceSha": args.source_sha,
        "deployment": args.deployment,
        "deploymentUid": uid,
        "preparedResourceVersion": resource_version,
        "container": args.container,
        "releaseVariable": args.release_variable,
        "commandIndex": args.command_index,
        "releaseAnnotation": args.release_annotation,
        "knownResumeThreadId": args.resume_thread_id,
        "oldReleaseSelector": old_release,
        "newReleaseSelector": args.release_selector,
        "oldCommandScript": old_command_script,
        "newCommandScript": new_command_script,
        "oldAnnotations": old_annotations,
        "oldBinarySha256": old_release_info["BINARY_SHA256"],
        "oldVersionOutput": old_release_info["VERSION_OUTPUT"],
        "oldDeploymentRevision": deployment_revision,
        "sqlxMigrationChecksums": {
            str(version): checksum for version, checksum in sqlx_checksums
        },
        "oldPod": old_pod,
        "releaseDirectory": release_dir,
        "binarySha256": sums["codex"],
        "archiveSha256": sums[args.archive_name],
        "postgresBackup": {
            "directory": backup["BACKUP_DIR"],
            "sha256": backup["BACKUP_SHA256"],
            "bytes": int(backup["BACKUP_BYTES"]),
            "restoreListEntries": int(backup["RESTORE_LIST_ENTRIES"]),
        },
    }
    atomic_write_json(args.state_file, state)
    print(
        json.dumps(
            {
                "status": "prepared",
                "oldReleaseSelector": old_release,
                "newReleaseSelector": args.release_selector,
                "postgresBackupSha256": backup["BACKUP_SHA256"],
            },
            sort_keys=True,
        )
    )


def current_pod(args: argparse.Namespace) -> None:
    deployment = deployment_document(args)
    _, script = container_index_and_script(
        deployment, args.container, args.command_index
    )
    release = release_from_script(script, args.release_variable)
    pod = select_ready_pod(args, deployment, release)
    print(json.dumps({"pod": pod, "releaseSelector": release}, sort_keys=True))


def patch_selector(
    args: argparse.Namespace,
    deployment: dict[str, Any],
    state: dict[str, Any],
    *,
    rollback_patch: bool,
) -> None:
    metadata = deployment.get("metadata", {})
    resource_version = metadata.get("resourceVersion")
    if not isinstance(resource_version, str):
        raise DeployError("deployment resourceVersion is missing")
    container_index, current_script = container_index_and_script(
        deployment, args.container, args.command_index
    )
    old_script = state.get("oldCommandScript")
    new_script = state.get("newCommandScript")
    old_annotations = state.get("oldAnnotations")
    if not isinstance(old_script, str) or not isinstance(new_script, str):
        raise DeployError("saved startup command scripts are missing")
    if not isinstance(old_annotations, dict):
        raise DeployError("saved release annotations are missing")
    expected_script = new_script if rollback_patch else old_script
    replacement_script = old_script if rollback_patch else new_script
    if current_script != expected_script:
        raise DeployError("deployment startup command changed concurrently")
    command_path = (
        f"/spec/template/spec/containers/{container_index}/command/{args.command_index}"
    )
    patch = [
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": resource_version,
        },
        {"op": "test", "path": command_path, "value": expected_script},
        {"op": "replace", "path": command_path, "value": replacement_script},
    ]
    if rollback_patch:
        patch.extend(
            rollback_annotation_patch(
                deployment,
                args.release_annotation,
                old_annotations,
                str(state["newReleaseSelector"]),
            )
        )
    else:
        require_annotation_snapshot(
            deployment, args.release_annotation, old_annotations
        )
        patch.extend(
            activation_annotation_patch(
                args.release_annotation,
                old_annotations,
                str(state["newReleaseSelector"]),
            )
        )
    patch_json = json.dumps(patch, separators=(",", ":"))
    kubectl(
        args,
        "patch",
        "deployment",
        args.deployment,
        "--type=json",
        "--dry-run=server",
        "--patch",
        patch_json,
    )
    kubectl(
        args,
        "patch",
        "deployment",
        args.deployment,
        "--type=json",
        "--patch",
        patch_json,
    )


def rollout_status(args: argparse.Namespace) -> None:
    kubectl(
        args,
        "rollout",
        "status",
        f"deployment/{args.deployment}",
        f"--timeout={args.rollout_timeout}s",
        timeout=args.rollout_timeout + 30,
    )


def verify_selected_release(
    args: argparse.Namespace, pod: str, selector: str, expected_sha256: str, runtime: str
) -> None:
    script = r'''set -eu
release_root="$1"
release_selector="$2"
expected_sha="$3"
runtime_version="$4"
binary="${release_root}/${release_selector}/bin/codex"
[ -x "${binary}" ]
actual_sha="$(sha256sum "${binary}" | awk '{print $1}')"
[ "${actual_sha}" = "${expected_sha}" ]
direct_version="$("${binary}" --version)"
case "${direct_version}" in *"${runtime_version}"*) ;; *) exit 1 ;; esac
selected=/home/codex/.codex/packages/standalone/current/bin/codex
[ -x "${selected}" ]
selected_version="$("${selected}" --version)"
case "${selected_version}" in *"${runtime_version}"*) ;; *) exit 1 ;; esac
printf 'BINARY_SHA256=%s\n' "${actual_sha}"
printf 'RUNTIME_VERSION=%s\n' "${runtime_version}"
'''
    result = remote_exec(
        args,
        pod,
        script,
        [args.release_root, selector, expected_sha256, runtime],
        timeout=120,
    )
    values = parse_safe_output(result.stdout, {"BINARY_SHA256", "RUNTIME_VERSION"})
    if values["BINARY_SHA256"] != expected_sha256:
        raise DeployError("selected release SHA-256 verification failed")
    if values["RUNTIME_VERSION"] != runtime:
        raise DeployError("selected release version verification failed")


def verify_daemon_running(
    args: argparse.Namespace, pod: str, expected_runtime: str
) -> None:
    script = r'''set -eu
expected_runtime="$1"
codex=/home/codex/.codex/packages/standalone/current/bin/codex
output="$(env HOME=/home/codex USER=codex LOGNAME=codex \
  su -s /bin/bash -m codex -c "${codex} app-server daemon version")"
case "${output}" in *"${expected_runtime}"*) ;; *) exit 1 ;; esac
printf 'DAEMON_RUNNING=1\n'
'''
    result = remote_exec(args, pod, script, [expected_runtime], timeout=120)
    values = parse_safe_output(result.stdout, {"DAEMON_RUNNING"})
    if values["DAEMON_RUNNING"] != "1":
        raise DeployError("app-server daemon is not running the expected release")


def verify_known_resume(args: argparse.Namespace, pod: str) -> None:
    smoke_script = Path(__file__).with_name("copy_workspace_smoke.py")
    if not smoke_script.is_file():
        raise DeployError("copy workspace smoke helper is missing")
    result = run(
        [
            sys.executable,
            str(smoke_script),
            "verify-known",
            "--namespace",
            args.namespace,
            "--pod",
            pod,
            "--container",
            args.container,
            "--resume-thread-id",
            args.resume_thread_id,
        ],
        timeout=180,
    )
    try:
        summary = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise DeployError("known-thread resume helper returned invalid JSON") from exc
    if not isinstance(summary, dict) or summary.get("status") != "knownNonemptyResumePassed":
        raise DeployError("known-thread resume helper did not pass")


def activate(args: argparse.Namespace) -> None:
    state = read_state(args.state_file)
    require_state_matches_args(args, state)
    if state.get("stage") != "prepared":
        raise DeployError("deployment state is not prepared")
    deployment = deployment_document(args)
    if deployment.get("metadata", {}).get("uid") != state.get("deploymentUid"):
        raise DeployError("deployment UID changed after preparation")
    if deployment.get("metadata", {}).get("resourceVersion") != state.get(
        "preparedResourceVersion"
    ):
        raise DeployError("deployment resourceVersion changed after preparation")
    patch_selector(
        args,
        deployment,
        state,
        rollback_patch=False,
    )
    state["stage"] = "selectorPatched"
    atomic_write_json(args.state_file, state)
    rollout_status(args)
    deployment = deployment_document(args)
    new_pod = select_ready_pod(
        args,
        deployment,
        str(state["newReleaseSelector"]),
        timeout=args.rollout_timeout,
    )
    verify_selected_release(
        args,
        new_pod,
        str(state["newReleaseSelector"]),
        str(state["binarySha256"]),
        str(state["runtimeVersion"]),
    )
    state["stage"] = "activated"
    state["newPod"] = new_pod
    atomic_write_json(args.state_file, state)
    print(
        json.dumps(
            {
                "status": "activated",
                "newPod": new_pod,
                "releaseSelector": state["newReleaseSelector"],
            },
            sort_keys=True,
        )
    )


def rollback(args: argparse.Namespace) -> None:
    state = read_state(args.state_file)
    require_state_matches_args(args, state)
    deployment = deployment_document(args)
    if deployment.get("metadata", {}).get("uid") != state.get("deploymentUid"):
        raise DeployError("refusing rollback because deployment UID changed")
    _, current_script = container_index_and_script(
        deployment, args.container, args.command_index
    )
    current_release = release_from_script(current_script, args.release_variable)
    old_release = str(state["oldReleaseSelector"])
    new_release = str(state["newReleaseSelector"])
    if current_release == new_release:
        patch_selector(args, deployment, state, rollback_patch=True)
        state["stage"] = "rollbackSelectorPatched"
        atomic_write_json(args.state_file, state)
        rollout_status(args)
    elif current_release != old_release:
        raise DeployError(
            "refusing rollback because the selector no longer matches either release"
        )
    else:
        require_annotation_snapshot(
            deployment,
            args.release_annotation,
            state.get("oldAnnotations", {}),
        )
    deployment = deployment_document(args)
    old_pod = select_ready_pod(
        args, deployment, old_release, timeout=args.rollout_timeout
    )
    verify_selected_release(
        args,
        old_pod,
        old_release,
        str(state["oldBinarySha256"]),
        old_release,
    )
    if old_release not in str(state.get("oldVersionOutput", "")):
        raise DeployError("saved rollback version does not match the old selector")
    verify_daemon_running(args, old_pod, old_release)
    verify_known_resume(args, old_pod)
    state["stage"] = "rolledBack"
    state["rollbackPod"] = old_pod
    atomic_write_json(args.state_file, state)
    print(
        json.dumps(
            {
                "status": "rolledBack",
                "releaseSelector": old_release,
                "rollbackPod": old_pod,
                "preservedFailedRelease": state["releaseDirectory"],
                "preservedPostgresBackup": state["postgresBackup"]["directory"],
            },
            sort_keys=True,
        )
    )


def restart_daemon(args: argparse.Namespace) -> None:
    state = read_state(args.state_file)
    require_state_matches_args(args, state)
    if state.get("stage") != "activated":
        raise DeployError("daemon restart requires an activated release")
    deployment = deployment_document(args)
    pod = select_ready_pod(
        args,
        deployment,
        str(state["newReleaseSelector"]),
        timeout=args.rollout_timeout,
    )
    script = r'''set -eu
runtime_version="$1"
codex=/home/codex/.codex/packages/standalone/current/bin/codex
run_as_codex() {
  env HOME=/home/codex USER=codex LOGNAME=codex \
    su -s /bin/bash -m codex -c "$1"
}
run_as_codex "${codex} app-server daemon stop" >/dev/null 2>&1 || true
pkill -TERM -u codex -f 'app-server --listen unix://' >/dev/null 2>&1 || true
sleep 2
run_as_codex "${codex} app-server daemon start" >/dev/null
version_output="$(run_as_codex "${codex} app-server daemon version")"
case "${version_output}" in *"${runtime_version}"*) ;; *) exit 1 ;; esac
printf 'DAEMON_RESTARTED=1\n'
'''
    result = remote_exec(
        args,
        pod,
        script,
        [str(state["runtimeVersion"])],
        timeout=180,
    )
    parse_safe_output(result.stdout, {"DAEMON_RESTARTED"})
    print(json.dumps({"status": "daemonRestarted", "pod": pod}, sort_keys=True))


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--deployment", required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--release-variable", default="CODEX_STANDALONE_RELEASE")
    parser.add_argument("--command-index", type=int, default=2)
    parser.add_argument(
        "--release-annotation",
        default="codex.openai.com/remote-sql-release",
    )
    parser.add_argument("--release-root", required=True)
    parser.add_argument("--pg-backup-root", required=True)
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument("--resume-thread-id", required=True)
    parser.add_argument("--rollout-timeout", type=int, default=600)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy a verified draft GitHub Release by switching one Deployment selector."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare")
    add_common_arguments(prepare_parser)
    prepare_parser.add_argument("--repository", required=True)
    prepare_parser.add_argument("--release-tag", required=True)
    prepare_parser.add_argument("--release-id", type=positive_int_arg, required=True)
    prepare_parser.add_argument("--postgres-pod", required=True)
    prepare_parser.add_argument("--postgres-container", default="postgres")
    prepare_parser.add_argument("--runtime-version", required=True)
    prepare_parser.add_argument("--source-sha", required=True)
    prepare_parser.add_argument("--release-selector", required=True)
    prepare_parser.add_argument("--archive-name", required=True)
    prepare_parser.add_argument("--sums-name", default="SHA256SUMS.txt")
    prepare_parser.add_argument("--provenance-name", default="PROVENANCE.txt")
    prepare_parser.add_argument("--github-token-env", default="GITHUB_TOKEN")
    prepare_parser.add_argument("--run-id", required=True)
    prepare_parser.add_argument("--run-attempt", required=True)
    prepare_parser.add_argument("--pg-backup-timeout", type=int, default=1800)
    prepare_parser.add_argument("--repository-root", type=Path, default=Path("."))
    prepare_parser.add_argument(
        "--migration-manifest",
        type=Path,
        default=Path(".github/remote-sql-migrations.sha256"),
    )

    for command in ("current-pod", "activate", "rollback", "restart-daemon"):
        command_parser = subparsers.add_parser(command)
        add_common_arguments(command_parser)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command_index < 0:
        raise DeployError("command index must be non-negative")
    if args.command == "prepare":
        if not re.fullmatch(r"[0-9a-f]{40}", args.source_sha):
            raise DeployError("source SHA must be a full lowercase Git SHA")
        prepare(args)
    elif args.command == "current-pod":
        current_pod(args)
    elif args.command == "activate":
        activate(args)
    elif args.command == "rollback":
        rollback(args)
    elif args.command == "restart-daemon":
        restart_daemon(args)
    else:
        raise DeployError(f"unsupported command {args.command}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except DeployError as exc:
        print(f"release deploy failed: {redact(str(exc))}", file=sys.stderr)
        sys.exit(1)
