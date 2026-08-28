#!/usr/bin/env python3

import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import select
import struct
import subprocess
import sys
import time
from typing import Any, Callable


HANDSHAKE_TIMEOUT_SECONDS = 20.0
REQUEST_TIMEOUT_SECONDS = 60.0
READ_CHUNK_SIZE = 65536
SAFE_THREAD_STATUSES = {"notLoaded", "idle"}
ALL_SOURCE_KINDS = [
    "cli",
    "vscode",
    "exec",
    "appServer",
    "subAgent",
    "subAgentReview",
    "subAgentCompact",
    "subAgentThreadSpawn",
    "subAgentOther",
    "unknown",
]


class SmokeError(RuntimeError):
    pass


def redact(text: str) -> str:
    import re

    return re.sub(
        r"postgres(?:ql)?://[^\s'\"]+",
        "[REDACTED_DATABASE_URL]",
        text,
        flags=re.IGNORECASE,
    )


def run(
    command: list[str],
    *,
    input_text: str | None = None,
    timeout: int = 120,
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
        raise SmokeError(f"command failed to run: {redact(str(exc))}") from exc
    if result.returncode != 0:
        diagnostic = redact((result.stderr or result.stdout).strip())
        raise SmokeError(
            f"command exited with {result.returncode}: "
            f"{diagnostic or 'no diagnostic output'}"
        )
    return result


class ProxyWebSocketClient:
    def __init__(self, command: list[str], request_timeout_seconds: float) -> None:
        self.command = command
        self.request_timeout_seconds = request_timeout_seconds
        self.process: subprocess.Popen[bytes] | None = None
        self.buffer = bytearray()
        self.next_request_id = 1
        self.inbox: list[dict[str, Any]] = []

    def __enter__(self) -> "ProxyWebSocketClient":
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def start(self) -> None:
        self.process = subprocess.Popen(
            self.command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._perform_handshake()

    def close(self) -> None:
        if self.process is None:
            return
        try:
            if self.process.stdin is not None and not self.process.stdin.closed:
                try:
                    self._send_frame(0x8, b"")
                except Exception:
                    pass
                self.process.stdin.close()
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        finally:
            self.process = None

    def initialize(self) -> dict[str, Any]:
        response = self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "copy_workspace_release_smoke",
                    "title": "Copy Workspace Release Smoke",
                    "version": "0.0.0",
                },
                "capabilities": {
                    "experimentalApi": True,
                    "optOutNotificationMethods": [],
                },
            },
        )
        self.notify("initialized", {})
        return response

    def request(
        self, method: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        request_id = self.next_request_id
        self.next_request_id += 1
        payload: dict[str, Any] = {"method": method, "id": request_id}
        if params is not None:
            payload["params"] = params
        self.send_json(payload)
        deadline = time.monotonic() + self.request_timeout_seconds
        while True:
            message = self.recv_json(deadline)
            if message.get("id") == request_id:
                if "error" in message:
                    raise SmokeError(
                        f"{method} failed: "
                        f"{json.dumps(message['error'], ensure_ascii=True)}"
                    )
                result = message.get("result")
                if not isinstance(result, dict):
                    raise SmokeError(f"{method} returned a non-object result")
                return result
            if "method" in message and "id" in message:
                self._reject_server_request(message)
            elif "method" in message:
                self.inbox.append(message)
            elif "id" in message:
                raise SmokeError("proxy returned a response for an unknown request id")

    def wait_notification(
        self,
        method: str,
        predicate: Callable[[dict[str, Any]], bool],
        timeout_seconds: float,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while True:
            for index, message in enumerate(self.inbox):
                if message.get("method") == method and predicate(message):
                    return self.inbox.pop(index)
            message = self.recv_json(deadline)
            if "method" in message and "id" in message:
                self._reject_server_request(message)
                continue
            if "method" in message:
                if message.get("method") == method and predicate(message):
                    return message
                self.inbox.append(message)
                continue
            if "id" in message:
                raise SmokeError("proxy returned an unexpected response while waiting for an event")

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        payload: dict[str, Any] = {"method": method}
        if params is not None:
            payload["params"] = params
        self.send_json(payload)

    def send_json(self, payload: dict[str, Any]) -> None:
        self._send_frame(0x1, json.dumps(payload, separators=(",", ":")).encode())

    def recv_json(self, deadline: float) -> dict[str, Any]:
        message_opcode: int | None = None
        chunks: list[bytes] = []
        while True:
            final, opcode, payload = self._recv_frame(deadline)
            if opcode == 0x8:
                raise SmokeError("proxy websocket closed unexpectedly")
            if opcode == 0x9:
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:
                continue
            if opcode in (0x1, 0x2):
                if message_opcode is not None:
                    raise SmokeError("proxy started a message before finishing the prior message")
                message_opcode = opcode
                chunks = [payload]
            elif opcode == 0x0:
                if message_opcode is None:
                    raise SmokeError("proxy sent a continuation frame without a message")
                chunks.append(payload)
            else:
                raise SmokeError(f"unexpected websocket opcode {opcode}")
            if not final:
                continue
            if message_opcode != 0x1:
                raise SmokeError("proxy returned a non-text websocket message")
            try:
                message = json.loads(b"".join(chunks).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise SmokeError("proxy returned invalid JSON") from exc
            if not isinstance(message, dict):
                raise SmokeError("proxy JSON message was not an object")
            return message

    def _perform_handshake(self) -> None:
        if self.process is None or self.process.stdin is None:
            raise SmokeError("proxy process did not expose stdin")
        websocket_key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
        request = (
            "GET /rpc HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {websocket_key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii")
        self.process.stdin.write(request)
        self.process.stdin.flush()
        deadline = time.monotonic() + HANDSHAKE_TIMEOUT_SECONDS
        while b"\r\n\r\n" not in self.buffer:
            self._fill_buffer(deadline, 1)
        header_bytes, _, remainder = self.buffer.partition(b"\r\n\r\n")
        self.buffer = bytearray(remainder)
        first_line = header_bytes.decode("iso-8859-1").split("\r\n", 1)[0]
        if "101" not in first_line:
            raise SmokeError(f"websocket handshake failed: {first_line}")

    def _recv_frame(self, deadline: float) -> tuple[bool, int, bytes]:
        self._fill_buffer(deadline, 2)
        first, second = self.buffer[0], self.buffer[1]
        del self.buffer[:2]
        final = first & 0x80 != 0
        opcode = first & 0x0F
        payload_len = second & 0x7F
        if payload_len == 126:
            self._fill_buffer(deadline, 2)
            payload_len = struct.unpack("!H", self.buffer[:2])[0]
            del self.buffer[:2]
        elif payload_len == 127:
            self._fill_buffer(deadline, 8)
            payload_len = struct.unpack("!Q", self.buffer[:8])[0]
            del self.buffer[:8]
        mask = b""
        if second & 0x80:
            self._fill_buffer(deadline, 4)
            mask = bytes(self.buffer[:4])
            del self.buffer[:4]
        self._fill_buffer(deadline, payload_len)
        payload = bytes(self.buffer[:payload_len])
        del self.buffer[:payload_len]
        if mask:
            payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        return final, opcode, payload

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self.process is None or self.process.stdin is None:
            raise SmokeError("proxy process is not running")
        header = bytearray([0x80 | opcode])
        payload_len = len(payload)
        if payload_len < 126:
            header.append(0x80 | payload_len)
        elif payload_len < 1 << 16:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", payload_len))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", payload_len))
        mask = secrets.token_bytes(4)
        header.extend(mask)
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.process.stdin.write(bytes(header) + masked)
        self.process.stdin.flush()

    def _fill_buffer(self, deadline: float, min_bytes: int) -> None:
        while len(self.buffer) < min_bytes:
            if self.process is None or self.process.stdout is None:
                raise SmokeError("proxy process is not running")
            if self.process.poll() is not None:
                raise SmokeError(f"proxy process exited with {self.process.returncode}")
            timeout = deadline - time.monotonic()
            if timeout <= 0:
                raise SmokeError("timed out waiting for proxy websocket data")
            ready, _, _ = select.select([self.process.stdout], [], [], timeout)
            if not ready:
                raise SmokeError("timed out waiting for proxy websocket data")
            chunk = os.read(self.process.stdout.fileno(), READ_CHUNK_SIZE)
            if not chunk:
                raise SmokeError("proxy websocket closed while reading data")
            self.buffer.extend(chunk)

    def _reject_server_request(self, message: dict[str, Any]) -> None:
        self.send_json(
            {
                "id": message["id"],
                "error": {
                    "code": -32601,
                    "message": "release smoke client does not support server requests",
                },
            }
        )


def proxy_command(args: argparse.Namespace) -> list[str]:
    binary = "/home/codex/.codex/packages/standalone/current/bin/codex"
    shell = (
        "env HOME=/home/codex USER=codex LOGNAME=codex "
        f"su -s /bin/bash -m codex -c '{binary} app-server proxy'"
    )
    return [
        "kubectl",
        "-n",
        args.namespace,
        "exec",
        "-i",
        args.pod,
        "-c",
        args.container,
        "--",
        "sh",
        "-lc",
        shell,
    ]


def kubectl_shell(
    args: argparse.Namespace, script: str, script_args: list[str]
) -> subprocess.CompletedProcess[str]:
    return run(
        [
            "kubectl",
            "-n",
            args.namespace,
            "exec",
            "-i",
            args.pod,
            "-c",
            args.container,
            "--",
            "sh",
            "-s",
            "--",
            *script_args,
        ],
        input_text=script,
        timeout=120,
    )


def parse_safe_output(text: str, keys: set[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if separator and key in keys:
            values[key] = value
    if keys - values.keys():
        raise SmokeError(f"safe gate output omitted {sorted(keys - values.keys())}")
    return values


def paginated_threads(
    client: ProxyWebSocketClient, *, use_state_db_only: bool = False
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for archived in (False, True):
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            params: dict[str, Any] = {
                "limit": 100,
                "sortKey": "created_at",
                "sortDirection": "desc",
                "modelProviders": [],
                "sourceKinds": ALL_SOURCE_KINDS,
                "archived": archived,
                "useStateDbOnly": use_state_db_only,
            }
            if cursor is not None:
                params["cursor"] = cursor
            page = client.request("thread/list", params)
            data = page.get("data")
            if not isinstance(data, list):
                raise SmokeError("thread/list returned non-list data")
            for thread in data:
                if not isinstance(thread, dict):
                    raise SmokeError("thread/list returned a non-object thread")
                records.append(thread)
            next_cursor = page.get("nextCursor")
            if next_cursor is None:
                break
            if not isinstance(next_cursor, str) or next_cursor in seen_cursors:
                raise SmokeError("thread/list returned an invalid pagination cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
    return records


def paginated_thread_statuses(
    client: ProxyWebSocketClient,
    *,
    use_state_db_only: bool = False,
) -> tuple[int, dict[str, int]]:
    records = paginated_threads(client, use_state_db_only=use_state_db_only)
    status_counts: dict[str, int] = {}
    for thread in records:
        status = thread.get("status")
        status_type = status.get("type") if isinstance(status, dict) else None
        if not isinstance(status_type, str):
            raise SmokeError("thread/list returned a thread without status.type")
        status_counts[status_type] = status_counts.get(status_type, 0) + 1
    return len(records), status_counts


def all_thread_ids(client: ProxyWebSocketClient) -> set[str]:
    identifiers: set[str] = set()
    for thread in paginated_threads(client):
        thread_id = thread.get("id")
        if not isinstance(thread_id, str) or not thread_id:
            raise SmokeError("thread/list returned a thread without an id")
        if thread_id in identifiers:
            raise SmokeError("thread/list returned a duplicate thread id")
        identifiers.add(thread_id)
    return identifiers


def statuses_are_idle(status_counts: dict[str, int]) -> bool:
    return all(status in SAFE_THREAD_STATUSES for status in status_counts)


def process_and_job_gate(args: argparse.Namespace) -> tuple[int, int, int]:
    script = r'''set -eu
pid_file=/home/codex/.codex/app-server-daemon/app-server.pid
[ -r "${pid_file}" ]
daemon_pid="$(sed -n 's/.*"pid"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "${pid_file}")"
case "${daemon_pid}" in ''|*[!0-9]*) exit 1 ;; esac
kill -0 "${daemon_pid}"
descendants="$(ps -eo pid=,ppid= | awk -v root="${daemon_pid}" '
  { parent[$1] = $2 }
  END {
    count = 0
    for (pid in parent) {
      current = pid
      depth = 0
      while (current in parent && depth < 1024) {
        if (parent[current] == root) { count++; break }
        current = parent[current]
        depth++
      }
    }
    print count
  }')"
command -v psql >/dev/null 2>&1
[ -n "${CODEX_REMOTE_SQL_URL:-}" ]
counts="$(PGDATABASE="${CODEX_REMOTE_SQL_URL}" psql -XAtq \
  -v ON_ERROR_STOP=1 \
  -c "SELECT (SELECT count(*) FROM agent_jobs WHERE status = 'running'), (SELECT count(*) FROM agent_job_items WHERE status = 'running')")"
jobs="${counts%%|*}"
items="${counts#*|}"
case "${descendants}:${jobs}:${items}" in *[!0-9:]*) exit 1 ;; esac
printf 'DAEMON_DESCENDANTS=%s\n' "${descendants}"
printf 'RUNNING_AGENT_JOBS=%s\n' "${jobs}"
printf 'RUNNING_AGENT_JOB_ITEMS=%s\n' "${items}"
'''
    result = kubectl_shell(args, script, [])
    values = parse_safe_output(
        result.stdout,
        {"DAEMON_DESCENDANTS", "RUNNING_AGENT_JOBS", "RUNNING_AGENT_JOB_ITEMS"},
    )
    counts = tuple(
        int(values[key])
        for key in (
            "DAEMON_DESCENDANTS",
            "RUNNING_AGENT_JOBS",
            "RUNNING_AGENT_JOB_ITEMS",
        )
    )
    return counts


def persisted_memory_mode(args: argparse.Namespace, thread_id: str) -> str:
    script = r'''set -eu
thread_id="$1"
case "${thread_id}" in ''|*[!0-9a-f-]*) exit 1 ;; esac
command -v psql >/dev/null 2>&1
[ -n "${CODEX_REMOTE_SQL_URL:-}" ]
mode="$(PGDATABASE="${CODEX_REMOTE_SQL_URL}" psql -XAtq \
  -v ON_ERROR_STOP=1 -v thread_id="${thread_id}" \
  -c "SELECT memory_mode FROM threads WHERE id = :'thread_id'")"
case "${mode}" in enabled|disabled) ;; *) exit 1 ;; esac
printf 'MEMORY_MODE=%s\n' "${mode}"
'''
    result = kubectl_shell(args, script, [thread_id])
    return parse_safe_output(result.stdout, {"MEMORY_MODE"})["MEMORY_MODE"]


def wait_idle(args: argparse.Namespace) -> None:
    deadline = time.monotonic() + args.idle_timeout_seconds
    last_summary: dict[str, Any] = {}
    while True:
        with ProxyWebSocketClient(
            proxy_command(args), args.request_timeout_seconds
        ) as client:
            client.initialize()
            thread_count, status_counts = paginated_thread_statuses(
                client, use_state_db_only=True
            )
        descendants, running_jobs, running_items = process_and_job_gate(args)
        last_summary = {
            "threadCount": thread_count,
            "statusCounts": status_counts,
            "daemonDescendants": descendants,
            "runningAgentJobs": running_jobs,
            "runningAgentJobItems": running_items,
        }
        safe = (
            statuses_are_idle(status_counts)
            and descendants == 0
            and running_jobs == 0
            and running_items == 0
        )
        if safe:
            with ProxyWebSocketClient(
                proxy_command(args), args.request_timeout_seconds
            ) as client:
                client.initialize()
                verify_count, verify_statuses = paginated_thread_statuses(
                    client, use_state_db_only=False
                )
            verify_process = process_and_job_gate(args)
            if statuses_are_idle(verify_statuses) and verify_process == (0, 0, 0):
                print(
                    json.dumps(
                        {
                            "status": "idle",
                            "threadCount": verify_count,
                            "statusCounts": verify_statuses,
                            "daemonDescendants": 0,
                            "runningAgentJobs": 0,
                            "runningAgentJobItems": 0,
                        },
                        sort_keys=True,
                    )
                )
                return
        if time.monotonic() >= deadline:
            raise SmokeError(
                "active-turn gate did not become idle within timeout: "
                + json.dumps(last_summary, sort_keys=True)
            )
        time.sleep(args.idle_poll_seconds)


def turns_page(client: ProxyWebSocketClient, thread_id: str) -> list[dict[str, Any]]:
    page = client.request(
        "thread/turns/list",
        {
            "threadId": thread_id,
            "limit": 100,
            "sortDirection": "desc",
            "itemsView": "full",
        },
    )
    data = page.get("data")
    if not isinstance(data, list) or not data:
        raise SmokeError(f"thread {thread_id} has no persisted turns")
    if not all(isinstance(turn, dict) for turn in data):
        raise SmokeError("thread/turns/list returned an invalid turn")
    return data


def require_model_available(client: ProxyWebSocketClient, model_name: str) -> None:
    cursor: str | None = None
    seen_cursors: set[str] = set()
    matches: list[dict[str, Any]] = []
    while True:
        params: dict[str, Any] = {"limit": 100, "includeHidden": True}
        if cursor is not None:
            params["cursor"] = cursor
        page = client.request("model/list", params)
        data = page.get("data")
        if not isinstance(data, list):
            raise SmokeError("model/list returned non-list data")
        matches.extend(
            model
            for model in data
            if isinstance(model, dict)
            and (model.get("id") == model_name or model.get("model") == model_name)
        )
        next_cursor = page.get("nextCursor")
        if next_cursor is None:
            break
        if not isinstance(next_cursor, str) or next_cursor in seen_cursors:
            raise SmokeError("model/list returned an invalid pagination cursor")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    if not matches:
        raise SmokeError(f"model/list does not advertise {model_name}")


def require_started_model_provider(
    response: dict[str, Any], model_name: str, model_provider: str
) -> None:
    if response.get("model") != model_name:
        raise SmokeError("thread/start used an unexpected model")
    if response.get("modelProvider") != model_provider:
        raise SmokeError("thread/start used an unexpected model provider")


def require_thread(response: dict[str, Any], thread_id: str | None = None) -> dict[str, Any]:
    thread = response.get("thread")
    if not isinstance(thread, dict):
        raise SmokeError("thread response omitted the thread object")
    actual_id = thread.get("id")
    if not isinstance(actual_id, str) or not actual_id:
        raise SmokeError("thread response returned an invalid thread id")
    if thread_id is not None and actual_id != thread_id:
        raise SmokeError("thread response returned the wrong thread id")
    return thread


def require_deduplicated_fork(
    first: dict[str, Any], second: dict[str, Any], source_thread_id: str
) -> str:
    first_id = first.get("id")
    if not isinstance(first_id, str) or not first_id or first_id == source_thread_id:
        raise SmokeError("first fork returned an invalid thread id")
    if first.get("forkedFromId") != source_thread_id:
        raise SmokeError("fork did not record the expected source thread")
    if second.get("id") != first_id:
        raise SmokeError("identical repeated fork was not deduplicated")
    if second.get("forkedFromId") != source_thread_id:
        raise SmokeError("deduplicated fork lost source provenance")
    return first_id


def agent_message_text(item: Any) -> str | None:
    if not isinstance(item, dict) or item.get("type") != "agentMessage":
        return None
    text = item.get("text")
    return text if isinstance(text, str) else None


def turn_contains_exact_message(turn: dict[str, Any], sentinel: str) -> bool:
    items = turn.get("items")
    if not isinstance(items, list):
        return False
    messages = [text for item in items if (text := agent_message_text(item)) is not None]
    return messages == [sentinel]


def wait_for_turn(
    client: ProxyWebSocketClient, turn_id: str, sentinel: str, timeout: float
) -> None:
    messages: list[str] = []

    def completed_item(message: dict[str, Any]) -> bool:
        params = message.get("params")
        if not isinstance(params, dict) or params.get("turnId") != turn_id:
            return False
        text = agent_message_text(params.get("item"))
        if text is not None:
            messages.append(text)
        return False

    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SmokeError("Kimi smoke turn did not complete before timeout")
        for message in list(client.inbox):
            if message.get("method") == "item/completed":
                client.inbox.remove(message)
                completed_item(message)
        completed = client.wait_notification(
            "turn/completed",
            lambda message: isinstance(message.get("params"), dict)
            and isinstance(message["params"].get("turn"), dict)
            and message["params"]["turn"].get("id") == turn_id,
            remaining,
        )
        turn = completed["params"]["turn"]
        if turn.get("status") != "completed":
            raise SmokeError(f"Kimi smoke turn ended with status {turn.get('status')!r}")
        summary_message = turn.get("finalOutput")
        if isinstance(summary_message, str):
            messages.append(summary_message)
        if messages != [sentinel] and not turn_contains_exact_message(turn, sentinel):
            raise SmokeError("Kimi smoke turn did not return the exact sentinel")
        return


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    os.replace(temporary, path)


def read_state(path: Path) -> dict[str, Any]:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SmokeError("failed to read smoke state") from exc
    if not isinstance(state, dict) or state.get("stateVersion") != 1:
        raise SmokeError("unsupported smoke state")
    return state


def pre_restart(args: argparse.Namespace) -> None:
    if not args.resume_thread_id:
        raise SmokeError("--resume-thread-id is required")
    nonce = secrets.token_hex(12)
    sentinel = f"KIMI_SMOKE_OK:{nonce}"
    objective = f"remote SQL restart smoke {nonce}"
    created_threads: list[str] = []
    partial_state: dict[str, Any] = {
        "stateVersion": 1,
        "knownThreadId": args.resume_thread_id,
        "createdThreadIds": created_threads,
    }
    atomic_write_json(args.state_file, partial_state)
    with ProxyWebSocketClient(proxy_command(args), args.request_timeout_seconds) as client:
        client.initialize()
        turns_page(client, args.resume_thread_id)
        require_thread(
            client.request(
                "thread/resume",
                {"threadId": args.resume_thread_id, "excludeTurns": True},
            ),
            args.resume_thread_id,
        )
        known_goal = client.request(
            "thread/goal/get", {"threadId": args.resume_thread_id}
        )
        if "goal" not in known_goal:
            raise SmokeError("known thread goal response omitted goal")
        require_model_available(client, args.model)

        started = client.request(
            "thread/start",
            {
                "model": args.model,
                "modelProvider": args.model_provider,
                "cwd": args.cwd,
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "personality": "none",
            },
        )
        require_started_model_provider(started, args.model, args.model_provider)
        smoke_thread = require_thread(started)
        smoke_thread_id = str(smoke_thread["id"])
        created_threads.append(smoke_thread_id)
        partial_state["createdThreadIds"] = created_threads
        atomic_write_json(args.state_file, partial_state)
        started_turn = client.request(
            "turn/start",
            {
                "threadId": smoke_thread_id,
                "input": [
                    {
                        "type": "text",
                        "text": f"Reply with exactly {sentinel} and do not call tools.",
                    }
                ],
            },
        )
        turn = started_turn.get("turn")
        turn_id = turn.get("id") if isinstance(turn, dict) else None
        if not isinstance(turn_id, str) or not turn_id:
            raise SmokeError("turn/start returned an invalid turn id")
        wait_for_turn(client, turn_id, sentinel, args.turn_timeout_seconds)
        persisted_turns = turns_page(client, smoke_thread_id)
        if not any(turn.get("id") == turn_id for turn in persisted_turns):
            raise SmokeError("Kimi turn was not persisted")

        set_goal = client.request(
            "thread/goal/set",
            {
                "threadId": smoke_thread_id,
                "objective": objective,
                "status": "paused",
                "tokenBudget": 4096,
            },
        )
        goal = set_goal.get("goal")
        if not isinstance(goal, dict):
            raise SmokeError("thread/goal/set omitted goal")
        expected_goal = {
            "objective": objective,
            "status": "paused",
            "tokenBudget": 4096,
        }
        for key, value in expected_goal.items():
            if goal.get(key) != value:
                raise SmokeError(f"thread/goal/set returned the wrong {key}")
        if client.request(
            "thread/goal/get", {"threadId": smoke_thread_id}
        ).get("goal") != goal:
            raise SmokeError("thread goal did not round-trip before restart")
        client.request(
            "thread/memoryMode/set",
            {"threadId": smoke_thread_id, "mode": "enabled"},
        )
        if persisted_memory_mode(args, smoke_thread_id) != "enabled":
            raise SmokeError("enabled memory mode was not persisted")

        before_fork_ids = all_thread_ids(client)
        fork_params = {
            "threadId": smoke_thread_id,
            "excludeTurns": True,
            "deferGoalContinuation": True,
        }
        first_fork = require_thread(client.request("thread/fork", fork_params))
        first_fork_id = str(first_fork["id"])
        if first_fork_id not in created_threads and first_fork_id != smoke_thread_id:
            created_threads.append(first_fork_id)
            partial_state["createdThreadIds"] = created_threads
            atomic_write_json(args.state_file, partial_state)
        second_fork = require_thread(client.request("thread/fork", fork_params))
        second_fork_id = str(second_fork["id"])
        if second_fork_id not in created_threads and second_fork_id != smoke_thread_id:
            created_threads.append(second_fork_id)
            partial_state["createdThreadIds"] = created_threads
            atomic_write_json(args.state_file, partial_state)
        fork_id = require_deduplicated_fork(
            first_fork, second_fork, smoke_thread_id
        )
        turns_page(client, fork_id)
        after_fork_ids = all_thread_ids(client)
        if after_fork_ids - before_fork_ids != {fork_id}:
            raise SmokeError("identical repeated fork did not create exactly one thread")

    state = {
        "stateVersion": 1,
        "knownThreadId": args.resume_thread_id,
        "smokeThreadId": smoke_thread_id,
        "forkThreadId": fork_id,
        "turnId": turn_id,
        "sentinel": sentinel,
        "goal": expected_goal,
        "createdThreadIds": created_threads,
    }
    atomic_write_json(args.state_file, state)
    print(
        json.dumps(
            {
                "status": "preRestartPassed",
                "knownNonemptyResume": True,
                "newTask": True,
                "kimiTurn": True,
                "goal": True,
                "memoryModePersisted": True,
                "repeatedForkDeduplicated": True,
            },
            sort_keys=True,
        )
    )


def post_restart(args: argparse.Namespace) -> None:
    state = read_state(args.state_file)
    smoke_thread_id = str(state["smokeThreadId"])
    known_thread_id = str(state["knownThreadId"])
    sentinel = str(state["sentinel"])
    turn_id = str(state["turnId"])
    with ProxyWebSocketClient(proxy_command(args), args.request_timeout_seconds) as client:
        client.initialize()
        require_thread(
            client.request(
                "thread/resume", {"threadId": smoke_thread_id, "excludeTurns": True}
            ),
            smoke_thread_id,
        )
        turns = turns_page(client, smoke_thread_id)
        matching_turn = next((turn for turn in turns if turn.get("id") == turn_id), None)
        if not isinstance(matching_turn, dict) or not turn_contains_exact_message(
            matching_turn, sentinel
        ):
            raise SmokeError("Kimi turn content did not survive daemon restart")
        goal = client.request(
            "thread/goal/get", {"threadId": smoke_thread_id}
        ).get("goal")
        if not isinstance(goal, dict):
            raise SmokeError("goal did not survive daemon restart")
        for key, expected in state["goal"].items():
            if goal.get(key) != expected:
                raise SmokeError(f"goal {key} did not survive daemon restart")
        client.request(
            "thread/memoryMode/set",
            {"threadId": smoke_thread_id, "mode": "disabled"},
        )
        if persisted_memory_mode(args, smoke_thread_id) != "disabled":
            raise SmokeError("disabled memory mode was not persisted after restart")
        client.request(
            "thread/memoryMode/set",
            {"threadId": smoke_thread_id, "mode": "enabled"},
        )
        if persisted_memory_mode(args, smoke_thread_id) != "enabled":
            raise SmokeError("enabled memory mode was not persisted after restart")
        fork_id = str(state["forkThreadId"])
        fork_thread = require_thread(
            client.request(
                "thread/read", {"threadId": fork_id, "includeTurns": False}
            ),
            fork_id,
        )
        if fork_thread.get("forkedFromId") != smoke_thread_id:
            raise SmokeError("fork provenance did not survive daemon restart")
        turns_page(client, fork_id)
        require_thread(
            client.request(
                "thread/resume", {"threadId": known_thread_id, "excludeTurns": True}
            ),
            known_thread_id,
        )
        turns_page(client, known_thread_id)
    print(
        json.dumps(
            {
                "status": "postRestartPassed",
                "knownNonemptyResume": True,
                "newTaskResume": True,
                "kimiTurnPersisted": True,
                "goalPersisted": True,
                "memoryModePersistedAfterRestart": True,
                "deduplicatedForkPersisted": True,
            },
            sort_keys=True,
        )
    )


def cleanup(args: argparse.Namespace) -> None:
    state = read_state(args.state_file)
    failures: list[str] = []
    with ProxyWebSocketClient(proxy_command(args), args.request_timeout_seconds) as client:
        client.initialize()
        for thread_id in reversed(state.get("createdThreadIds", [])):
            try:
                client.request("thread/delete", {"threadId": thread_id})
            except SmokeError:
                failures.append(str(thread_id))
    if failures:
        raise SmokeError(f"failed to delete {len(failures)} smoke threads")
    print(
        json.dumps(
            {"status": "cleaned", "threadCount": len(state.get("createdThreadIds", []))},
            sort_keys=True,
        )
    )


def verify_known(args: argparse.Namespace) -> None:
    with ProxyWebSocketClient(proxy_command(args), args.request_timeout_seconds) as client:
        client.initialize()
        turns_page(client, args.resume_thread_id)
        require_thread(
            client.request(
                "thread/resume",
                {"threadId": args.resume_thread_id, "excludeTurns": True},
            ),
            args.resume_thread_id,
        )
        turns_page(client, args.resume_thread_id)
    print(
        json.dumps(
            {"status": "knownNonemptyResumePassed", "threadId": args.resume_thread_id},
            sort_keys=True,
        )
    )


def resolve_known(args: argparse.Namespace) -> None:
    with ProxyWebSocketClient(proxy_command(args), args.request_timeout_seconds) as client:
        client.initialize()
        scanned_threads = paginated_threads(client, use_state_db_only=False)
        candidate_ids = [args.resume_thread_id]
        candidate_ids.extend(
            str(thread["id"])
            for thread in scanned_threads
            if isinstance(thread.get("id"), str)
            and thread.get("id") != args.resume_thread_id
        )
        selected: str | None = None
        for candidate_id in candidate_ids:
            try:
                turns_page(client, candidate_id)
            except SmokeError:
                continue
            selected = candidate_id
            break
        if selected is None:
            raise SmokeError("no existing thread has readable nonempty full turn history")
    print(
        json.dumps(
            {
                "status": "knownNonemptyHistoryResolved",
                "threadId": selected,
                "fallbackUsed": selected != args.resume_thread_id,
            },
            sort_keys=True,
        )
    )


def add_kube_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--pod", required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument(
        "--request-timeout-seconds", type=float, default=REQUEST_TIMEOUT_SECONDS
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fail-closed PostgreSQL copy-workspace release gates and smoke tests."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    idle = subparsers.add_parser("wait-idle")
    add_kube_arguments(idle)
    idle.add_argument("--idle-timeout-seconds", type=float, default=900.0)
    idle.add_argument("--idle-poll-seconds", type=float, default=10.0)

    pre = subparsers.add_parser("pre-restart")
    add_kube_arguments(pre)
    pre.add_argument("--state-file", type=Path, required=True)
    pre.add_argument("--resume-thread-id", required=True)
    pre.add_argument("--model", default="kimi-k3")
    pre.add_argument("--model-provider", default="kimi")
    pre.add_argument("--cwd", default="/workspace/repo")
    pre.add_argument("--turn-timeout-seconds", type=float, default=300.0)

    for command in ("verify-known", "resolve-known"):
        verify = subparsers.add_parser(command)
        add_kube_arguments(verify)
        verify.add_argument("--resume-thread-id", required=True)

    for command in ("post-restart", "cleanup"):
        subparser = subparsers.add_parser(command)
        add_kube_arguments(subparser)
        subparser.add_argument("--state-file", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "wait-idle":
        wait_idle(args)
    elif args.command == "pre-restart":
        pre_restart(args)
    elif args.command == "post-restart":
        post_restart(args)
    elif args.command == "cleanup":
        cleanup(args)
    elif args.command == "verify-known":
        verify_known(args)
    elif args.command == "resolve-known":
        resolve_known(args)
    else:
        raise SmokeError(f"unsupported command {args.command}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SmokeError as exc:
        print(f"copy-workspace smoke failed: {redact(str(exc))}", file=sys.stderr)
        sys.exit(1)
