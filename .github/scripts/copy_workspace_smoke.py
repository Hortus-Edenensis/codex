#!/usr/bin/env python3

import argparse
import base64
import json
import os
import secrets
import select
import struct
import subprocess
import sys
import time
from dataclasses import dataclass


HANDSHAKE_TIMEOUT_SECONDS = 20.0
REQUEST_TIMEOUT_SECONDS = 30.0
READ_CHUNK_SIZE = 65536


class SmokeError(RuntimeError):
    pass


@dataclass
class SmokeSummary:
    interactive_thread_count: int
    known_thread_id: str
    smoke_thread_id: str
    smoke_goal_objective: str
    codex_home: str | None


class ProxyWebSocketClient:
    def __init__(self, command: list[str]) -> None:
        self.command = command
        self.process: subprocess.Popen[bytes] | None = None
        self.buffer = bytearray()
        self.next_request_id = 1

    def __enter__(self) -> "ProxyWebSocketClient":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
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

    def initialize(self) -> dict:
        response = self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "copy_workspace_smoke",
                    "title": "Copy Workspace Smoke",
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

    def request(self, method: str, params: dict | None = None) -> dict:
        request_id = self.next_request_id
        self.next_request_id += 1
        payload: dict[str, object] = {"method": method, "id": request_id}
        if params is not None:
            payload["params"] = params
        self.send_json(payload)

        deadline = time.monotonic() + REQUEST_TIMEOUT_SECONDS
        while True:
            message = self.recv_json(deadline)
            if "id" in message and message["id"] == request_id:
                if "error" in message:
                    raise SmokeError(
                        f"{method} failed: {json.dumps(message['error'], ensure_ascii=True)}"
                    )
                result = message.get("result")
                if not isinstance(result, dict):
                    raise SmokeError(f"{method} returned a non-object result: {result!r}")
                return result
            if "method" in message and "id" in message:
                self._reject_server_request(message)

    def notify(self, method: str, params: dict | None = None) -> None:
        payload: dict[str, object] = {"method": method}
        if params is not None:
            payload["params"] = params
        self.send_json(payload)

    def send_json(self, payload: dict[str, object]) -> None:
        self._send_frame(0x1, json.dumps(payload, separators=(",", ":")).encode("utf-8"))

    def recv_json(self, deadline: float) -> dict:
        while True:
            opcode, payload = self._recv_frame(deadline)
            if opcode == 0x8:
                raise SmokeError("proxy websocket closed unexpectedly")
            if opcode == 0x9:
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:
                continue
            if opcode != 0x1:
                raise SmokeError(f"unexpected websocket opcode from proxy: {opcode}")
            try:
                message = json.loads(payload.decode("utf-8"))
            except Exception as exc:
                raise SmokeError(f"failed to decode websocket payload: {exc}") from exc
            if not isinstance(message, dict):
                raise SmokeError(f"expected JSON object message, got: {message!r}")
            return message

    def _perform_handshake(self) -> None:
        if self.process is None or self.process.stdin is None or self.process.stdout is None:
            raise SmokeError("proxy process did not expose stdio pipes")

        websocket_key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
        request = (
            "GET /rpc HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {websocket_key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        ).encode("ascii")
        self.process.stdin.write(request)
        self.process.stdin.flush()

        deadline = time.monotonic() + HANDSHAKE_TIMEOUT_SECONDS
        while b"\r\n\r\n" not in self.buffer:
            self._fill_buffer(deadline, min_bytes=1)
        header_bytes, _, remainder = self.buffer.partition(b"\r\n\r\n")
        self.buffer = bytearray(remainder)

        header_text = header_bytes.decode("iso-8859-1")
        first_line = header_text.split("\r\n", 1)[0]
        if "101" not in first_line:
            stderr_text = self._read_stderr()
            raise SmokeError(
                f"websocket handshake failed: {first_line}\nproxy stderr:\n{stderr_text}"
            )

    def _recv_frame(self, deadline: float) -> tuple[int, bytes]:
        self._fill_buffer(deadline, min_bytes=2)
        first, second = self.buffer[0], self.buffer[1]
        del self.buffer[:2]

        fin = (first & 0x80) != 0
        opcode = first & 0x0F
        masked = (second & 0x80) != 0
        payload_len = second & 0x7F

        if payload_len == 126:
            self._fill_buffer(deadline, min_bytes=2)
            payload_len = struct.unpack("!H", self.buffer[:2])[0]
            del self.buffer[:2]
        elif payload_len == 127:
            self._fill_buffer(deadline, min_bytes=8)
            payload_len = struct.unpack("!Q", self.buffer[:8])[0]
            del self.buffer[:8]

        mask_key = b""
        if masked:
            self._fill_buffer(deadline, min_bytes=4)
            mask_key = bytes(self.buffer[:4])
            del self.buffer[:4]

        self._fill_buffer(deadline, min_bytes=payload_len)
        payload = bytes(self.buffer[:payload_len])
        del self.buffer[:payload_len]

        if masked:
            payload = bytes(byte ^ mask_key[idx % 4] for idx, byte in enumerate(payload))

        if not fin:
            raise SmokeError("fragmented websocket frames are not supported in smoke client")

        return opcode, payload

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self.process is None or self.process.stdin is None:
            raise SmokeError("proxy process is not running")
        header = bytearray()
        header.append(0x80 | (opcode & 0x0F))
        payload_len = len(payload)
        if payload_len < 126:
            header.append(0x80 | payload_len)
        elif payload_len < (1 << 16):
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", payload_len))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", payload_len))

        mask = secrets.token_bytes(4)
        header.extend(mask)
        masked_payload = bytes(byte ^ mask[idx % 4] for idx, byte in enumerate(payload))

        self.process.stdin.write(bytes(header) + masked_payload)
        self.process.stdin.flush()

    def _fill_buffer(self, deadline: float, min_bytes: int) -> None:
        if self.process is None or self.process.stdout is None:
            raise SmokeError("proxy process is not running")

        while len(self.buffer) < min_bytes:
            self._raise_if_process_exited()
            timeout = deadline - time.monotonic()
            if timeout <= 0:
                raise SmokeError("timed out waiting for proxy websocket data")
            ready, _, _ = select.select([self.process.stdout], [], [], timeout)
            if not ready:
                raise SmokeError("timed out waiting for proxy websocket data")
            chunk = os.read(self.process.stdout.fileno(), READ_CHUNK_SIZE)
            if not chunk:
                self._raise_if_process_exited()
                raise SmokeError("proxy websocket closed while reading data")
            self.buffer.extend(chunk)

    def _raise_if_process_exited(self) -> None:
        if self.process is None:
            raise SmokeError("proxy process is not running")
        return_code = self.process.poll()
        if return_code is None:
            return
        raise SmokeError(
            f"proxy process exited with code {return_code}\nproxy stderr:\n{self._read_stderr()}"
        )

    def _read_stderr(self) -> str:
        if self.process is None or self.process.stderr is None:
            return ""
        try:
            stderr_fd = self.process.stderr.fileno()
            chunks: list[bytes] = []
            while True:
                ready, _, _ = select.select([stderr_fd], [], [], 0)
                if not ready:
                    break
                chunk = os.read(stderr_fd, READ_CHUNK_SIZE)
                if not chunk:
                    break
                chunks.append(chunk)
            return b"".join(chunks).decode("utf-8", errors="replace")
        except Exception:
            return ""

    def _reject_server_request(self, message: dict) -> None:
        request_id = message.get("id")
        if request_id is None:
            return
        self.send_json(
            {
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": "copy-workspace smoke client does not support server requests",
                },
            }
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke-test copy-workspace app-server resume and goal flows via app-server proxy."
    )
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--pod", required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--resume-thread-id", required=True)
    parser.add_argument("--min-interactive-threads", type=int, default=8)
    return parser.parse_args()


def proxy_command(args: argparse.Namespace) -> list[str]:
    proxy_shell = (
        "env HOME=/home/codex USER=codex LOGNAME=codex "
        'su -m codex -c "/home/codex/.codex/packages/standalone/current/bin/codex app-server proxy"'
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
        proxy_shell,
    ]


def run_smoke(args: argparse.Namespace) -> SmokeSummary:
    command = proxy_command(args)
    smoke_thread_id: str | None = None
    smoke_goal_objective = f"copy-workspace smoke goal persistence {int(time.time())}"
    codex_home: str | None = None

    try:
        with ProxyWebSocketClient(command) as client:
            initialize = client.initialize()
            codex_home = initialize.get("codexHome")

            listed = client.request("thread/list", {"limit": 100, "sourceKinds": []})
            interactive_threads = listed.get("data")
            if not isinstance(interactive_threads, list):
                raise SmokeError("thread/list returned a non-list data payload")
            interactive_thread_count = len(interactive_threads)
            if interactive_thread_count < args.min_interactive_threads:
                raise SmokeError(
                    "interactive thread count stayed below the expected threshold: "
                    f"{interactive_thread_count} < {args.min_interactive_threads}"
                )

            read_result = client.request(
                "thread/read", {"threadId": args.resume_thread_id, "includeTurns": False}
            )
            read_thread = read_result.get("thread")
            if not isinstance(read_thread, dict) or read_thread.get("id") != args.resume_thread_id:
                raise SmokeError("thread/read did not return the requested known thread")

            resumed = client.request(
                "thread/resume",
                {"threadId": args.resume_thread_id, "excludeTurns": True},
            )
            resumed_thread = resumed.get("thread")
            if (
                not isinstance(resumed_thread, dict)
                or resumed_thread.get("id") != args.resume_thread_id
            ):
                raise SmokeError("thread/resume did not return the requested known thread")

            known_goal_snapshot = client.request(
                "thread/goal/get", {"threadId": args.resume_thread_id}
            )
            if "goal" not in known_goal_snapshot:
                raise SmokeError("thread/goal/get response for known thread omitted `goal`")

            started = client.request("thread/start", {"personality": "none"})
            started_thread = started.get("thread")
            if not isinstance(started_thread, dict):
                raise SmokeError("thread/start did not return a thread object")
            smoke_thread_id = started_thread.get("id")
            if not isinstance(smoke_thread_id, str) or not smoke_thread_id:
                raise SmokeError("thread/start returned an invalid smoke thread id")

            client.request(
                "thread/name/set",
                {
                    "threadId": smoke_thread_id,
                    "name": f"copy-workspace smoke {int(time.time())}",
                },
            )
            set_goal = client.request(
                "thread/goal/set",
                {
                    "threadId": smoke_thread_id,
                    "objective": smoke_goal_objective,
                    "tokenBudget": 1234,
                },
            )
            goal = set_goal.get("goal")
            if not isinstance(goal, dict):
                raise SmokeError("thread/goal/set did not return a goal object")
            if goal.get("objective") != smoke_goal_objective:
                raise SmokeError("thread/goal/set returned the wrong objective")
            if goal.get("tokenBudget") != 1234:
                raise SmokeError("thread/goal/set returned the wrong token budget")

            get_goal = client.request("thread/goal/get", {"threadId": smoke_thread_id})
            if get_goal.get("goal") != goal:
                raise SmokeError("thread/goal/get did not round-trip the newly stored goal")

        if smoke_thread_id is None:
            raise SmokeError("smoke thread id was never created")

        with ProxyWebSocketClient(command) as client:
            client.initialize()
            persisted_goal = client.request("thread/goal/get", {"threadId": smoke_thread_id})
            goal = persisted_goal.get("goal")
            if not isinstance(goal, dict):
                raise SmokeError("persisted thread/goal/get did not return a goal object")
            if goal.get("objective") != smoke_goal_objective:
                raise SmokeError("persisted thread goal objective did not survive reconnect")
            if goal.get("tokenBudget") != 1234:
                raise SmokeError("persisted thread goal budget did not survive reconnect")
            client.request("thread/delete", {"threadId": smoke_thread_id})

        return SmokeSummary(
            interactive_thread_count=interactive_thread_count,
            known_thread_id=args.resume_thread_id,
            smoke_thread_id=smoke_thread_id,
            smoke_goal_objective=smoke_goal_objective,
            codex_home=codex_home,
        )
    except Exception:
        if smoke_thread_id is not None:
            try:
                with ProxyWebSocketClient(command) as cleanup_client:
                    cleanup_client.initialize()
                    cleanup_client.request("thread/delete", {"threadId": smoke_thread_id})
            except Exception as cleanup_exc:
                print(
                    f"cleanup warning: failed to delete smoke thread {smoke_thread_id}: {cleanup_exc}",
                    file=sys.stderr,
                )
        raise


def main() -> int:
    args = parse_args()
    summary = run_smoke(args)
    print(
        json.dumps(
            {
                "status": "ok",
                "interactiveThreadCount": summary.interactive_thread_count,
                "knownThreadId": summary.known_thread_id,
                "smokeThreadId": summary.smoke_thread_id,
                "smokeGoalObjective": summary.smoke_goal_objective,
                "codexHome": summary.codex_home,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
