import importlib.util
import io
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("copy_workspace_smoke.py")
SPEC = importlib.util.spec_from_file_location("copy_workspace_smoke", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)


class FakeClient:
    def __init__(self, responses: list[dict]) -> None:
        self.responses = list(responses)
        self.requests: list[tuple[str, dict]] = []

    def request(self, method: str, params: dict) -> dict:
        self.requests.append((method, params))
        if not self.responses:
            raise AssertionError(f"unexpected request {method}")
        return self.responses.pop(0)


class CopyWorkspaceSmokeTests(unittest.TestCase):
    def test_state_round_trip_returns_the_saved_object(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "smoke-state.json"
            expected = {"stateVersion": 1, "createdThreadIds": ["one"]}
            smoke.atomic_write_json(path, expected)
            self.assertEqual(smoke.read_state(path), expected)

    def test_recv_json_reassembles_fragmented_text_with_ping(self) -> None:
        client = smoke.ProxyWebSocketClient(["unused"], 1.0)
        payload = json.dumps({"method": "thread/list", "params": {"count": 343}}).encode()
        midpoint = len(payload) // 2
        frames = [
            (False, 0x1, payload[:midpoint]),
            (True, 0x9, b"ping"),
            (True, 0x0, payload[midpoint:]),
        ]
        with mock.patch.object(client, "_recv_frame", side_effect=frames), mock.patch.object(
            client, "_send_frame"
        ) as send:
            message = client.recv_json(time.monotonic() + 1)
        self.assertEqual(message["params"]["count"], 343)
        send.assert_called_once_with(0xA, b"ping")

    def test_raw_frame_exposes_fin_for_continuation(self) -> None:
        client = smoke.ProxyWebSocketClient(["unused"], 1.0)
        client.buffer = bytearray([0x01, 0x03]) + bytearray(b"abc")
        final, opcode, payload = client._recv_frame(time.monotonic() + 1)
        self.assertFalse(final)
        self.assertEqual(opcode, 0x1)
        self.assertEqual(payload, b"abc")

    def test_thread_status_gate_accepts_only_idle_and_not_loaded(self) -> None:
        self.assertTrue(smoke.statuses_are_idle({"notLoaded": 343}))
        self.assertTrue(smoke.statuses_are_idle({"notLoaded": 342, "idle": 1}))
        self.assertFalse(smoke.statuses_are_idle({"notLoaded": 342, "active": 1}))
        self.assertFalse(smoke.statuses_are_idle({"systemError": 1}))

    def test_paginated_thread_statuses_reads_archived_and_nonarchived_pages(self) -> None:
        pages = [
            {
                "data": [{"id": "one", "status": {"type": "notLoaded"}}],
                "nextCursor": "next",
            },
            {
                "data": [{"id": "two", "status": {"type": "idle"}}],
                "nextCursor": None,
            },
            {
                "data": [{"id": "three", "status": {"type": "notLoaded"}}],
                "nextCursor": None,
            },
        ]
        client = FakeClient(pages)
        count, statuses = smoke.paginated_thread_statuses(client)
        self.assertEqual(count, 3)
        self.assertEqual(statuses, {"notLoaded": 2, "idle": 1})
        self.assertTrue(
            all(params["useStateDbOnly"] is False for _, params in client.requests)
        )

    def test_fast_status_scan_must_be_explicitly_state_db_only(self) -> None:
        client = FakeClient(
            [
                {"data": [], "nextCursor": None},
                {"data": [], "nextCursor": None},
            ]
        )
        smoke.paginated_thread_statuses(client, use_state_db_only=True)
        self.assertTrue(
            all(params["useStateDbOnly"] is True for _, params in client.requests)
        )

    def test_idle_gate_acceptance_uses_scan_and_repair(self) -> None:
        context = mock.MagicMock()
        context.__enter__.return_value = mock.MagicMock()
        args = SimpleNamespace(
            idle_timeout_seconds=1,
            idle_poll_seconds=0,
            namespace="namespace",
            pod="pod",
            container="workspace",
            request_timeout_seconds=1,
        )
        with mock.patch.object(
            smoke, "ProxyWebSocketClient", return_value=context
        ), mock.patch.object(
            smoke,
            "paginated_thread_statuses",
            side_effect=[(343, {"notLoaded": 343}), (343, {"notLoaded": 343})],
        ) as statuses, mock.patch.object(
            smoke, "process_and_job_gate", return_value=(0, 0, 0)
        ), mock.patch("sys.stdout", new=io.StringIO()):
            smoke.wait_idle(args)
        self.assertTrue(statuses.call_args_list[0].kwargs["use_state_db_only"])
        self.assertFalse(statuses.call_args_list[1].kwargs["use_state_db_only"])

    def test_process_gate_supports_direct_server_and_postgres_pod(self) -> None:
        args = SimpleNamespace(
            namespace="namespace",
            pod="workspace-pod",
            container="workspace",
            postgres_pod="postgres-pod",
            postgres_container="postgres",
        )
        process = mock.Mock(stdout="DAEMON_DESCENDANTS=0\n")
        jobs = mock.Mock(
            stdout="RUNNING_AGENT_JOBS=0\nRUNNING_AGENT_JOB_ITEMS=0\n"
        )
        with mock.patch.object(
            smoke, "kubectl_shell", return_value=process
        ) as workspace_shell, mock.patch.object(
            smoke, "postgres_shell", return_value=jobs
        ) as database_shell:
            self.assertEqual(smoke.process_and_job_gate(args), (0, 0, 0))
        self.assertIn("app-server --listen unix", workspace_shell.call_args.args[1])
        self.assertIn("POSTGRES_PASSWORD", database_shell.call_args.args[1])

    def test_persisted_memory_mode_reads_from_postgres_pod(self) -> None:
        args = SimpleNamespace(
            namespace="namespace",
            pod="workspace-pod",
            container="workspace",
            postgres_pod="postgres-pod",
            postgres_container="postgres",
        )
        result = mock.Mock(stdout="MEMORY_MODE=enabled\n")
        with mock.patch.object(
            smoke, "postgres_shell", return_value=result
        ) as database_shell, mock.patch.object(
            smoke, "kubectl_shell", side_effect=AssertionError("workspace shell unused")
        ):
            self.assertEqual(smoke.persisted_memory_mode(args, "abc123"), "enabled")
        database_shell.assert_called_once()
        script = database_shell.call_args.args[1]
        self.assertIn("PGPASSWORD", script)
        self.assertIn("-h 127.0.0.1", script)
        self.assertIn("${POSTGRES_DB}", script)
        self.assertNotIn("CODEX_REMOTE_SQL_URL", script)

    def test_turns_page_fails_closed_on_empty_history(self) -> None:
        with self.assertRaisesRegex(smoke.SmokeError, "no persisted turns"):
            smoke.turns_page(FakeClient([{"data": [], "nextCursor": None}]), "known")

    def test_exact_agent_message_requires_one_exact_sentinel(self) -> None:
        sentinel = "KIMI_SMOKE_OK:abc"
        exact = {"items": [{"type": "agentMessage", "text": sentinel}]}
        extra = {
            "items": [
                {"type": "agentMessage", "text": sentinel},
                {"type": "agentMessage", "text": "extra"},
            ]
        }
        self.assertTrue(smoke.turn_contains_exact_message(exact, sentinel))
        self.assertFalse(smoke.turn_contains_exact_message(extra, sentinel))

    def test_identical_repeated_fork_must_return_same_id(self) -> None:
        first = {"id": "fork-1", "forkedFromId": "source"}
        second = {"id": "fork-1", "forkedFromId": "source"}
        self.assertEqual(
            smoke.require_deduplicated_fork(first, second, "source"), "fork-1"
        )
        with self.assertRaisesRegex(smoke.SmokeError, "not deduplicated"):
            smoke.require_deduplicated_fork(
                first, {"id": "fork-2", "forkedFromId": "source"}, "source"
            )

    def test_model_list_must_advertise_requested_kimi_model(self) -> None:
        smoke.require_model_available(
            FakeClient(
                [
                    {
                        "data": [{"id": "other", "model": "other"}],
                        "nextCursor": "page-2",
                    },
                    {
                        "data": [{"id": "kimi-k3", "model": "kimi-k3"}],
                        "nextCursor": None,
                    },
                ]
            ),
            "kimi-k3",
        )
        with self.assertRaisesRegex(smoke.SmokeError, "does not advertise"):
            smoke.require_model_available(
                FakeClient([{"data": [], "nextCursor": None}]), "kimi-k3"
            )

    def test_thread_start_must_confirm_kimi_provider(self) -> None:
        response = {"model": "kimi-k3", "modelProvider": "kimi"}
        smoke.require_started_model_provider(response, "kimi-k3", "kimi")
        with self.assertRaisesRegex(smoke.SmokeError, "model provider"):
            smoke.require_started_model_provider(
                {"model": "kimi-k3", "modelProvider": "other"},
                "kimi-k3",
                "kimi",
            )

    def test_resolve_known_scans_filesystem_and_validates_resume(self) -> None:
        context = mock.MagicMock()
        client = mock.MagicMock()
        client.request.return_value = {"thread": {"id": "fixed", "path": None}}
        context.__enter__.return_value = client
        args = SimpleNamespace(
            namespace="namespace",
            pod="pod",
            container="workspace",
            resume_thread_id="fixed",
            request_timeout_seconds=1,
        )
        with mock.patch.object(
            smoke, "ProxyWebSocketClient", return_value=context
        ), mock.patch.object(
            smoke, "paginated_threads", return_value=[]
        ) as scan, mock.patch.object(
            smoke, "turns_page", return_value=[{"id": "turn"}]
        ), mock.patch("sys.stdout", new=io.StringIO()):
            smoke.resolve_known(args)
        scan.assert_called_once_with(
            context.__enter__.return_value, use_state_db_only=False
        )
        client.request.assert_called_once_with(
            "thread/resume", {"threadId": "fixed", "excludeTurns": True}
        )

    def test_resolve_known_skips_history_that_cannot_resume(self) -> None:
        context = mock.MagicMock()
        client = mock.MagicMock()
        client.request.side_effect = [
            smoke.SmokeError("legacy resume failed"),
            {"thread": {"id": "fallback", "path": None}},
        ]
        context.__enter__.return_value = client
        args = SimpleNamespace(
            namespace="namespace",
            pod="pod",
            container="workspace",
            resume_thread_id="fixed",
            request_timeout_seconds=1,
        )
        output = io.StringIO()
        with mock.patch.object(
            smoke, "ProxyWebSocketClient", return_value=context
        ), mock.patch.object(
            smoke, "paginated_threads", return_value=[{"id": "fallback"}]
        ), mock.patch.object(
            smoke, "turns_page", return_value=[{"id": "turn"}]
        ), mock.patch("sys.stdout", new=output):
            smoke.resolve_known(args)
        summary = json.loads(output.getvalue())
        self.assertEqual(summary["threadId"], "fallback")
        self.assertTrue(summary["fallbackUsed"])

    def test_all_thread_ids_rejects_duplicates(self) -> None:
        client = FakeClient(
            [
                {
                    "data": [
                        {"id": "same", "status": {"type": "notLoaded"}},
                        {"id": "same", "status": {"type": "notLoaded"}},
                    ],
                    "nextCursor": None,
                },
                {"data": [], "nextCursor": None},
            ]
        )
        with self.assertRaisesRegex(smoke.SmokeError, "duplicate thread id"):
            smoke.all_thread_ids(client)

    def test_parse_args_adds_postgres_defaults_to_pre_and_post_restart(self) -> None:
        with mock.patch(
            "sys.argv",
            [
                "copy_workspace_smoke.py",
                "pre-restart",
                "--namespace",
                "namespace",
                "--pod",
                "workspace-pod",
                "--container",
                "workspace",
                "--state-file",
                "state.json",
                "--resume-thread-id",
                "thread",
            ],
        ):
            pre_args = smoke.parse_args()
        self.assertEqual(pre_args.postgres_pod, "codex-postgres-0")
        self.assertEqual(pre_args.postgres_container, "postgres")

        with mock.patch(
            "sys.argv",
            [
                "copy_workspace_smoke.py",
                "post-restart",
                "--namespace",
                "namespace",
                "--pod",
                "workspace-pod",
                "--container",
                "workspace",
                "--state-file",
                "state.json",
            ],
        ):
            post_args = smoke.parse_args()
        self.assertEqual(post_args.postgres_pod, "codex-postgres-0")
        self.assertEqual(post_args.postgres_container, "postgres")


if __name__ == "__main__":
    unittest.main()
