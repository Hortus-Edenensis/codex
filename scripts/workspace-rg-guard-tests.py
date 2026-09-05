#!/usr/bin/env python3
"""Small, bounded integration checks; no production paths or scans."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time

MODULE = Path(__file__).with_name("workspace-rg-guard.py")
spec = importlib.util.spec_from_file_location("guard", MODULE)
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
DRIVER = """
import importlib.util, json, sys
spec = importlib.util.spec_from_file_location('guard', sys.argv[1])
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
sys.exit(guard.run(json.loads(sys.argv[2]), **json.loads(sys.argv[3])))
"""


def assert_stopped(pid):
    # Linux may briefly retain an orphan zombie; it cannot perform further I/O.
    for _ in range(50):
        try:
            os.kill(pid, 0)
            status = Path(f"/proc/{pid}/stat")
            if status.exists() and status.read_text().split()[2] == "Z":
                return
        except ProcessLookupError:
            return
        time.sleep(0.02)
    raise AssertionError(f"scan process survived: {pid}")


def main():
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        source = root / "source"
        source.mkdir()
        small = source / "small.txt"
        small.write_text("hello\nworld\n")
        history = root / "history"
        history.mkdir()
        (history / "sessions").mkdir()
        (source / "back-to-root").symlink_to(root)
        fake = root / "rg-real"
        fake.write_text(
            f"#!{sys.executable}\n"
            + """
import os, signal, subprocess, sys, time
assert sys.argv[1:7] == ['--no-config', '--no-mmap', '-j', '2', '--max-filesize', '1M']
args = sys.argv[7:]
assert not any(arg in ('--no-config', '--no-mmap', '-j', '--threads', '--max-filesize') or arg.startswith(('--threads=', '--max-filesize=')) for arg in args)
if 'CHILD' in args:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    child = subprocess.Popen([sys.executable, '-c', 'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)'])
    open('child.pid', 'w').write(str(child.pid))
    time.sleep(30)
elif 'SLEEP' in args:
    time.sleep(30)
elif 'PARENT_DEATH' in args:
    open('rg.pid', 'w').write(str(os.getpid()))
    time.sleep(30)
elif 'NOISY' in args:
    os.write(1, b'x' * 4096)
elif 'MIXED' in args:
    os.write(1, b'x' * 60)
    os.write(2, b'y' * 60)
elif 'FLOOD' in args:
    for _ in range(256):
        os.write(1, b'x' * 4096)
elif 'NONE' in args:
    sys.exit(1)
else:
    print('hello')
"""
        )
        fake.chmod(0o755)
        count = 0

        def command(args, state=None, **overrides):
            nonlocal count
            count += 1
            state = root / f"state-{count}" if state is None else state
            state.mkdir(exist_ok=True)
            kwargs = dict(
                real_rg=str(fake),
                state_dir=str(state),
                cwd=str(source),
                broad_roots=[str(root)],
                history_roots=[str(history)],
                deadline=2,
                grace=0.08,
            )
            kwargs.update(overrides)
            return [
                sys.executable,
                "-c",
                DRIVER,
                str(MODULE),
                json.dumps(args),
                json.dumps(kwargs),
            ]

        def check(args, expected, message="", **kwargs):
            result = subprocess.run(
                command(args, **kwargs),
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=5,
            )
            assert result.returncode == expected, (args, result)
            assert message.encode() in result.stderr, result
            return result

        actual_rg = shutil.which("rg")
        assert actual_rg, "rg required for normal match/no-match integration checks"
        assert (
            b"hello" in check(["-n", "hello", str(small)], 0, real_rg=actual_rg).stdout
        )
        check(["-F", "absent", str(small)], 1, real_rg=actual_rg)
        check(["-nF", "-e", "hello", "--glob=*.txt", str(small)], 0, real_rg=actual_rg)
        check(["--", "hello", str(small)], 0, real_rg=actual_rg)
        check(
            ["--no-mmap", "-j2", "--max-filesize=1M", "hello", str(small)],
            0,
            real_rg=actual_rg,
        )
        check(
            ["--threads", "1", "--max-filesize", "1024K", "hello", str(small)],
            0,
            real_rg=actual_rg,
        )
        check(["--threads=99", "MATCH", str(source)], 2, "threads must")
        check(["--max-filesize=2M", "MATCH", str(source)], 2, "at most 1 MiB")
        check(
            [
                "--no-config",
                "--no-mmap",
                "-j2",
                "--max-filesize=1M",
                "MATCH",
                str(small),
            ],
            0,
        )
        normalized = guard.inspect_args(
            [
                "--no-config",
                "--no-mmap",
                "-nj2",
                "--threads",
                "1",
                "--max-filesize=1M",
                "--max-filesize",
                "512K",
                "-e",
                "MATCH",
                str(small),
            ],
            source,
            stat.S_IFCHR,
            [root],
            [history],
        )[2]
        assert normalized == [
            "--no-config",
            "--no-mmap",
            "-j",
            "1",
            "--max-filesize",
            "512K",
            "-n",
            "--regexp=MATCH",
            str(small),
        ]
        check(["--", "--threads", str(small)], 1, real_rg=actual_rg)
        check(["-n", "MATCH", str(root)], 2, "broad directory")
        check(["MATCH"], 2, "broad directory", cwd=str(root))
        check(["MATCH", str(source / "back-to-root")], 2, "broad directory")
        check(["MATCH", str(history / "sessions")], 2, "history directory")
        for marker_kind in ("directory", "file"):
            repo = root / f"repo-{marker_kind}"
            repo_source = repo / "src"
            repo_source.mkdir(parents=True)
            if marker_kind == "directory":
                (repo / ".git").mkdir()
            else:
                (repo / ".git").write_text("gitdir: /unused/worktree\n")
            repo_file = repo / "README.md"
            repo_file.write_text("hello\n")
            check(["MATCH", str(repo)], 2, "repository root")
            check(["MATCH", "."], 2, "repository root", cwd=str(repo))
            check(["MATCH"], 2, "repository root", cwd=str(repo))
            check(["--files", str(repo)], 2, "repository root")
            check(["MATCH", str(repo_source)], 0)
            check(["hello", str(repo_file)], 0, real_rg=actual_rg)
        for system_root in ("/tmp", "/var", "/usr", "/etc", "/opt"):
            if Path(system_root).is_dir():
                try:
                    guard.inspect_args(["MATCH", system_root], source, stat.S_IFCHR)
                except guard.Blocked as error:
                    assert "broad directory" in str(error)
                else:
                    raise AssertionError(f"system root allowed: {system_root}")
        for unsafe in (
            "-uuu",
            "--hidden",
            "--no-ignore",
            "-L",
            "--follow",
            "-a",
            "-z",
            "--mmap",
            "--pre=cat",
        ):
            check([unsafe, "MATCH", str(source)], 2, "unsupported or unsafe")
        large = source / "large"
        with large.open("wb") as stream:
            stream.truncate(guard.MAX_FILE + 1)
        check(["MATCH", str(large)], 2, "exceeds 1 MiB")
        directory, key, forwarded = guard.inspect_args(
            ["-nF", "-e", "not/a/path", "--glob=*.py", str(source)],
            source,
            stat.S_IFCHR,
            [root],
            [history],
        )
        assert directory
        assert guard.inspect_args(
            ["-g", "*.py", "--", "-pattern", str(source)],
            source,
            stat.S_IFCHR,
            [root],
            [history],
        )[0]
        assert guard.inspect_args(
            ["--files", str(source)], source, stat.S_IFCHR, [root], [history]
        )[0]
        assert not guard.inspect_args(["MATCH"], root, stat.S_IFIFO, [root], [history])[
            0
        ]
        check(["MATCH", str(source)], 0, "files over 1 MiB excluded")
        check(["NOISY", str(small)], 2, "combined stdout/stderr", output_limit=100)
        check(["MIXED", str(small)], 2, "combined stdout/stderr", output_limit=100)
        check(["SLEEP", str(small)], 2, "deadline", deadline=0.12)
        stalled = subprocess.Popen(
            command(["FLOOD", str(small)], deadline=0.12),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(0.35)
        _, error = stalled.communicate(timeout=3)
        assert stalled.returncode == 2 and b"deadline" in error, (
            stalled.returncode,
            error,
        )
        orphan_command = command(["SLEEP", str(small)])
        launcher = "import json,subprocess,sys,time; subprocess.Popen(json.loads(sys.argv[1])); time.sleep(0.1)"
        disconnected = subprocess.run(
            [sys.executable, "-c", launcher, json.dumps(orphan_command)],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=3,
        )
        assert b"disconnected" in disconnected.stderr
        check(["CHILD", str(source)], 2, "deadline", deadline=0.2)
        child = int((source / "child.pid").read_text())
        assert_stopped(child)
        if sys.platform == "linux":
            supervisor = subprocess.Popen(
                command(["PARENT_DEATH", str(small)]),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                pid_file = source / "rg.pid"
                for _ in range(100):
                    if pid_file.exists() and pid_file.read_text():
                        break
                    time.sleep(0.01)
                rg_pid = int(pid_file.read_text())
                supervisor.kill()
                supervisor.communicate(timeout=3)
                assert supervisor.returncode == -signal.SIGKILL
                assert_stopped(rg_pid)
            finally:
                if supervisor.poll() is None:
                    supervisor.terminate()
                    supervisor.communicate(timeout=3)
        shared = root / "shared"
        process = subprocess.Popen(
            command(["SLEEP", str(source)], state=shared),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for _ in range(100):
            if (shared / "usage.json").exists():
                break
            time.sleep(0.01)
        check(["MATCH", str(small)], 2, "concurrent scans", state=shared)
        process.send_signal(signal.SIGTERM)
        _, error = process.communicate(timeout=3)
        assert process.returncode == 2 and b"interrupted" in error
        repeat = root / "repeat"
        check(["MATCH", str(source)], 0, state=repeat)
        check(["-n", "MATCH", str(source)], 2, "cooling down", state=repeat)
        check(["MATCH", str(small)], 0, state=repeat)
        check(["MATCH", str(small)], 0, state=repeat)
        budget = root / "budget"
        budget.mkdir()
        now = time.monotonic()
        (budget / "usage.json").write_text(json.dumps([[now - 31, now, "previous"]]))
        check(["MATCH", str(source)], 2, "budget exhausted", state=budget)
        (budget / "usage.json").write_text(json.dumps([[now - 1, now, "previous"]]))
        check(
            ["SLEEP", str(source)],
            2,
            "rolling time budget",
            state=budget,
            scan_budget=1.1,
        )
        print(
            f"PASS: {count} subprocess checks plus parser and descendant-cleanup checks"
        )


if __name__ == "__main__":
    main()
