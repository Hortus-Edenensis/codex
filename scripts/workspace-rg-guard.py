#!/usr/bin/env python3
"""Linux Workspace 32323 rg entrypoint; install beside the original rg-real."""

import ctypes
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
import sys
import time

STATE_DIR = Path("/run/codex-workspace-32323-rg")
BROAD_ROOTS = (
    "/",
    "/workspace",
    "/workspace/repo",
    "/workspace/repo/SDGO-server",
    "/home",
    "/home/codex",
    "/proc",
    "/sys",
    "/dev",
    "/run",
    "/tmp",
    "/var",
    "/usr",
    "/etc",
    "/opt",
    "/mnt",
    "/media",
    "/root",
)
HISTORY_ROOTS = ("/home/codex/.codex", "/workspace/repo/.git/codex-persist/codex-home")
MAX_FILE = 1024 * 1024
FLAGS = set("nNlqFiSswxvcoHh0Ub")
LONG_FLAGS = {
    "line-number",
    "no-line-number",
    "files-with-matches",
    "files-without-match",
    "quiet",
    "fixed-strings",
    "ignore-case",
    "case-sensitive",
    "smart-case",
    "word-regexp",
    "line-regexp",
    "invert-match",
    "count",
    "count-matches",
    "only-matching",
    "with-filename",
    "no-filename",
    "null",
    "null-data",
    "multiline",
    "multiline-dotall",
    "heading",
    "no-heading",
    "json",
    "files",
    "no-messages",
    "no-config",
    "no-mmap",
    "help",
    "version",
    "stats",
    "trim",
    "no-unicode",
    "unicode",
    "crlf",
    "pcre2",
    "no-pcre2",
}
SHORT_VALUES = {
    "e": "regexp",
    "g": "glob",
    "t": "type",
    "T": "type-not",
    "A": "after-context",
    "B": "before-context",
    "C": "context",
    "m": "max-count",
    "r": "replace",
    "d": "max-depth",
    "j": "threads",
}
LONG_VALUES = set(SHORT_VALUES.values()) | {
    "max-filesize",
    "iglob",
    "color",
    "colors",
    "encoding",
    "max-columns",
    "path-separator",
    "sort",
    "sortr",
    "engine",
    "label",
}


class Blocked(Exception):
    pass


def inspect_args(
    args, cwd, stdin_mode, broad_roots=BROAD_ROOTS, history_roots=HISTORY_ROOTS
):
    patterns, positional = [], []
    forwarded = []
    threads, max_filesize = "2", "1M"
    listing = metadata = False
    index = 0
    after_dash = False
    while index < len(args):
        arg = args[index]
        index += 1
        if arg == "--" and not after_dash:
            after_dash = True
            forwarded.append(arg)
            continue
        if after_dash or not arg.startswith("-") or arg == "-":
            positional.append(arg)
            forwarded.append(arg)
            continue
        options = []
        if arg.startswith("--"):
            name, equal, value = arg[2:].partition("=")
            if name in LONG_FLAGS and not equal:
                listing |= name == "files"
                metadata |= name in ("help", "version")
                if name not in ("no-config", "no-mmap"):
                    forwarded.append(arg)
                continue
            if name not in LONG_VALUES:
                raise Blocked(f"unsupported or unsafe option: --{name}")
            options.append((name, value if equal else None))
        else:
            for offset, char in enumerate(arg[1:]):
                if char in SHORT_VALUES:
                    options.append((SHORT_VALUES[char], arg[offset + 2 :] or None))
                    break
                if char not in FLAGS:
                    raise Blocked(f"unsupported or unsafe option: -{char}")
                forwarded.append(f"-{char}")
        for name, value in options:
            if value is None:
                if index == len(args):
                    raise Blocked(f"missing value for --{name}")
                value = args[index]
                index += 1
            if name == "regexp":
                patterns.append(value)
            if name == "threads":
                if value not in ("1", "2"):
                    raise Blocked("threads must be 1 or 2")
                threads = value
            elif name == "max-filesize":
                size = re.fullmatch(r"([0-9]+)([KMG]?)", value)
                units = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}
                if not size or not 0 < int(size[1]) * units[size[2]] <= MAX_FILE:
                    raise Blocked("max-filesize must be positive and at most 1 MiB")
                max_filesize = value
            else:
                forwarded.append(f"--{name}={value}")
    forwarded = [
        "--no-config",
        "--no-mmap",
        "-j",
        threads,
        "--max-filesize",
        max_filesize,
        *forwarded,
    ]
    if metadata:
        return False, "metadata", forwarded
    if not patterns and not listing:
        if not positional:
            raise Blocked("a pattern or --files is required")
        patterns.append(positional.pop(0))
    piped = stat.S_ISFIFO(stdin_mode) or stat.S_ISREG(stdin_mode)
    targets = positional or (["-"] if piped and not listing else [str(cwd)])
    broad = {Path(path).resolve() for path in broad_roots}
    history = {Path(path).resolve() for path in history_roots}
    canonical = []
    directory = False
    for target in targets:
        if target == "-":
            if not piped:
                raise Blocked(
                    "stdin search requires a pipe or regular-file redirection"
                )
            canonical.append("-")
            continue
        path = Path(target)
        lexical = Path(os.path.abspath(cwd / path if not path.is_absolute() else path))
        path = lexical.resolve(strict=True)
        if path in broad:
            raise Blocked(
                f"broad directory scan denied; select a source subdirectory: {path}"
            )
        if any(
            path == root or root in path.parents or path in root.parents
            for root in history
        ) or any(
            lexical == Path(root) or Path(root) in lexical.parents
            for root in history_roots
        ):
            raise Blocked(f"history directory access is disabled: {path}")
        info = path.stat()
        if stat.S_ISDIR(info.st_mode):
            if os.path.lexists(path / ".git"):
                raise Blocked(
                    f"repository root scan denied; select a source subdirectory: {path}"
                )
            directory = True
        elif not stat.S_ISREG(info.st_mode):
            raise Blocked(
                f"only regular files and source directories are allowed: {path}"
            )
        elif info.st_size > MAX_FILE:
            raise Blocked(f"explicit file exceeds 1 MiB; narrow the input: {path}")
        canonical.append(str(path))
    signature = json.dumps([sorted(patterns), sorted(set(canonical)), listing])
    return directory, hashlib.sha256(signature.encode()).hexdigest(), forwarded


def stop_group(process, grace):
    def send(sig):
        try:
            os.killpg(process.pid, sig)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # macOS reports EPERM for a process group after its last member exited.
            if sys.platform != "darwin":
                raise
            try:
                process.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                raise PermissionError("cannot signal scan process group")
            return False

    send(signal.SIGTERM)
    end = time.monotonic() + grace
    while time.monotonic() < end:
        process.poll()
        if not send(0):
            break
        time.sleep(0.01)
    send(signal.SIGKILL)
    process.wait()


def arm_parent_death(expected_parent):
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = [ctypes.c_int] + [ctypes.c_ulong] * 4
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "cannot install parent-death signal")
    if os.getppid() != expected_parent:
        os.kill(os.getpid(), signal.SIGKILL)


def run(
    args,
    *,
    real_rg=None,
    state_dir=STATE_DIR,
    cwd=None,
    deadline=20.0,
    grace=2.0,
    output_limit=MAX_FILE,
    window=60.0,
    scan_budget=30.0,
    cooldown=15.0,
    broad_roots=BROAD_ROOTS,
    history_roots=HISTORY_ROOTS,
):
    cwd = Path.cwd() if cwd is None else Path(cwd)
    real_rg = (
        Path(__file__).resolve().with_name("rg-real") if real_rg is None else real_rg
    )
    process = None
    handlers = {}
    saved_blocking = {}
    parent = os.getppid()
    interrupted = []
    try:
        if parent == 1:
            raise Blocked("caller disconnected before the scan started")
        directory, key, forwarded = inspect_args(
            args, cwd, os.fstat(0).st_mode, broad_roots, history_roots
        )
        # Installation owns the directory; never fall back to an unlocked scan.
        with open(Path(state_dir) / "scan.lock", "a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise Blocked(
                    "another Workspace rg is active; concurrent scans are disabled"
                )
            state_file = Path(state_dir) / "usage.json"
            now = time.monotonic()
            entries = json.loads(state_file.read_text()) if state_file.exists() else []
            entries = [
                entry
                for entry in entries
                if now - window < entry[1] and entry[0] <= now
            ]
            duration = deadline
            if directory:
                if any(
                    entry[2] == key and now - entry[1] < cooldown for entry in entries
                ):
                    raise Blocked(
                        "repeated directory query is cooling down for 15 seconds"
                    )
                used = sum(
                    max(0, end - max(now - window, start)) for start, end, _ in entries
                )
                duration = min(duration, scan_budget - used)
                if duration <= 0.01:
                    raise Blocked(
                        "directory scan budget exhausted (30 seconds per rolling 60 seconds)"
                    )
                # A killed supervisor leaves its reservation charged instead of resetting the budget.
                entries.append([now, now + duration, key])
                state_file.write_text(json.dumps(entries))
            until = now + duration
            for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
                handlers[sig] = signal.signal(
                    sig, lambda number, frame: interrupted.append(number)
                )
            try:
                if directory:
                    os.write(
                        2,
                        b"workspace-rg-guard: bounded scan, ignores respected, files over 1 MiB excluded\n",
                    )
                env = os.environ.copy()
                env.pop("RIPGREP_CONFIG_PATH", None)
                supervisor = os.getpid()
                process = subprocess.Popen(
                    [str(real_rg), *forwarded],
                    cwd=cwd,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    start_new_session=True,
                    preexec_fn=(lambda: arm_parent_death(supervisor))
                    if sys.platform == "linux"
                    else None,
                )
                total = 0
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ, 1)
                    selector.register(process.stderr, selectors.EVENT_READ, 2)
                    while selector.get_map() or process.poll() is None:
                        if interrupted or os.getppid() != parent:
                            raise Blocked(
                                "caller disconnected or interrupted; scan terminated"
                            )
                        if time.monotonic() >= until:
                            raise Blocked(
                                "scan deadline or rolling time budget exceeded; results are incomplete"
                            )
                        for event, _ in selector.select(
                            min(0.05, max(0, until - time.monotonic()))
                        ):
                            chunk = os.read(event.fd, 16384)
                            if not chunk:
                                selector.unregister(event.fileobj)
                                continue
                            total += len(chunk)
                            if total > output_limit:
                                raise Blocked(
                                    "combined stdout/stderr exceeded 1 MiB; results are incomplete"
                                )
                            destination = event.data
                            if destination not in saved_blocking:
                                saved_blocking[destination] = os.get_blocking(
                                    destination
                                )
                                os.set_blocking(destination, False)
                            while chunk:
                                if interrupted or os.getppid() != parent:
                                    raise Blocked(
                                        "caller disconnected or interrupted; scan terminated"
                                    )
                                if time.monotonic() >= until:
                                    raise Blocked(
                                        "output consumer stalled; scan deadline exceeded"
                                    )
                                try:
                                    chunk = chunk[os.write(destination, chunk) :]
                                except BlockingIOError:
                                    time.sleep(0.01)
                return process.wait() if process.returncode in (0, 1, 2) else 2
            finally:
                if process is not None:
                    stop_group(process, grace)
                    process.stdout.close()
                    process.stderr.close()
                if directory:
                    entries[-1][1] = time.monotonic()
                    state_file.write_text(json.dumps(entries))
    except (
        Blocked,
        OSError,
        ValueError,
        TypeError,
        IndexError,
        KeyError,
        subprocess.SubprocessError,
    ) as error:
        try:
            os.write(2, f"workspace-rg-guard: {error}\n".encode())
        except OSError:
            pass
        return 2
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        for descriptor, blocking in saved_blocking.items():
            os.set_blocking(descriptor, blocking)


if __name__ == "__main__":
    sys.exit(run(sys.argv[1:]))
