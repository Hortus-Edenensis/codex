#!/usr/bin/env python3
"""Preserve a Codex config while selecting a native Kimi provider."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import tempfile
import tomllib


TABLE_RE = re.compile(r"^\s*\[([^]]+)]\s*(?:#.*)?$")
KEY_RE = re.compile(r"^\s*([A-Za-z0-9_-]+)\s*=")


def quoted(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def configure(text: str, *, model: str, base_url: str, env_key: str) -> str:
    lines = text.splitlines(keepends=True)
    output: list[str] = []
    section: str | None = None
    skipping_kimi = False
    model_seen = False
    provider_seen = False
    inserted_top_level = False

    def insert_missing_top_level() -> None:
        nonlocal inserted_top_level
        if inserted_top_level:
            return
        if not model_seen:
            output.append(f"model = {quoted(model)}\n")
        if not provider_seen:
            output.append('model_provider = "kimi"\n')
        inserted_top_level = True

    for line in lines:
        table_match = TABLE_RE.match(line)
        if table_match:
            if section is None:
                insert_missing_top_level()
            section = table_match.group(1).strip()
            skipping_kimi = section == "model_providers.kimi"
            if skipping_kimi:
                continue
        elif skipping_kimi:
            continue

        if section is None:
            key_match = KEY_RE.match(line)
            key = key_match.group(1) if key_match else None
            if key == "model":
                output.append(f"model = {quoted(model)}\n")
                model_seen = True
                continue
            if key == "model_provider":
                output.append('model_provider = "kimi"\n')
                provider_seen = True
                continue
        output.append(line)

    insert_missing_top_level()
    while output and not output[-1].strip():
        output.pop()
    output.extend(
        [
            "\n\n[model_providers.kimi]\n",
            'name = "Kimi"\n',
            f"base_url = {quoted(base_url)}\n",
            f"env_key = {quoted(env_key)}\n",
            'wire_api = "chat"\n',
            "request_max_retries = 4\n",
            "stream_max_retries = 5\n",
            "stream_idle_timeout_ms = 300000\n",
        ]
    )
    configured = "".join(output)
    tomllib.loads(configured)
    return configured


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    parser.add_argument("--model", required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--env-key", default="KIMI_API_KEY")
    args = parser.parse_args()

    original = args.config.read_text() if args.config.exists() else ""
    configured = configure(
        original,
        model=args.model,
        base_url=args.base_url,
        env_key=args.env_key,
    )
    args.config.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{args.config.name}.", dir=args.config.parent)
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(configured)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, args.config)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


if __name__ == "__main__":
    main()
