#!/usr/bin/env python3

import importlib.util
from pathlib import Path
import tomllib
import unittest


SCRIPT = Path(__file__).with_name("configure_kimi_provider.py")
SPEC = importlib.util.spec_from_file_location("configure_kimi_provider", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ConfigureKimiProviderTest(unittest.TestCase):
    def test_preserves_unrelated_config_and_replaces_existing_kimi_block(self) -> None:
        original = '''\
# keep this comment
model = "gpt-5.6-sol"
model_provider = "openai"
experimental_thread_store = { type = "postgres", database_url_env = "DB_URL" }

[features]
remote_models = true

[model_providers.kimi]
name = "Old Kimi"
base_url = "https://old.invalid/v1"
wire_api = "responses"

[model_providers.openai]
name = "OpenAI"
'''
        configured = MODULE.configure(
            original,
            model="kimi-k3",
            base_url="https://api.moonshot.cn/v1",
            env_key="KIMI_API_KEY",
        )
        parsed = tomllib.loads(configured)

        self.assertIn("# keep this comment", configured)
        self.assertEqual(parsed["model"], "kimi-k3")
        self.assertEqual(parsed["model_provider"], "kimi")
        self.assertEqual(parsed["experimental_thread_store"]["type"], "postgres")
        self.assertTrue(parsed["features"]["remote_models"])
        self.assertEqual(parsed["model_providers"]["openai"]["name"], "OpenAI")
        self.assertEqual(
            parsed["model_providers"]["kimi"],
            {
                "name": "Kimi",
                "base_url": "https://api.moonshot.cn/v1",
                "env_key": "KIMI_API_KEY",
                "wire_api": "chat",
                "request_max_retries": 4,
                "stream_max_retries": 5,
                "stream_idle_timeout_ms": 300000,
            },
        )

    def test_adds_missing_top_level_keys_before_first_table(self) -> None:
        configured = MODULE.configure(
            "[features]\nremote_models = true\n",
            model="kimi-k3",
            base_url="https://api.moonshot.cn/v1",
            env_key="KIMI_API_KEY",
        )
        parsed = tomllib.loads(configured)

        self.assertEqual(parsed["model"], "kimi-k3")
        self.assertEqual(parsed["model_provider"], "kimi")
        self.assertTrue(parsed["features"]["remote_models"])


if __name__ == "__main__":
    unittest.main()
