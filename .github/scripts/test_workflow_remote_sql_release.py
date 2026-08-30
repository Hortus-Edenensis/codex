#!/usr/bin/env python3

from pathlib import Path
import unittest


WORKFLOW = (
    Path(__file__).resolve().parents[1] / "workflows" / "remote-sql-release.yml"
)


class RemoteSqlReleaseWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text()

    def test_release_jobs_only_use_github_hosted_ubuntu(self) -> None:
        runner_lines = [
            line.strip()
            for line in self.workflow.splitlines()
            if line.lstrip().startswith("runs-on:")
        ]

        self.assertEqual(runner_lines, ["runs-on: ubuntu-22.04"] * 3)
        self.assertNotIn("self-hosted", self.workflow)

    def test_release_tag_is_locked_to_copy_seven(self) -> None:
        tag = "v0.151.0-remote-sql-copy.7"

        self.assertGreaterEqual(self.workflow.count(tag), 3)

    def test_sqlite_migrations_trigger_release_validation(self) -> None:
        self.assertIn("- codex-rs/state/migrations/**", self.workflow)

    def test_draft_lookup_uses_authenticated_release_collection(self) -> None:
        self.assertIn(
            'gh api "repos/${GITHUB_REPOSITORY}/releases?per_page=100"',
            self.workflow,
        )
        self.assertNotIn("gh release view", self.workflow)


if __name__ == "__main__":
    unittest.main()
