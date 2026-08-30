from pathlib import Path
import re
import unittest


WORKFLOW = (
    Path(__file__).resolve().parents[1] / "workflows" / "remote-sql-release.yml"
)


class RemoteSqlReleaseWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.text = WORKFLOW.read_text(encoding="utf-8")

    def test_release_tag_and_version_base_match_copy8(self) -> None:
        self.assertIn("v0.151.0-remote-sql-copy.8", self.text)
        self.assertIn("VERSION_BASE: 0.151.0-remote-sql-copy.8", self.text)

    def test_workflow_never_uses_self_hosted_runner(self) -> None:
        self.assertNotIn("self-hosted", self.text)
        self.assertRegex(self.text, r"runs-on:\s+ubuntu-22\.04")

    def test_workflow_keeps_only_build_and_draft_jobs(self) -> None:
        self.assertIn("migration-integrity:", self.text)
        self.assertIn("build-release:", self.text)
        self.assertIn("draft-release:", self.text)
        self.assertNotIn("deploy-copy-workspace:", self.text)
        self.assertNotIn("finalize-release:", self.text)

    def test_helper_checks_cover_smoke_and_workflow_tests(self) -> None:
        self.assertIn("test_copy_workspace_smoke.py", self.text)
        self.assertIn("test_workflow_remote_sql_release.py", self.text)
        self.assertIn(".github/scripts/deploy_remote_sql_release.py", self.text)
        self.assertIn(".github/scripts/copy_workspace_smoke.py", self.text)

    def test_release_notes_are_provider_neutral(self) -> None:
        banned = "".join(chr(code) for code in (75, 105, 109, 105))
        self.assertNotIn(banned, self.text)
        self.assertRegex(
            self.text,
            re.compile(r"native Codex conversations", re.IGNORECASE),
        )


if __name__ == "__main__":
    unittest.main()
