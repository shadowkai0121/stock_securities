from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.core.config import resolve_token


class TokenResolutionTests(unittest.TestCase):
    def test_cli_token_wins(self) -> None:
        with patch.dict(os.environ, {"FINMIND_SPONSOR_API_KEY": "env_token"}, clear=True):
            token = resolve_token("cli_token")
        self.assertEqual(token, "cli_token")

    def test_env_wins_over_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text("FINMIND_SPONSOR_API_KEY=file_token\n", encoding="utf-8")
            with patch.dict(os.environ, {"FINMIND_SPONSOR_API_KEY": "env_token"}, clear=True):
                token = resolve_token(None, env_path=env_path)
        self.assertEqual(token, "env_token")

    def test_env_file_used_when_env_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text("FINMIND_TOKEN=file_token\n", encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                token = resolve_token(None, env_path=env_path)
        self.assertEqual(token, "file_token")

    def test_missing_token_raises(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                resolve_token(None, env_path=Path("does_not_exist.env"))


if __name__ == "__main__":
    unittest.main()
