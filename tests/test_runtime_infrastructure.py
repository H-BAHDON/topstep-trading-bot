from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from main import AccountInstanceLock, load_environment
from topstep_api import ProjectXError


class RuntimeInfrastructureTests(unittest.TestCase):
    def test_selected_environment_overrides_base_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            selected = Path(directory) / ".env.selected"
            selected.write_text(
                "ACTIVE_STRATEGY=ema12_hourly_trailing_43\nCONTRACTS=10\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {}, clear=True):
                loaded = load_environment(str(selected))
                self.assertEqual(loaded, selected.resolve())
                self.assertEqual(
                    os.environ["ACTIVE_STRATEGY"], "ema12_hourly_trailing_43"
                )
                self.assertEqual(os.environ["CONTRACTS"], "10")

    def test_account_instance_lock_prevents_two_bots(self) -> None:
        lock = AccountInstanceLock(987654321)
        try:
            with self.assertRaises(ProjectXError):
                AccountInstanceLock(987654321)
        finally:
            lock.close()
        replacement = AccountInstanceLock(987654321)
        replacement.close()
