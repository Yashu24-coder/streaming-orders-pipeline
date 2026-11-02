from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from streaming.config import load  # noqa: E402


@pytest.fixture()
def cfg(tmp_path):
    base = load()
    return base.__class__(
        **{**base.__dict__,
           "raw_dir": tmp_path / "raw",
           "output_dir": tmp_path / "lake" / "windows",
           "dead_letter_dir": tmp_path / "lake" / "dlq",
           "checkpoint_dir": tmp_path / "checkpoints",
           "reports_dir": tmp_path / "reports",
           "micro_batch_size": 100,
           "event_count": 400}
    )
