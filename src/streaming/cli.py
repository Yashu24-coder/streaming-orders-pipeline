"""Command line entry point.

    python -m streaming.cli produce      # generate the event stream
    python -m streaming.cli run          # process it in micro-batches
    python -m streaming.cli report       # rebuild the charts and run summary
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import REPO_ROOT, load
from .local_runner import run
from .producer import generate_events, to_file, to_kafka


def cmd_produce(cfg, args) -> int:
    events = generate_events(cfg)
    if cfg.has_kafka:
        sent = to_kafka(events, cfg)
        print(f"produced {sent:,} events to {cfg.topic}")
    else:
        path = to_file(events, cfg)
        print(f"wrote {len(events):,} events to {path.relative_to(REPO_ROOT)}")
    return 0


def cmd_run(cfg, args) -> int:
    summary = run(cfg)
    return 0 if summary["events_accepted"] > 0 else 1


def cmd_report(cfg, args) -> int:
    from .report import build

    summary = json.loads((cfg.reports_dir / "run_summary.json").read_text())
    build(cfg, summary)
    return 0


COMMANDS = {"produce": cmd_produce, "run": cmd_run, "report": cmd_report}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="streaming", description=__doc__)
    parser.add_argument("command", choices=sorted(COMMANDS))
    args = parser.parse_args(argv)
    return COMMANDS[args.command](load(), args)


if __name__ == "__main__":
    sys.exit(main())
