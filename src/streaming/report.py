"""Charts for the README, drawn from the micro-batches that were just written."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from .config import REPO_ROOT, Config  # noqa: E402
from .sink import read_windows  # noqa: E402

IMG = REPO_ROOT / "docs" / "images"
NAVY, TEAL, GREY, RULE, RED = "#1F3552", "#0F6B63", "#5A6672", "#DDE3E9", "#B4483C"


def _style(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(RULE)
    ax.tick_params(colors=GREY, labelsize=8.5)
    ax.yaxis.grid(True, color="#E9EDF1", linewidth=0.7)
    ax.set_axisbelow(True)


def throughput(cfg: Config, summary: dict) -> None:
    batches = summary["batches"]
    ids = [b["batch_id"] for b in batches]
    fig, ax = plt.subplots(figsize=(9, 3.2), dpi=200)
    _style(ax)
    ax.bar(ids, [b["accepted"] for b in batches], color=TEAL, width=0.75, label="accepted")
    ax.bar(ids, [b["dead_lettered"] for b in batches], bottom=[b["accepted"] for b in batches],
           color=RED, width=0.75, label="dead-lettered")
    ax.set_ylim(0, max(b["events"] for b in batches) * 1.25)
    ax.set_title("Events per micro-batch", color=NAVY, fontsize=11.5, loc="left",
                 pad=10, fontweight="bold")
    ax.set_xlabel("micro-batch", color=GREY, fontsize=8.5)
    ax.legend(frameon=False, fontsize=8, loc="upper right")
    fig.tight_layout()
    fig.savefig(IMG / "throughput.png", facecolor="white")
    plt.close(fig)


def revenue_windows(cfg: Config) -> None:
    df = read_windows(cfg.output_dir)
    grouped = df.groupby("window_start", as_index=False)["revenue_inr"].sum()
    fig, ax = plt.subplots(figsize=(9, 3.4), dpi=200)
    _style(ax)
    ax.plot(range(len(grouped)), grouped["revenue_inr"] / 1000, color=NAVY, linewidth=2)
    ax.fill_between(range(len(grouped)), grouped["revenue_inr"] / 1000, color=NAVY, alpha=0.08)
    ax.set_title("Revenue per 5-minute tumbling window (INR thousands)", color=NAVY,
                 fontsize=11.5, loc="left", pad=10, fontweight="bold")
    step = max(len(grouped) // 10, 1)
    ax.set_xticks(range(0, len(grouped), step),
                  [str(grouped["window_start"].iloc[i])[11:16] for i in range(0, len(grouped), step)],
                  fontsize=7.5)
    ax.set_xlabel("window start (UTC)", color=GREY, fontsize=8.5)
    fig.tight_layout()
    fig.savefig(IMG / "revenue_windows.png", facecolor="white")
    plt.close(fig)


def dead_letters(summary: dict) -> None:
    counts = summary["reason_counts"]
    reasons = sorted(counts, key=counts.get)
    fig, ax = plt.subplots(figsize=(9, 3.0), dpi=200)
    _style(ax)
    ax.barh(reasons, [counts[r] for r in reasons], color=RED, height=0.6)
    for y, reason in enumerate(reasons):
        ax.text(counts[reason] + max(counts.values()) * 0.01, y, f"{counts[reason]:,}",
                va="center", fontsize=8.2, color=GREY)
    ax.set_xlim(0, max(counts.values()) * 1.2)
    ax.set_title("Dead-letter records by reason code", color=NAVY, fontsize=11.5,
                 loc="left", pad=10, fontweight="bold")
    ax.xaxis.grid(True, color="#E9EDF1", linewidth=0.7)
    ax.yaxis.grid(False)
    fig.tight_layout()
    fig.savefig(IMG / "dead_letters.png", facecolor="white")
    plt.close(fig)


def build(cfg: Config, summary: dict) -> None:
    IMG.mkdir(parents=True, exist_ok=True)
    throughput(cfg, summary)
    revenue_windows(cfg)
    dead_letters(summary)
    print("charts written to docs/images/")
