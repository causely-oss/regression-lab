#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Generate a comparison report from an eval results JSONL file.

Usage:
  python report.py results/run_<id>.jsonl [--score]

With --score: opens each response interactively so you can enter accuracy scores.
Scores: 0 = wrong, 1 = partial, 2 = correct
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


def load_results(path: Path) -> list[dict]:
    results = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                results.append(json.loads(line))
    return results


def score_interactively(results: list[dict], path: Path):
    """Walk through each result and prompt for an accuracy score."""
    updated = []
    for r in results:
        if r.get("accuracy") is not None:
            updated.append(r)
            continue

        print(f"\n{'='*70}")
        print(f"Prompt {r['prompt_id']} | Condition {r['condition']} | State: {r['state']}")
        print(f"Prompt: {r['prompt_text']}")
        print(f"\nGround truth:\n  {r['ground_truth'].strip()}")
        print(f"\nAgent response:\n{r['response'][:1500]}")
        if len(r["response"]) > 1500:
            print(f"  ... [{len(r['response'])} chars total]")

        while True:
            raw = input("\nAccuracy score (0=wrong, 1=partial, 2=correct): ").strip()
            if raw in ("0", "1", "2"):
                r["accuracy"] = int(raw)
                break
            print("Enter 0, 1, or 2")

        updated.append(r)

    # Write scores back
    with open(path, "w") as f:
        for r in updated:
            f.write(json.dumps(r) + "\n")
    print(f"\nScores saved to {path}")
    return updated


def print_report(results: list[dict]):
    # Group by (prompt_id, condition)
    by_key: dict[tuple, dict] = {}
    for r in results:
        by_key[(r["prompt_id"], r["condition"])] = r

    prompt_ids = sorted({r["prompt_id"] for r in results})
    conditions = sorted({r["condition"] for r in results})

    # ── Per-prompt comparison table ──────────────────────────────────────────
    print("\n" + "="*90)
    print("PER-PROMPT COMPARISON")
    print("="*90)

    col_w = 14
    header_parts = ["P#", "State", "Fault"]
    for c in conditions:
        label = f"Cond {c}"
        header_parts += [f"{label} Acc", f"{label} Lat(s)", f"{label} Tok", f"{label} Tools"]

    print("  ".join(f"{h:<{col_w}}" for h in header_parts))
    print("-" * 90)

    for pid in prompt_ids:
        # grab state/fault from any result for this prompt
        sample = next((r for r in results if r["prompt_id"] == pid), {})
        row = [str(pid), sample.get("state", "")[:8], (sample.get("fault") or "none")[:10]]

        for c in conditions:
            r = by_key.get((pid, c))
            if r:
                acc = str(r.get("accuracy", "-"))
                lat = f"{r['latency_seconds']:.1f}"
                tok = str(r["total_tokens"])
                tools = str(r["tool_call_count"])
            else:
                acc = lat = tok = tools = "-"
            row += [acc, lat, tok, tools]

        print("  ".join(f"{v:<{col_w}}" for v in row))

    # ── Aggregate summary ────────────────────────────────────────────────────
    print("\n" + "="*90)
    print("AGGREGATE SUMMARY (across all prompts)")
    print("="*90)

    for c in conditions:
        cond_results = [r for r in results if r["condition"] == c]
        if not cond_results:
            continue
        scored = [r for r in cond_results if r.get("accuracy") is not None]
        avg_acc = sum(r["accuracy"] for r in scored) / len(scored) if scored else None
        avg_lat = sum(r["latency_seconds"] for r in cond_results) / len(cond_results)
        avg_tok = sum(r["total_tokens"] for r in cond_results) / len(cond_results)
        avg_tools = sum(r["tool_call_count"] for r in cond_results) / len(cond_results)

        label = "Prometheus only" if c == "A" else "Prometheus + Causely"
        print(f"\nCondition {c} — {label} ({len(cond_results)} prompts)")
        print(f"  Accuracy (0-2) : {f'{avg_acc:.2f}' if avg_acc is not None else 'unscored'}")
        print(f"  Avg latency    : {avg_lat:.1f}s")
        print(f"  Avg tokens     : {int(avg_tok)}")
        print(f"  Avg tool calls : {avg_tools:.1f}")

    # ── Tool usage breakdown ─────────────────────────────────────────────────
    print("\n" + "="*90)
    print("TOOL USAGE BREAKDOWN")
    print("="*90)

    for c in conditions:
        tool_counts: dict[str, int] = defaultdict(int)
        for r in results:
            if r["condition"] == c:
                for t in r.get("tool_calls", []):
                    tool_counts[t["name"]] += 1
        if not tool_counts:
            continue
        label = "Prometheus only" if c == "A" else "Prometheus + Causely"
        print(f"\nCondition {c} — {label}")
        for tool, count in sorted(tool_counts.items(), key=lambda x: -x[1]):
            print(f"  {count:>4}x  {tool}")

    # ── Delta (B vs A) ────────────────────────────────────────────────────────
    if "A" in conditions and "B" in conditions:
        print("\n" + "="*90)
        print("DELTA: Condition B vs A (positive = B better/higher)")
        print("="*90)

        deltas_lat, deltas_tok, deltas_tools, deltas_acc = [], [], [], []
        for pid in prompt_ids:
            a = by_key.get((pid, "A"))
            b = by_key.get((pid, "B"))
            if not a or not b:
                continue
            deltas_lat.append(b["latency_seconds"] - a["latency_seconds"])
            deltas_tok.append(b["total_tokens"] - a["total_tokens"])
            deltas_tools.append(b["tool_call_count"] - a["tool_call_count"])
            if a.get("accuracy") is not None and b.get("accuracy") is not None:
                deltas_acc.append(b["accuracy"] - a["accuracy"])

        def avg(lst):
            return sum(lst) / len(lst) if lst else 0

        print(f"  Accuracy delta : {f'{avg(deltas_acc):+.2f}' if deltas_acc else 'unscored'}")
        print(f"  Latency delta  : {avg(deltas_lat):+.1f}s")
        print(f"  Token delta    : {int(avg(deltas_tok)):+d}")
        print(f"  Tool call delta: {avg(deltas_tools):+.1f}")


def main():
    parser = argparse.ArgumentParser(description="Report on eval results")
    parser.add_argument("results_file", help="Path to JSONL results file")
    parser.add_argument("--score", action="store_true", help="Interactively score unscored responses")
    args = parser.parse_args()

    path = Path(args.results_file)
    if not path.exists():
        print(f"File not found: {path}")
        sys.exit(1)

    results = load_results(path)
    print(f"Loaded {len(results)} results from {path}")

    if args.score:
        results = score_interactively(results, path)

    print_report(results)


if __name__ == "__main__":
    main()
