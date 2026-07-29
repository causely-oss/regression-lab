#!/usr/bin/env python3
"""
Eval harness: compare Claude + Grafana MCP vs Claude + Grafana MCP + Causely MCP.

Conditions:
  A  —  local Grafana MCP only  (mcp__grafana__*)
  B  —  local Grafana MCP + Causely MCP  (mcp__grafana__* + mcp__claude_ai_Causely__*)

Prerequisites:
  - kubectl port-forward -n scenario-01 svc/prometheus 9090:9090 &
  - kubectl port-forward -n scenario-01 svc/grafana 3000:3000 &
  - Causely MCP authenticated via /mcp in Claude Code  (condition B only)

Usage:
  python run_eval.py [--conditions A,B] [--prompts 1,2,3] [--dry-run]
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

SCENARIO_ROOT = Path(__file__).parent.parent
EVAL_DIR = Path(__file__).parent
RESULTS_DIR = EVAL_DIR / "results"
MCP_CONFIG = EVAL_DIR / "grafana-mcp.json"

MODEL = "claude-sonnet-4-6"
FAULT_SETTLE_SECONDS = 90
RESTORE_SETTLE_SECONDS = 90

# Tools to count as "real" MCP calls (exclude internal harness tools)
MCP_TOOL_PREFIX = "mcp__"

SYSTEM_APPEND = """\
You are diagnosing issues in a 36-service Go microservices platform running in Kubernetes \
in the 'scenario-01' namespace. Services cover checkout, payments, search, catalog, orders, \
auth, cart, and streaming flows. Data stores: PostgreSQL (payments-db), Redis, Kafka.
When using Causely, always scope queries to the 'scenario-01' namespace.
Use tools systematically: identify what is unhealthy first, then trace to root cause.
End with: root cause, evidence, and remediation recommendation.\
"""


def build_cmd(prompt: str, condition: str) -> list[str]:
    cmd = [
        "claude", "--print",
        "--dangerously-skip-permissions",
        "--no-session-persistence",
        "--model", MODEL,
        "--mcp-config", str(MCP_CONFIG),
        "--output-format", "stream-json",
        "--verbose",
        "--append-system-prompt", SYSTEM_APPEND,
    ]
    # Condition A: strict MCP config means ONLY the local Grafana MCP is available.
    # Condition B: allow all configured MCPs (local Grafana + Causely from Claude Code).
    if condition == "A":
        cmd.append("--strict-mcp-config")
    cmd += ["--", prompt]
    return cmd


def parse_stream_json(stdout: str) -> dict:
    tool_calls = []
    final_text = ""
    input_tokens = 0
    output_tokens = 0
    duration_ms = 0

    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue

        etype = event.get("type", "")

        if etype == "assistant":
            for block in event.get("message", {}).get("content", []):
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "tool_use" and block.get("name", "").startswith(MCP_TOOL_PREFIX):
                    tool_calls.append({
                        "name": block["name"],
                        "input_keys": list(block.get("input", {}).keys()),
                    })

        elif etype == "result":
            final_text = event.get("result", "")
            usage = event.get("usage", {})
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)
            duration_ms = event.get("duration_ms", 0)

    return {
        "response": final_text,
        "latency_seconds": round(duration_ms / 1000, 2),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "tool_calls": tool_calls,
        "tool_call_count": len(tool_calls),
        "unique_tools_used": sorted({t["name"] for t in tool_calls}),
    }


def run_prompt(prompt_text: str, condition: str) -> dict:
    cmd = build_cmd(prompt_text, condition)
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600,
                          cwd=str(SCENARIO_ROOT))
    wall_seconds = round(time.time() - t0, 2)

    result = parse_stream_json(proc.stdout)

    if proc.returncode != 0 and not result["response"]:
        result["response"] = f"[ERROR exit={proc.returncode}] {proc.stderr[:500]}"

    result["wall_clock_seconds"] = wall_seconds
    return result


def run_script(script: str, label: str, timeout: int = 60):
    path = SCENARIO_ROOT / script
    if not path.exists():
        print(f"  Warning: {script} not found, skipping")
        return
    print(f"  {label}: {script}")
    proc = subprocess.run(["bash", str(path)], capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        print(f"  Warning (exit {proc.returncode}): {proc.stderr.strip()[:200]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--conditions", default="A,B",
                        help="Comma-separated conditions to run (default: A,B)")
    parser.add_argument("--prompts", default=None,
                        help="Comma-separated prompt IDs to run (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print prompts without calling the API")
    args = parser.parse_args()

    conditions = [c.strip().upper() for c in args.conditions.split(",")]
    prompt_filter = {int(x) for x in args.prompts.split(",")} if args.prompts else None

    with open(EVAL_DIR / "prompts.yaml") as f:
        all_prompts = yaml.safe_load(f)["prompts"]

    prompts = [p for p in all_prompts if prompt_filter is None or p["id"] in prompt_filter]

    if args.dry_run:
        for p in prompts:
            print(f"[{p['id']}] {p['state']} | fault={p.get('fault')} | {p['text']}")
        return

    RESULTS_DIR.mkdir(exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    out_path = RESULTS_DIR / f"run_{run_id}.jsonl"

    print(f"Run ID     : {run_id}")
    print(f"Model      : {MODEL}")
    print(f"Conditions : {conditions}  (A=Grafana MCP, B=Grafana+Causely MCP)")
    print(f"Prompts    : {[p['id'] for p in prompts]}")
    print(f"Output     : {out_path}")

    # Prompt-first ordering: for each prompt, run all conditions before advancing.
    # This keeps the environment state identical for A vs B comparisons.
    active_fault: str | None = None

    for p in prompts:
        target_fault = p.get("fault")

        # Inject if this prompt starts a new fault scenario
        if p.get("inject_before") and target_fault != active_fault:
            print(f"\n{'='*65}")
            print(f"INJECT: {p['inject_before']}")
            print("="*65)
            run_script(p["inject_before"], "Inject", timeout=120)
            print(f"  Waiting {FAULT_SETTLE_SECONDS}s for metrics to settle...")
            time.sleep(FAULT_SETTLE_SECONDS)
            active_fault = target_fault

        print(f"\n{'='*65}")
        print(f"PROMPT {p['id']} [{p['state']}]: {p['text'][:75]}...")
        print("="*65)

        for condition in conditions:
            label = "Grafana MCP only" if condition == "A" else "Grafana+Causely MCP"
            print(f"\n  [{condition}] {label}")
            result = run_prompt(p["text"], condition)

            result.update({
                "run_id": run_id,
                "condition": condition,
                "prompt_id": p["id"],
                "prompt_text": p["text"],
                "state": p["state"],
                "fault": p.get("fault"),
                "ground_truth": p["ground_truth"],
                "accuracy": None,
            })

            print(
                f"  -> {result['latency_seconds']}s api | "
                f"{result['total_tokens']} tokens | "
                f"{result['tool_call_count']} mcp calls"
            )

            with open(out_path, "a") as f:
                f.write(json.dumps(result) + "\n")

        # Restore at end of each fault group using restore_all for a clean slate
        if p.get("restore_after"):
            print(f"\n  Restore: inject/restore_all.sh")
            run_script("inject/restore_all.sh", "Restore all", timeout=300)
            active_fault = None
            print(f"  Waiting {RESTORE_SETTLE_SECONDS}s for environment to return to healthy...")
            time.sleep(RESTORE_SETTLE_SECONDS)

    if active_fault:
        print(f"\n  Final restore: inject/restore_all.sh")
        run_script("inject/restore_all.sh", "Final restore all", timeout=300)

    print(f"\nDone. Score with: python eval/report.py {out_path} --score")


if __name__ == "__main__":
    main()
