#!/usr/bin/env python3
"""
graphify-stop-hook.py — Claude Code Stop hook

Fires after every Claude turn. Reads the session transcript, sums tokens
and duration across all turns, writes a cumulative session_cost snapshot
to graphify-out/metrics/{session_id}.jsonl.

Safety contract:
  - NEVER exits with code 2 (would block Claude from completing its turn)
  - Always exits 0, even on error
  - Silent on failure — never surface errors to the developer
"""

import glob
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _find_transcript(session_id: str) -> Optional[Path]:
    pattern = str(Path.home() / ".claude" / "projects" / "*" / f"{session_id}.jsonl")
    matches = glob.glob(pattern)
    return Path(matches[0]) if matches else None


def _git_info(cwd: str):
    def _run_git(*args):
        try:
            return subprocess.check_output(
                ["git", *args], cwd=cwd, stderr=subprocess.DEVNULL
            ).decode().strip()
        except Exception:
            return ""

    branch = _run_git("rev-parse", "--abbrev-ref", "HEAD")
    sha    = _run_git("rev-parse", "--short", "HEAD")
    name   = _run_git("config", "user.name")
    email  = _run_git("config", "user.email")
    return branch, sha, name, email


def _run(data: dict) -> None:
    if os.environ.get("GRAPHIFY_NO_TELEMETRY") == "1":
        return

    session_id = data.get("session_id", "") or os.environ.get("CLAUDE_SESSION_ID", "")
    cwd = data.get("cwd", os.getcwd())

    if not session_id:
        return

    transcript_path = _find_transcript(session_id)
    if not transcript_path:
        return

    # Accumulate token counts and duration across all turns
    input_tokens = 0
    cache_read_tokens = 0
    cache_write_tokens = 0
    output_tokens = 0
    duration_ms = 0
    model_name = ""  # detected from transcript; used for pricing tier
    turn_count = 0

    with open(transcript_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                turn = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Capture model name from any assistant turn that has it
            if not model_name:
                m = turn.get("model") or turn.get("message", {}).get("model", "")
                if m:
                    model_name = m

            # Count user turns. Prompt text is intentionally not captured.
            role = turn.get("message", {}).get("role", "")
            if role == "user":
                turn_count += 1

            # Token usage from assistant turns
            usage = turn.get("usage") or turn.get("message", {}).get("usage", {})
            if usage:
                input_tokens += usage.get("input_tokens", 0) or 0
                cache_read_tokens += usage.get("cache_read_input_tokens", 0) or 0
                cache_write_tokens += usage.get("cache_creation_input_tokens", 0) or 0
                output_tokens += usage.get("output_tokens", 0) or 0

            # Duration from turn_duration system events (type=system, subtype=turn_duration)
            if turn.get("subtype") == "turn_duration":
                duration_ms += turn.get("durationMs", 0) or 0

    # Pricing per million tokens — detect model from transcript, fall back to Sonnet rates.
    # Haiku 4.5: input $0.80, cache_write $1.00, cache_read $0.08, output $4.00
    # Sonnet:    input $3.00, cache_write $3.75, cache_read $0.30, output $15.00
    # Opus:      input $15.0, cache_write $18.75, cache_read $1.50, output $75.00
    PRICING = {
        "haiku":  (0.80,  1.00,  0.08,  4.00),
        "sonnet": (3.00,  3.75,  0.30, 15.00),
        "opus":   (15.00, 18.75, 1.50, 75.00),
    }
    model_tier = "sonnet"  # default
    if model_name:
        m = model_name.lower()
        if "haiku" in m:
            model_tier = "haiku"
        elif "opus" in m:
            model_tier = "opus"
    p_in, p_cw, p_cr, p_out = PRICING[model_tier]
    cost_usd = (
        input_tokens * p_in +
        cache_write_tokens * p_cw +
        cache_read_tokens * p_cr +
        output_tokens * p_out
    ) / 1_000_000

    git_branch, git_sha, developer_name, _ = _git_info(cwd)
    repo = Path(cwd).name

    # If a previous entry exists for this session, preserve the original branch
    # and sha so we can compute LOC from session start (not just last commit).
    metrics_file = Path(cwd) / "graphify-out" / "metrics" / f"{session_id}.jsonl"
    original_branch = git_branch
    session_start_sha = git_sha
    if metrics_file.exists():
        try:
            first_line = metrics_file.read_text(encoding="utf-8").splitlines()[0]
            first = json.loads(first_line)
            original_branch = first.get("git_branch", git_branch)
            session_start_sha = first.get("git_sha") or git_sha
        except Exception:
            pass

    # LOC changed since session started — diff from the sha at session start
    # to the current working tree (captures both committed and uncommitted changes).
    # Using git diff HEAD only shows uncommitted changes, which is always 0 after
    # Claude commits during a turn.
    loc_added, loc_removed = 0, 0
    try:
        diff_out = subprocess.check_output(
            ["git", "diff", "--shortstat", session_start_sha],
            cwd=cwd, stderr=subprocess.DEVNULL
        ).decode()
        m_add = re.search(r"(\d+) insertion", diff_out)
        m_del = re.search(r"(\d+) deletion", diff_out)
        if m_add:
            loc_added = int(m_add.group(1))
        if m_del:
            loc_removed = int(m_del.group(1))
    except Exception:
        pass

    event = {
        "event": "session_cost",
        "ts": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        # NOTE: developer_email and first_message (a 200-char excerpt of the
        # user's prompt) are deliberately not recorded. This repo has no CI
        # dashboard consuming these metrics, so that data would be collected
        # with nothing reading it.
        "developer": developer_name,
        "git_branch": original_branch,
        "git_sha": git_sha,
        "repo": repo,
        "turn_count": turn_count,
        "model": model_name,
        "input_tokens": input_tokens,
        "cache_read_tokens": cache_read_tokens,
        "cache_write_tokens": cache_write_tokens,
        "output_tokens": output_tokens,
        "cost_usd": round(cost_usd, 6),
        "duration_ms": duration_ms,
        "model_tier": model_tier,
        "loc_added": loc_added,
        "loc_removed": loc_removed,
    }

    metrics_dir = Path(cwd) / "graphify-out" / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    with open(metrics_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")

    # Metrics stay machine-local. This repo has no CI workflow and no
    # graphify-data branch, so there is nothing to ship them to — the JSONL is
    # gitignored and exists only for local inspection.


def main() -> None:
    try:
        raw = sys.stdin.read()
        data = json.loads(raw) if raw.strip() else {}
    except (json.JSONDecodeError, OSError):
        data = {}

    try:
        _run(data)
    except Exception as e:
        print(f"[graphify-stop-hook] WARNING: {e}", file=sys.stderr)
    finally:
        sys.exit(0)


if __name__ == "__main__":
    main()
