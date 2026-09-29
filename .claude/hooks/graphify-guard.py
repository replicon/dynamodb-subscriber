#!/usr/bin/env python3
"""
graphify-guard.py — Claude Code PreToolUse + PostToolUse hook

WHAT IT DOES
------------
Two enforcement rules:

  Rule A (PreToolUse): Block source file Reads until a graphify query has run.
  Rule B (PostToolUse): If graphify query returned TRUNCATED at any budget,
                        mark the session — then block the NEXT source Read until
                        a non-truncated query at a higher budget clears the flag.

This enforces the mandatory graphify-first rule
(CLAUDE.md — "MANDATORY: Query the knowledge graph before reading source files"):
  1. "ALWAYS query the knowledge graph before reading source files."
  2. "If output contains [!] TRUNCATED → re-run at the next budget tier."

HOW IT WORKS
------------
Three roles in one script (selected by argv and tool_name):

  PreToolUse / Bash   → if the command reads a source file:
                          - block if no graphify query run yet this session
                          - block if truncation flag is set (must re-run at higher budget)
                        Otherwise always allow.

  PreToolUse / Read   → if the file is index.js or under test/:
                          - block if no graphify query run yet this session
                          - block if truncation flag is set (must re-run at higher budget)

  PostToolUse / Bash  → invoked with --post flag; checks whether the bash
                        command was a graphify query that returned TRUNCATED
                        and sets the truncation flag accordingly.

STATE FILES (auto-cleaned on reboot)
--------------------------------------
  {tempdir}/graphify_guard_{session_id}      → graph has been queried
  {tempdir}/graphify_truncated_{session_id}  → last query was TRUNCATED (re-run at higher budget)
  (see tempfile.gettempdir() — OS-dependent)

EXIT CODES (Claude Code convention)
-------------------------------------
  0  → allow / no action
  2  → block; message printed to stderr is shown to Claude and the user
"""

import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# UTF-8 stderr so box-drawing / emoji chars (━ ⛔ ▶) don't raise
# UnicodeEncodeError on Windows cp1252 terminals.
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── Constants ─────────────────────────────────────────────────────────────────

SOURCE_DIRS = ("test/",)

# The entire library is a single index.js at the repo root — there is no src/
# or lib/ dir, so a directory prefix alone would guard nothing.
SOURCE_ROOT_FILES = (
    "index.js",
)

# Excluded from the graph by .graphifyignore, or build output. Reads here must
# never be blocked — the graph holds nothing to answer with.
NON_INDEXED_DIRS = (
    "node_modules/",
    "coverage/",
    "aidlc-docs/",
    "_daidlc/",
    ".claude/",
    ".github/",
    "graphify-out/",
)


def _get_state_dir() -> str:
    """Return platform-appropriate temp directory."""
    return tempfile.gettempdir()


def _state_file(session_id: str) -> str:
    base = _get_state_dir()
    return os.path.join(base, f"graphify_guard_{session_id}")


def _truncated_file(session_id: str) -> str:
    base = _get_state_dir()
    return os.path.join(base, f"graphify_truncated_{session_id}")


def _is_graph_queried(session_id: str) -> bool:
    return os.path.exists(_state_file(session_id))


def _is_truncated_pending(session_id: str) -> bool:
    return os.path.exists(_truncated_file(session_id))


def _mark_graph_queried(session_id: str) -> None:
    try:
        open(_state_file(session_id), "w").close()
    except OSError:
        pass


def _mark_truncated(session_id: str, budget: int = 0) -> None:
    try:
        with open(_truncated_file(session_id), "w") as f:
            f.write(str(budget))
    except OSError:
        pass


def _get_truncated_budget(session_id: str) -> int:
    try:
        return int(open(_truncated_file(session_id)).read().strip())
    except Exception:
        return 0


def _clear_truncated(session_id: str) -> None:
    try:
        os.remove(_truncated_file(session_id))
    except OSError:
        pass


def _append_event(data: dict, extra: dict) -> None:
    """Append a JSONL telemetry event for this session."""
    if os.environ.get("GRAPHIFY_NO_TELEMETRY") == "1":
        return
    session_id = data.get("session_id", "unknown")
    cwd = data.get("cwd", os.getcwd())
    metrics_dir = Path(cwd) / "graphify-out" / "metrics"
    metrics_file = metrics_dir / f"{session_id}.jsonl"
    try:
        git_branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=cwd,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        git_branch = ""
    try:
        git_sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=cwd,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        git_sha = ""
    repo = Path(cwd).name

    def _relativize(path_str: str) -> str:
        """Strip cwd prefix so paths in events are repo-relative (no PII)."""
        cwd_prefix = str(Path(cwd)).replace("\\", "/").rstrip("/") + "/"
        normalized = path_str.replace("\\", "/")
        if normalized.startswith(cwd_prefix):
            return normalized[len(cwd_prefix):]
        return normalized

    # Relativize any file_path in extra before writing
    if "file_path" in extra:
        extra = {**extra, "file_path": _relativize(extra["file_path"])}

    base = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "git_branch": git_branch,
        "git_sha": git_sha,
        "repo": repo,
    }
    event_line = {**base, **extra}
    try:
        metrics_dir.mkdir(parents=True, exist_ok=True)
        with open(metrics_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(event_line) + "\n")
    except Exception as e:
        print(f"[graphify-guard] WARNING: could not write telemetry: {e}", file=sys.stderr)


def _repo_relative(file_path: str, cwd: Optional[str] = None) -> str:
    """Best-effort repo-relative path. Returns "" if the path is outside cwd.

    Paths outside the repo cannot be in the graph, so the guard must not
    claim them.
    """
    norm = file_path.replace("\\", "/").rstrip("/")
    root = (cwd or os.getcwd()).replace("\\", "/").rstrip("/")
    if not norm:
        return ""
    # Absolute path: only in scope when it sits under the repo root.
    if re.match(r"^(/|[A-Za-z]:/)", norm):
        if norm.lower() == root.lower():
            return ""
        if norm.lower().startswith(root.lower() + "/"):
            return norm[len(root) + 1:]
        return ""
    # Relative path that climbs out of the repo is out of scope.
    if norm.startswith("../"):
        return ""
    return norm[2:] if norm.startswith("./") else norm


def _is_source_file(file_path: str, cwd: Optional[str] = None) -> bool:
    """True only for in-repo files the knowledge graph actually indexes."""
    rel = _repo_relative(file_path, cwd)
    if not rel:
        return False
    # Never guard paths the graph does not index — blocking these would tell
    # the developer to consult a graph that cannot answer.
    if any(rel == d.rstrip("/") or rel.startswith(d) for d in NON_INDEXED_DIRS):
        return False
    if any(rel.startswith(d) for d in SOURCE_DIRS):
        return True
    return rel in SOURCE_ROOT_FILES


BUDGET_LADDER = [6000, 16000, 32000, 64000]


def _budget_from_command(command: str) -> int:
    m = re.search(r"--budget\s+(\d+)", command)
    # No --budget flag: treat as the lowest documented tier so escalation lands
    # on 16000 next, matching the ladder in CLAUDE.md.
    return int(m.group(1)) if m else BUDGET_LADDER[0]


def _next_budget(current: int) -> Optional[int]:
    """Return the next budget tier above current, or None if already at max."""
    for b in BUDGET_LADDER:
        if b > current:
            return b
    return None


# grep/rg/awk are search tools, not whole-file reads — the graph is not a
# substitute for them, so they are deliberately absent here. sed is excluded
# because its common in-repo use (sed -i) is a write.
_BASH_READ_COMMANDS = ("cat ", "head ", "tail ", "less ", "bat ")


def _is_bash_source_read(command: str, cwd: Optional[str] = None) -> bool:
    """Return True if the command reads a graph-indexed source file whole."""
    if not any(re.search(r"(^|[|;&]|\&\&)\s*" + re.escape(t), command)
               for t in _BASH_READ_COMMANDS):
        return False
    # Check each whitespace-separated token as a candidate path rather than
    # substring-matching the raw command, so "node_modules/foo/index.js" is not
    # caught by the bare "index.js" entry in SOURCE_ROOT_FILES.
    for tok in re.split(r"\s+", command):
        tok = tok.strip("'\"")
        if tok and not tok.startswith("-") and _is_source_file(tok, cwd):
            return True
    return False


# ── PostToolUse role ──────────────────────────────────────────────────────────

def post_main(data: dict) -> None:
    """Check graphify output after Bash runs; set truncation flag if needed."""
    session_id = data.get("session_id", "unknown")
    tool_input = data.get("tool_input", {})
    command = tool_input.get("command", "")

    if "graphify query" not in command:
        sys.exit(0)

    budget = _budget_from_command(command)

    # Extract output — handle all payload shapes Claude Code may send:
    # {"output": "..."}, {"content": "..."}, {"content": [...]}, plain string, list
    tool_response = data.get("tool_response", "")
    if isinstance(tool_response, dict):
        raw = (
            tool_response.get("output")
            or tool_response.get("content")
            or tool_response.get("stdout")
            or tool_response.get("text")
            or tool_response.get("result")
            or ""
        )
        output = raw if isinstance(raw, str) else json.dumps(raw)
    elif isinstance(tool_response, list):
        output = json.dumps(tool_response)
    else:
        output = str(tool_response)

    # Large outputs: Claude Code saves Bash output to a temp file and passes a
    # file reference instead of inline content, leaving `output` empty. Read the
    # file so large graphify queries still mark the session as queried.
    if not output.strip() and isinstance(tool_response, dict):
        file_ref = (
            tool_response.get("filePath")
            or tool_response.get("file_path")
            or tool_response.get("path")
        )
        if file_ref and os.path.exists(file_ref):
            try:
                with open(file_ref, "r", encoding="utf-8", errors="replace") as fh:
                    output = fh.read()
            except OSError:
                pass

    # Only mark queried if graphify actually produced output — empty output means
    # the command failed (e.g. binary not on PATH) and no real query ran.
    if not output.strip():
        sys.exit(0)

    _mark_graph_queried(session_id)

    if "[!] TRUNCATED" in output:
        next_b = _next_budget(budget)
        # At the top of the ladder there is no higher tier to escalate to, so
        # holding the flag would block source reads for the rest of the session
        # with no way to clear it. Warn, but let the developer proceed.
        if next_b is None:
            _clear_truncated(session_id)
        else:
            _mark_truncated(session_id, budget)
        _append_event(data, {"event": "query_run", "budget": budget, "truncated": True})
        if next_b is not None:
            _append_event(data, {"event": "budget_escalated", "from_budget": budget, "to_budget": next_b})
            print(
                "\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "⛔  GRAPHIFY BUDGET LADDER\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "\n"
                f"  graphify query returned TRUNCATED at --budget {budget}.\n"
                f"  Source file reads are BLOCKED until you re-run at --budget {next_b}.\n"
                "\n"
                f"  ▶  graphify query \"<same terms>\" --budget {next_b}\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                file=sys.stderr,
            )
        else:
            # Already at max budget tier — must narrow
            print(
                "\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "⚠️  GRAPHIFY — NARROW THE QUERY\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "\n"
                f"  graphify query is STILL TRUNCATED at --budget {budget} (max tier).\n"
                "  Narrow the query — fewer/more specific terms, or add --context.\n"
                "\n"
                "  ▶  graphify query \"<specific symbol or file>\" --budget 6000\n"
                "\n"
                "  No higher tier exists, so source reads are NOT blocked — but the\n"
                "  graph answer above is incomplete. Prefer narrowing the query first.\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                file=sys.stderr,
            )
            sys.exit(0)
        sys.exit(2)

    # Not truncated and non-empty output — clear any pending truncation flag.
    _clear_truncated(session_id)
    _append_event(data, {"event": "query_run", "budget": budget, "truncated": False})
    sys.exit(0)


# ── PreToolUse role ───────────────────────────────────────────────────────────

def pre_main(data: dict) -> None:
    session_id = data.get("session_id", "unknown")
    tool_name  = data.get("tool_name", "")
    tool_input = data.get("tool_input") or {}
    if not isinstance(tool_input, dict):
        sys.exit(0)

    # Explicit opt-out, named in every block message below.
    if os.environ.get("GRAPHIFY_GUARD_DISABLE") == "1":
        sys.exit(0)

    # No graphify CLI → the developer cannot satisfy this guard. graph.json is
    # committed in this repo, so the fresh-clone escape below never fires and
    # they would be blocked with no way out. Fail open instead.
    if shutil.which("graphify") is None:
        sys.exit(0)

    # If the graph hasn't been pulled yet (fresh clone), skip all enforcement.
    cwd = data.get("cwd", os.getcwd())
    if not os.path.exists(os.path.join(cwd, "graphify-out", "graph.json")):
        sys.exit(0)

    # Bash: enforce graph-first rule and truncation ladder for source-read commands.
    if tool_name == "Bash":
        command = tool_input.get("command") or ""
        if not isinstance(command, str) or not _is_bash_source_read(command, cwd):
            sys.exit(0)
        if _is_truncated_pending(session_id):
            _append_event(data, {"event": "bash_blocked_truncated", "command_preview": command[:100]})
            next_b = _next_budget(_get_truncated_budget(session_id))
            if next_b is not None:
                print(
                    "\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "⛔  GRAPHIFY BUDGET LADDER\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "\n"
                    "  Blocked: Bash source read while truncation pending.\n"
                    "\n"
                    "  The last graphify query was TRUNCATED.\n"
                    "  Re-run at the next budget tier before reading source files.\n"
                    "\n"
                    f"  ▶  graphify query \"<same terms>\" --budget {next_b}\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                    file=sys.stderr,
                )
            else:
                print(
                    "\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "⛔  GRAPHIFY — NARROW THE QUERY\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "\n"
                    "  Blocked: Bash source read while truncation pending.\n"
                    "\n"
                    "  graphify query is STILL TRUNCATED at --budget 64000 (max tier).\n"
                    "  Narrow the query — fewer/more specific terms, or add --context.\n"
                    "\n"
                    "  ▶  graphify query \"<specific symbol or file>\" --budget 6000\n"
                    "\n"
                    "  Source file reads remain BLOCKED until a non-truncated query runs.\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                    file=sys.stderr,
                )
            sys.exit(2)
        if not _is_graph_queried(session_id):
            _append_event(data, {"event": "bash_blocked_no_query", "command_preview": command[:100]})
            print(
                "\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "⛔  GRAPH-FIRST RULE\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "\n"
                f"  Blocked: Bash source read '{command[:80]}'\n"
                "\n"
                "  No graphify query has been run yet this session.\n"
                "  Query the knowledge graph FIRST, then re-issue the command.\n"
                "\n"
                "  ▶  graphify query \"<relevant concepts>\" --budget 6000\n"
                "\n"
                "  If output is truncated, re-run at the next budget tier (16000, 32000, 64000).\n"
                "  Only go to source if the graph doesn't cover it.\n"
                "\n"
                "  (This check clears once any graphify query runs in this session)\n"
                "  (Set GRAPHIFY_GUARD_DISABLE=1 to turn this guard off entirely)\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                file=sys.stderr,
            )
            sys.exit(2)
        sys.exit(0)

    # Read: enforce graph-first + truncation-rerun
    if tool_name == "Read":
        file_path = tool_input.get("file_path") or ""
        if not isinstance(file_path, str):
            sys.exit(0)
        norm_path = file_path.replace("\\", "/")
        if not _is_source_file(file_path, cwd):
            sys.exit(0)

        # Block: truncation pending
        if _is_truncated_pending(session_id):
            _append_event(data, {"event": "read_blocked_truncated", "file_path": norm_path})
            next_b = _next_budget(_get_truncated_budget(session_id))
            if next_b is not None:
                print(
                    "\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "⛔  GRAPHIFY BUDGET LADDER\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "\n"
                    f"  Blocked: Read '{file_path}'\n"
                    "\n"
                    "  The last graphify query was TRUNCATED.\n"
                    "  Re-run at the next budget tier before reading source files.\n"
                    "\n"
                    f"  ▶  graphify query \"<same terms>\" --budget {next_b}\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                    file=sys.stderr,
                )
            else:
                print(
                    "\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "⛔  GRAPHIFY — NARROW THE QUERY\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "\n"
                    f"  Blocked: Read '{file_path}'\n"
                    "\n"
                    "  graphify query is STILL TRUNCATED at --budget 64000 (max tier).\n"
                    "  Narrow the query — fewer/more specific terms, or add --context.\n"
                    "\n"
                    "  ▶  graphify query \"<specific symbol or file>\" --budget 6000\n"
                    "\n"
                    "  Source file reads remain BLOCKED until a non-truncated query runs.\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                    file=sys.stderr,
                )
            sys.exit(2)

        # Block: no graph query yet this session
        if not _is_graph_queried(session_id):
            _append_event(data, {"event": "read_blocked_no_query", "file_path": norm_path})
            print(
                "\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "⛔  GRAPH-FIRST RULE\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "\n"
                f"  Blocked: Read '{file_path}'\n"
                "\n"
                "  No graphify query has been run yet this session.\n"
                "  Query the knowledge graph FIRST, then re-issue the Read.\n"
                "\n"
                "  ▶  graphify query \"<relevant concepts>\" --budget 6000\n"
                "\n"
                "  If output is truncated, re-run at the next budget tier (16000, 32000, 64000).\n"
                "  Only go to source if the graph doesn't cover it.\n"
                "\n"
                "  (This check clears once any graphify query runs in this session)\n"
                "  (Set GRAPHIFY_GUARD_DISABLE=1 to turn this guard off entirely)\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                file=sys.stderr,
            )
            sys.exit(2)

        # Read passes through — non-source files returned above, so this is
        # unconditionally a source read.
        _append_event(data, {"event": "read_allowed", "file_path": norm_path})

    sys.exit(0)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    try:
        raw = sys.stdin.read()
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(
            f"[graphify-guard] WARNING: could not parse hook payload ({exc}). "
            "Allowing tool call — check that Claude Code is sending valid JSON.",
            file=sys.stderr,
        )
        sys.exit(0)
    except OSError:
        sys.exit(0)

    if "--post" in sys.argv:
        post_main(data)
    else:
        pre_main(data)


if __name__ == "__main__":
    main()
