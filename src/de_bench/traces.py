"""Post-process a trial's raw capture into standard trace artifacts.

Three views per trial, written next to each other:

- ``session.jsonl``    — the harness's native session file, lossless ground truth
  (HF's trace viewer renders pi/claude-code/codex sessions as-is).
- ``trajectory.jsonl`` — the session normalized to Letta's trajectory-v1 records
  via the ``agent-trajectory`` package, for cross-agent analysis.
- ``usage.jsonl``      — one row per assistant step (tokens, cost). Trajectory's
  schema deliberately excludes usage (additionalProperties: false), so the
  economics live in this sidecar; row ``step`` i corresponds to the i-th
  assistant record in trajectory.jsonl.

All of this is best-effort: a missing node runtime or a format drift downgrades
to a note in summary.json, never a failed trial.
"""

from __future__ import annotations

import json
from pathlib import Path

from de_bench.agents import Agent


def emit_traces(trial_dir: Path, agent: Agent, transcript: str) -> dict:
    """Write trajectory.jsonl and usage.jsonl for a trial; return pointers + notes."""
    info: dict = {"artifacts": {}}

    if agent.step_usage is not None:
        rows = agent.step_usage(transcript)
        if rows:
            path = trial_dir / "usage.jsonl"
            path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
            info["artifacts"]["usage"] = path.name

    session_path = trial_dir / "session.jsonl"
    if agent.trajectory_source and session_path.exists() and session_path.stat().st_size > 0:
        try:
            from trajectory import normalize_transcript

            result = normalize_transcript(
                source=agent.trajectory_source,
                transcript=session_path.read_text(encoding="utf-8"),
            )
            out = trial_dir / "trajectory.jsonl"
            out.write_text("\n".join(json.dumps(r) for r in result["records"]) + "\n")
            info["artifacts"]["trajectory"] = out.name
            if result.get("diagnostics"):
                info["trajectory_diagnostics"] = len(result["diagnostics"])
        except Exception as exc:  # noqa: BLE001 - normalization is best-effort
            info["trajectory_error"] = f"{type(exc).__name__}: {exc}"

    return info
