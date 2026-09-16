"""claude-code (Claude Code CLI): Anthropic's official coding harness.

Runs `claude -p` in headless stream-json mode against the LLM gateway — Claude Code
honors ANTHROPIC_BASE_URL/ANTHROPIC_API_KEY natively, so the gateway secret's env
vars are all it needs (no extension trick like pi's; see modal_app.py).
"""

from __future__ import annotations

import json

from de_bench.agents import Agent, Telemetry

#: Where Claude Code writes its session files inside the trial container: one
#: <cwd-slug>/*.jsonl per project dir. The runner picks the newest .jsonl recursively.
SESSION_DIR = "/root/.claude/projects"


def build_argv(prompt: str, system_prompt: str, model: str, thinking: str) -> list[str]:
    """Claude Code headless: one prompt in, stream-json events out on stdout."""
    argv = [
        "claude",
        "-p",
        "--output-format",
        "stream-json",
        "--verbose",  # required by stream-json in print mode
        "--model",
        model,
        "--dangerously-skip-permissions",
    ]
    if system_prompt:
        argv += ["--append-system-prompt", system_prompt]
    # --effort is the CLI's thinking knob and takes the same five levels this
    # harness uses (low/medium/high/xhigh/max). Without it every run rides the
    # CLI default (medium) whatever the config says — see
    # RCA-claude-code-effort-flag.md.
    if thinking:
        argv += ["--effort", thinking]
    argv.append(prompt)
    return argv


def pre_run(env: dict) -> None:
    """Container prep Claude Code expects before a headless run.

    Running as root, the CLI refuses --dangerously-skip-permissions unless
    IS_SANDBOX=1 says the container is disposable. A fresh container also lacks
    onboarding state, so seed a minimal ~/.claude.json to skip the interactive
    first-run prompts.
    """
    import os

    env["IS_SANDBOX"] = "1"
    env["DISABLE_AUTOUPDATER"] = "1"
    env["DISABLE_TELEMETRY"] = "1"
    state_path = os.path.join(env.get("HOME", "/root"), ".claude.json")
    if not os.path.exists(state_path):
        with open(state_path, "w") as f:
            json.dump({"hasCompletedOnboarding": True, "bypassPermissionsModeAccepted": True}, f)


def _assistant_steps(transcript: str) -> list[dict]:
    """Ordered unique assistant messages from the stream-json stdout.

    The stream can emit multiple `assistant` events for one API message (same
    message.id), so usage and tool_use blocks are deduped by id before counting.
    """
    steps: dict[str, dict] = {}
    order: list[str] = []
    for event in _events(transcript):
        if event.get("type") != "assistant":
            continue
        message = event.get("message") or {}
        mid = message.get("id") or f"idx-{len(order)}"
        if mid not in steps:
            steps[mid] = {"message": message, "tool_ids": set()}
            order.append(mid)
        step = steps[mid]
        step["message"] = message
        for block in message.get("content") or []:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                step["tool_ids"].add(block.get("id") or f"tool-{len(step['tool_ids'])}")
    return [steps[m] for m in order]


def _events(transcript: str):
    for line in transcript.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            yield event


def parse_telemetry(transcript: str) -> Telemetry:
    """Telemetry from Claude Code's stream-json stdout.

    Per-step tokens come from each assistant message's `usage`; the terminal
    `result` event carries the run's total_cost_usd (per-step cost is not exposed).
    """
    inp = out = cache_r = cache_w = tool_calls = 0
    cost = 0.0

    steps = _assistant_steps(transcript)
    for step in steps:
        u = step["message"].get("usage") or {}
        inp += int(u.get("input_tokens") or 0)
        out += int(u.get("output_tokens") or 0)
        cache_r += int(u.get("cache_read_input_tokens") or 0)
        cache_w += int(u.get("cache_creation_input_tokens") or 0)
        tool_calls += len(step["tool_ids"])

    for event in _events(transcript):
        if event.get("type") == "result":
            cost = float(event.get("total_cost_usd") or 0.0)

    return Telemetry(
        turns=len(steps),
        tool_calls=tool_calls,
        input_tokens=inp,
        output_tokens=out,
        cache_read_tokens=cache_r,
        cache_write_tokens=cache_w,
        cost_usd=round(cost, 6),
    )


def step_usage(transcript: str) -> list[dict]:
    """Per-assistant-step usage rows from Claude Code's stream-json stdout.

    Same row shape as pi's: step i lines up with the i-th assistant record in the
    trajectory normalization. Claude Code only reports the run's total cost (on
    the `result` event, surfaced via parse_telemetry), so per-step cost_usd is 0.
    """
    rows: list[dict] = []
    for step, s in enumerate(_assistant_steps(transcript)):
        message = s["message"]
        u = message.get("usage") or {}
        rows.append(
            {
                "step": step,
                "timestamp": None,
                "model": message.get("model"),
                "input_tokens": int(u.get("input_tokens") or 0),
                "output_tokens": int(u.get("output_tokens") or 0),
                "cache_read_tokens": int(u.get("cache_read_input_tokens") or 0),
                "cache_write_tokens": int(u.get("cache_creation_input_tokens") or 0),
                "cost_usd": 0.0,
            }
        )
    return rows


AGENT = Agent(
    name="claude-code",
    build_argv=build_argv,
    parse_telemetry=parse_telemetry,
    trajectory_source="claude-code",
    step_usage=step_usage,
    session_dir=SESSION_DIR,
    pre_run=pre_run,
    version_argv=("claude", "--version"),
)
