"""codex (OpenAI Codex CLI): headless `codex exec --json` against the LLM gateway.

codex only speaks OpenAI wire APIs, so requested claude tiers map to the gateway's
gpt tiers (resolve_model), and pre_run writes a ~/.codex/config.toml declaring a
custom model provider that points codex's "responses" wire API at the gateway's
OpenAI routes (`${ANTHROPIC_BASE_URL}/v1`, bearer-authed with ANTHROPIC_API_KEY).
"""

from __future__ import annotations

import json
import os

from de_bench.agents import Agent, Telemetry

#: Where codex writes its rollout session files inside the trial container
#: (nested by date, e.g. sessions/2026/08/05/rollout-*.jsonl — the runner's
#: recursive newest-.jsonl capture handles that).
SESSION_DIR = "/root/.codex/sessions"

#: Gateway $/MTok (input, output) for the OpenAI models it serves. Cache reads
#: bill at 10% of the input price; cache writes are free.
PRICES: dict[str, tuple[float, float]] = {
    "gpt-5.6-sol": (5.0, 30.0),
    "gpt-5.6-terra": (2.5, 15.0),
    "gpt-5.6-luna": (1.0, 6.0),
}

#: claude tier -> nearest gateway gpt tier.
_CLAUDE_TIERS = {
    "haiku": "gpt-5.6-luna",
    "sonnet": "gpt-5.6-terra",
    "opus": "gpt-5.6-sol",
}

#: codex's `--json` events carry no model id, but cost depends on it. Both
#: resolve_model (called client-side when specs are built) and build_argv (called
#: in-container right before the run) record the model here, so the telemetry
#: parsers running later in the same process can price the tokens.
_current_model = "gpt-5.6-luna"


def resolve_model(requested: str) -> str:
    """codex can't serve claude models; map claude tiers to gateway gpt tiers."""
    global _current_model
    if requested.startswith("gpt-5.6-"):
        resolved = requested
    else:
        resolved = next(
            (gpt for tier, gpt in _CLAUDE_TIERS.items() if tier in requested),
            "gpt-5.6-luna",
        )
    _current_model = resolved
    return resolved


def pre_run(env: dict) -> None:
    """Write ~/.codex/config.toml declaring the gateway as a custom model provider,
    and put the trial's PATH somewhere a login shell will find it.

    codex runs every command through `bash -lc`. A login shell sources /etc/profile,
    which resets PATH to the Debian default — so the two directories the trial adds,
    /root/.astro/bin (the `af` shim) and the per-task Airflow venv, were gone by the
    time the agent's command ran. Exported variables survived, which is what made it
    hard to see: the agent had AIRFLOW_HOME pointing at a live environment it could
    not reach. `af: command not found` in 190 trials, against roughly zero for every
    other harness, and codex graded on the image's stock Airflow rather than the one
    built for the task.
    """
    profile_d = "/etc/profile.d"
    os.makedirs(profile_d, exist_ok=True)
    with open(os.path.join(profile_d, "de-bench-path.sh"), "w") as f:
        f.write(f'export PATH="{env.get("PATH", os.environ.get("PATH", ""))}"\n')

    base = (env.get("ANTHROPIC_BASE_URL") or "").rstrip("/")
    # codex appends /responses to base_url, and the gateway serves OpenAI models
    # under /v1 — normalize so we end up at exactly one /v1.
    if base.endswith("/v1"):
        base = base[: -len("/v1")]
    codex_home = os.path.join(env.get("HOME", "/root"), ".codex")
    os.makedirs(codex_home, exist_ok=True)
    config = (
        'model_provider = "astro"\n'
        "\n"
        "[model_providers.astro]\n"
        'name = "LLM gateway"\n'
        f'base_url = "{base}/v1"\n'
        'env_key = "ANTHROPIC_API_KEY"\n'
        'wire_api = "responses"\n'
    )
    with open(os.path.join(codex_home, "config.toml"), "w") as f:
        f.write(config)


def build_argv(prompt: str, system_prompt: str, model: str, thinking: str) -> list[str]:
    """codex in headless JSON mode: JSONL events on stdout, exits when the turn ends."""
    global _current_model
    _current_model = model
    # codex's reasoning effort only spans low..high; clamp the outer levels.
    effort = {"off": "low", "minimal": "low", "xhigh": "high", "max": "high"}.get(thinking, thinking)
    if effort not in ("low", "medium", "high"):
        effort = "low"
    argv = [
        "codex",
        "exec",
        "--json",
        "--skip-git-repo-check",
        # The Modal container is the sandbox; don't let codex try to nest its own.
        "--dangerously-bypass-approvals-and-sandbox",
        "-m",
        model,
        "-c",
        f'model_reasoning_effort="{effort}"',
    ]
    # codex has no append-system-prompt flag, so the benchmark system prompt is
    # prepended to the task prompt with a separator instead.
    argv.append(f"{system_prompt}\n\n---\n\n{prompt}" if system_prompt else prompt)
    return argv


#: item.completed item types that represent tool use (vs. agent_message/reasoning).
_TOOL_ITEM_TYPES = {"command_execution", "file_change", "mcp_tool_call", "web_search"}


def _cost_usd(input_tokens: int, output_tokens: int, cache_read_tokens: int) -> float:
    in_price, out_price = PRICES.get(_current_model, PRICES["gpt-5.6-luna"])
    return (
        input_tokens * in_price
        + output_tokens * out_price
        + cache_read_tokens * 0.1 * in_price
    ) / 1e6


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
    """Telemetry from codex's JSONL event stream (`exec --json`).

    One `turn.completed` per user turn (so one per exec run) carries the turn's
    aggregate usage; `input_tokens` includes the cached share reported as
    `cached_input_tokens`, so the uncached part is the difference. The gateway
    also reports `cache_write_input_tokens` — a subset of the uncached input,
    already billed at the input rate (cache writes carry no surcharge on these
    models), so it's recorded but doesn't enter the cost. codex reports no cost,
    so it's computed from the gateway price table.
    """
    turns = tool_calls = 0
    inp = out = cache_r = cache_w = 0

    for event in _events(transcript):
        kind = event.get("type")
        if kind == "turn.completed":
            turns += 1
            u = event.get("usage") or {}
            cached = int(u.get("cached_input_tokens") or 0)
            inp += max(int(u.get("input_tokens") or 0) - cached, 0)
            out += int(u.get("output_tokens") or 0)
            cache_r += cached
            cache_w += int(u.get("cache_write_input_tokens") or 0)
        elif kind == "item.completed":
            if (event.get("item") or {}).get("type") in _TOOL_ITEM_TYPES:
                tool_calls += 1

    return Telemetry(
        turns=turns,
        tool_calls=tool_calls,
        input_tokens=inp,
        output_tokens=out,
        cache_read_tokens=cache_r,
        cache_write_tokens=cache_w,
        cost_usd=round(_cost_usd(inp, out, cache_r), 6),
    )


def step_usage(transcript: str) -> list[dict]:
    """Per-turn usage rows from codex's JSONL event stream.

    codex reports usage only at `turn.completed` (aggregate over the turn's API
    calls), not per assistant message, so rows are per turn — usually exactly one
    for an exec run — and pair with trajectory records only loosely. Events carry
    no timestamps either.
    """
    rows: list[dict] = []
    step = 0
    for event in _events(transcript):
        if event.get("type") != "turn.completed":
            continue
        u = event.get("usage") or {}
        cached = int(u.get("cached_input_tokens") or 0)
        inp = max(int(u.get("input_tokens") or 0) - cached, 0)
        out = int(u.get("output_tokens") or 0)
        rows.append(
            {
                "step": step,
                "timestamp": None,
                "model": _current_model,
                "input_tokens": inp,
                "output_tokens": out,
                "cache_read_tokens": cached,
                "cache_write_tokens": int(u.get("cache_write_input_tokens") or 0),
                "cost_usd": round(_cost_usd(inp, out, cached), 6),
            }
        )
        step += 1
    return rows


AGENT = Agent(
    name="codex",
    build_argv=build_argv,
    parse_telemetry=parse_telemetry,
    trajectory_source="codex",
    step_usage=step_usage,
    session_dir=SESSION_DIR,
    resolve_model=resolve_model,
    pre_run=pre_run,
    version_argv=("codex", "--version"),
)
