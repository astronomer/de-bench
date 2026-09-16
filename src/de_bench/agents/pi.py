"""pi (pi-coding-agent): the raw-harness baseline.

Runs Astronomer's build (`@earendil-works/pi-coding-agent`) in one-shot JSON mode
against the LLM gateway — a pi extension baked into the image points pi's anthropic
provider at ANTHROPIC_BASE_URL and registers the gateway's OpenAI routes as a second
provider (see modal_app.py), so pi runs both model families.
"""

from __future__ import annotations

import json
from collections.abc import Mapping

from de_bench.agents import Agent, Telemetry

#: Where pi writes its session file inside the trial container. The session file is
#: the canonical trace: HF's trace viewer renders it as-is, and trajectory's "pi"
#: source normalizes it.
SESSION_DIR = "/tmp/pi-session"

#: The provider the image's extension registers for the gateway's OpenAI routes. Its
#: own id rather than an override of pi's built-in `openai`, because supplying models
#: replaces a provider's list.
OPENAI_PROVIDER = "astro-openai"

#: The gpt models the gateway serves, in the shape pi's `registerProvider` wants —
#: read by modal_app.py when it builds the extension. pi already knows the claude ids
#: and the extension only moves their base URL, so only this family needs a table; the
#: prices are the gateway's own, and match codex.PRICES.
#:
#: `cacheWrite` is the INPUT rate, not 0. OpenAI has no cache-write line item — the
#: gateway's OpenAI branch prices `input_tokens - cached_tokens` at the input rate
#: (astro/lib/go/llmcatalog/cost.go, ApiOpenAIResponses; no OpenAI model in
#: models.yaml carries a cache_write_per_million key). Newly-cached tokens stay
#: inside `input_tokens` there, so they bill as ordinary uncached input. pi reports
#: the three buckets disjointly instead — Anthropic's shape, where cache-write IS a
#: separate line item at 125% of input — leaving those tokens only in `cacheWrite`.
#: Pricing that bucket at 0 made every new prompt token free: a first call of 12,370
#: new tokens on sol reported $0.000165 against the gateway's $0.062. Setting it to
#: the input rate makes pi's disjoint split arithmetically equal to the gateway's
#: subtraction.
OPENAI_MODELS: tuple[Mapping[str, object], ...] = (
    {
        "id": "gpt-5.6-sol",
        "name": "GPT-5.6 Sol",
        "reasoning": True,
        "input": ["text", "image"],
        "cost": {"input": 5.0, "output": 30.0, "cacheRead": 0.5, "cacheWrite": 5.0},
        "contextWindow": 1050000,
        "maxTokens": 128000,
    },
    {
        "id": "gpt-5.6-terra",
        "name": "GPT-5.6 Terra",
        "reasoning": True,
        "input": ["text", "image"],
        "cost": {"input": 2.5, "output": 15.0, "cacheRead": 0.25, "cacheWrite": 2.5},
        "contextWindow": 1050000,
        "maxTokens": 128000,
    },
    {
        "id": "gpt-5.6-luna",
        "name": "GPT-5.6 Luna",
        "reasoning": True,
        "input": ["text", "image"],
        "cost": {"input": 1.0, "output": 6.0, "cacheRead": 0.1, "cacheWrite": 1.0},
        "contextWindow": 1050000,
        "maxTokens": 128000,
    },
)


def provider_for(model: str) -> str:
    """Which pi provider serves this model.

    By prefix rather than by table, so a claude id that shipped this morning works
    this morning — pi knows those without being told.
    """
    return OPENAI_PROVIDER if model.startswith("gpt-") else "anthropic"


def build_argv(prompt: str, system_prompt: str, model: str, thinking: str) -> list[str]:
    """pi in one-shot JSON mode: events out on stdout, exits when the turn ends."""
    argv = [
        "pi",
        "--mode",
        "json",
        "--session-dir",
        SESSION_DIR,
        "--provider",
        provider_for(model),
        "--model",
        model,
    ]
    if thinking:
        argv += ["--thinking", thinking]
    if system_prompt:
        argv += ["--append-system-prompt", system_prompt]
    argv.append(prompt)
    return argv


def parse_telemetry(transcript: str) -> Telemetry:
    """Telemetry from pi's JSONL event stream (`--mode json`).

    The stream carries streaming `message_update` events plus terminal ones; only the
    terminal events count: `turn_end` per agent turn, `tool_execution_start` per tool
    call, and per-API-call `usage` (tokens + cost breakdown) on each assistant
    `message_end`.
    """
    turns = tool_calls = 0
    inp = out = cache_r = cache_w = 0
    cost = 0.0

    for line in transcript.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        kind = event.get("type")
        if kind == "turn_end":
            turns += 1
        elif kind == "tool_execution_start":
            tool_calls += 1
        elif kind == "message_end":
            message = event.get("message") or {}
            if message.get("role") != "assistant":
                continue
            u = message.get("usage") or {}
            inp += int(u.get("input") or 0)
            out += int(u.get("output") or 0)
            cache_r += int(u.get("cacheRead") or 0)
            cache_w += int(u.get("cacheWrite") or 0)
            c = u.get("cost")
            if isinstance(c, dict):
                cost += float(c.get("total") or 0.0)
            elif isinstance(c, (int, float)):
                cost += float(c)

    return Telemetry(
        turns=turns,
        tool_calls=tool_calls,
        input_tokens=inp,
        output_tokens=out,
        cache_read_tokens=cache_r,
        cache_write_tokens=cache_w,
        cost_usd=round(cost, 6),
    )


def step_usage(transcript: str) -> list[dict]:
    """Per-assistant-step usage rows from pi's JSONL event stream.

    Row `step` counts assistant messages in order, so step i lines up with the i-th
    assistant record in the trajectory normalization of the same session. Trajectory's
    schema deliberately carries no usage (additionalProperties: false), so this
    sidecar is where the economics live.
    """
    rows: list[dict] = []
    step = 0
    for line in transcript.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict) or event.get("type") != "message_end":
            continue
        message = event.get("message") or {}
        if message.get("role") != "assistant":
            continue
        u = message.get("usage") or {}
        cost = u.get("cost") or {}
        rows.append(
            {
                "step": step,
                "timestamp": message.get("timestamp") or event.get("timestamp"),
                "model": message.get("model"),
                "input_tokens": int(u.get("input") or 0),
                "output_tokens": int(u.get("output") or 0),
                "cache_read_tokens": int(u.get("cacheRead") or 0),
                "cache_write_tokens": int(u.get("cacheWrite") or 0),
                "cost_usd": float(cost.get("total") or 0.0) if isinstance(cost, dict) else float(cost or 0.0),
            }
        )
        step += 1
    return rows


AGENT = Agent(
    name="pi",
    build_argv=build_argv,
    parse_telemetry=parse_telemetry,
    trajectory_source="pi",
    step_usage=step_usage,
    session_dir=SESSION_DIR,
    version_argv=("pi", "--version"),
)
