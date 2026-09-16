"""Agent registry: how to invoke each harness inside the trial container.

Every agent gets the same contract: a workspace at /work, a task prompt, a shared
system prompt, a model, and a wall-clock cap enforced by the runner. An adapter is
one module in this package defining an `AGENT` (argv builder + telemetry parsers +
where its session file lands), registered here. Anything an agent needs installed
goes into the shared trial image in modal_app.py — same container for everyone.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


@dataclass(frozen=True)
class Telemetry:
    turns: int = 0
    tool_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    cost_usd: float = 0.0

    def as_dict(self) -> dict:
        return self.__dict__.copy()


@dataclass(frozen=True)
class Agent:
    """One harness.

    - build_argv(prompt, system_prompt, model, thinking) -> argv run at /work.
    - parse_telemetry(stdout) -> Telemetry, best-effort; absence is never an error.
    - trajectory_source: source id for the `agent-trajectory` normalizer, if supported.
    - step_usage(stdout) -> per-assistant-step usage rows for usage.jsonl.
    - session_dir: directory inside the container where the harness writes its
      native session file(s); the newest *.jsonl under it (recursive) is captured.
    - resolve_model(requested) -> the model this harness actually runs (e.g. codex
      only speaks the gateway's OpenAI routes, so claude-* maps to a gpt tier).
    - pre_run(env): runs in-container right before the agent (write config files
      the CLI expects, etc.).
    """

    name: str
    build_argv: Callable[[str, str, str, str], list[str]]
    parse_telemetry: Callable[[str], Telemetry]
    trajectory_source: str | None = None
    step_usage: Callable[[str], list[dict]] | None = None
    session_dir: str | None = None
    resolve_model: Callable[[str], str] = field(default=lambda m: m)
    pre_run: Callable[[dict], None] | None = None
    #: argv that prints the harness version, run once per trial for provenance.
    version_argv: tuple[str, ...] | None = None


from de_bench.agents.claude_code import AGENT as _claude_code  # noqa: E402
from de_bench.agents.codex import AGENT as _codex  # noqa: E402
from de_bench.agents.opencode import AGENT as _opencode  # noqa: E402
from de_bench.agents.pi import AGENT as _pi  # noqa: E402

AGENTS: dict[str, Agent] = {
    _pi.name: _pi,
    _claude_code.name: _claude_code,
    _codex.name: _codex,
    _opencode.name: _opencode,
}
