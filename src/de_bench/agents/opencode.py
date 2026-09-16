"""opencode (opencode-ai): SST's open-source coding agent.

Runs `opencode run` headless with `--format json` against the LLM gateway: pre_run
writes ~/.config/opencode/opencode.json pointing the anthropic provider's baseURL at
the gateway (token stays in the ANTHROPIC_API_KEY env var via opencode's `{env:...}`
substitution) and granting edit/bash/webfetch up front, since a headless run turns
"ask" into a denial. Behavior below was verified against opencode 1.18.14, the
version pinned in the image.
"""

from __future__ import annotations

import json
import shlex

from de_bench.agents import Agent, Telemetry

CONFIG_DIR = "/root/.config/opencode"

#: Benchmark system prompt file, referenced from config `instructions` (opencode's
#: append-to-system-prompt mechanism); build_argv's wrapper writes it before the run.
SYSTEM_PROMPT_PATH = f"{CONFIG_DIR}/de-bench-system-prompt.md"

#: opencode keeps sessions in a SQLite db (~/.local/share/opencode/opencode.db), not
#: files, so build_argv's wrapper runs `opencode export <sessionID>` after the run and
#: drops the result here — one JSON document, named .jsonl so the runner captures it.
#: It is exactly the {info, messages: [{info, parts}]} shape trajectory's "opencode"
#: source parses. A wall-capped trial dies before the export, leaving no session.
SESSION_DIR = "/tmp/opencode-session"

_RUN_LOG = "/tmp/opencode-run.jsonl"

#: opencode maps reasoning effort per model as named variants; its anthropic models
#: define only "high" (16k thinking budget) and "max" (32k). Anything else — including
#: de-bench's default "low" — runs without extended thinking.
_THINKING_VARIANTS = {"high": "high", "max": "max"}


def build_argv(prompt: str, system_prompt: str, model: str, thinking: str) -> list[str]:
    """opencode headless, wrapped in bash for stdin, streaming, and session export.

    `opencode run` JSON-escapes a positional message argument (quotes it, backslashes
    inner quotes), so the prompt goes in on stdin, which passes it verbatim. The event
    stream is teed to a file so the wrapper can pull the session id out afterward and
    export the session for capture.

    The export runs from a SIGTERM handler as well as after a normal exit. It is a
    step in this script rather than something opencode does for itself, so a trial
    killed at the wall cap used to die before reaching it: all 13 of opencode's capped
    trials in one matrix kept a transcript and lost the session and the trajectory,
    which left the trials most worth reading as the only unreadable ones. The runner
    sends SIGTERM and waits five seconds before SIGKILL, and that is the window this
    trap uses. `opencode run` is backgrounded and waited on, because bash runs a trap
    only between commands — with it in the foreground the handler would not fire until
    it returned, which is the thing that is not happening.
    """
    run = ["opencode", "run", "--model", f"anthropic/{model}", "--format", "json"]
    variant = _THINKING_VARIANTS.get(thinking)
    if variant:
        run += ["--variant", variant]
    script = "\n".join(
        [
            "set -o pipefail",
            f"mkdir -p {CONFIG_DIR} {SESSION_DIR}",
            f"printf '%s' {shlex.quote(system_prompt)} > {SYSTEM_PROMPT_PATH}",
            "export_session() {",
            f"  sid=$(grep -o '\"sessionID\":\"[^\"]*\"' {_RUN_LOG} 2>/dev/null | head -1 | cut -d'\"' -f4)",
            '  if [ -n "$sid" ]; then',
            f'    opencode export "$sid" > {SESSION_DIR}/session.jsonl 2>/dev/null || true',
            "  fi",
            "}",
            "trap 'export_session; exit 143' TERM",
            f"printf '%s' {shlex.quote(prompt)} | {shlex.join(run)} | tee {_RUN_LOG} &",
            "wait $!",
            "rc=$?",
            "export_session",
            "exit $rc",
        ]
    )
    return ["bash", "-c", script]


def pre_run(env: dict) -> None:
    """Write opencode's config: gateway-backed anthropic provider, full autonomy."""
    import os

    # Bake the resolved gateway root in (config-side {env:...} can't normalize a
    # trailing slash or /v1); opencode's anthropic provider appends /messages.
    base = (env.get("ANTHROPIC_BASE_URL") or "").rstrip("/")
    base = base.removesuffix("/v1")
    config = {
        "$schema": "https://opencode.ai/config.json",
        "provider": {
            "anthropic": {
                "options": {
                    "baseURL": f"{base}/v1",
                    # {env:...} substitution keeps the token out of the file.
                    "apiKey": "{env:ANTHROPIC_API_KEY}",
                }
            }
        },
        "instructions": [SYSTEM_PROMPT_PATH],
        # Headless `run` turns any "ask" into a denial, and nothing can answer a
        # prompt in a benchmark container. This was a list of the categories that
        # ask — edit, bash, webfetch — until opencode added `external_directory`
        # and eleven opus trials stopped at "permission requested:
        # external_directory (/root/.af/*)" having written nothing. The schema
        # accepts a bare string as the default for every category — but that
        # default does not reach `external_directory`, which still auto-rejected.
        # It has to be named. The agent trips it constantly because /root/.af is
        # Airflow's home: every `airflow` command it runs reaches outside /work.
        "permission": {
            "read": "allow", "edit": "allow", "glob": "allow", "grep": "allow",
            "list": "allow", "bash": "allow", "task": "allow", "lsp": "allow",
            "skill": "allow", "external_directory": "allow",
            "todowrite": "allow", "question": "allow", "webfetch": "allow",
            "websearch": "allow", "doom_loop": "allow",
        },
        # Title/summary calls use the small model; keep it on the gateway too.
        "small_model": "anthropic/claude-haiku-4-5",
        "autoupdate": False,
    }
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(f"{CONFIG_DIR}/opencode.json", "w") as f:
        json.dump(config, f, indent=2)


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
    """Telemetry from opencode's JSONL event stream (`--format json`).

    Each event is {type, timestamp, sessionID, part}: one `tool_use` per tool call,
    and one `step_finish` per assistant API step whose part carries the step's token
    breakdown and cost (opencode prices from its model catalog). A step ending the
    turn has reason "stop"; tool-call steps have reason "tool-calls".
    """
    turns = tool_calls = 0
    inp = out = cache_r = cache_w = 0
    cost = 0.0

    for event in _events(transcript):
        kind = event.get("type")
        if kind == "tool_use":
            tool_calls += 1
        elif kind == "step_finish":
            part = event.get("part") or {}
            if part.get("reason") == "stop":
                turns += 1
            t = part.get("tokens") or {}
            cache = t.get("cache") or {}
            inp += int(t.get("input") or 0)
            out += int(t.get("output") or 0)
            cache_r += int(cache.get("read") or 0)
            cache_w += int(cache.get("write") or 0)
            cost += float(part.get("cost") or 0.0)

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
    """Per-step usage rows, one per `step_finish` event.

    Same shape as pi's rows. `model` is null: the event stream doesn't carry the model
    id (summary.json records it). Alignment with trajectory.jsonl is looser than pi's:
    a tool-only step emits no assistant message record there, so row *step* i does not
    reliably pair with the i-th assistant record.
    """
    rows: list[dict] = []
    step = 0
    for event in _events(transcript):
        if event.get("type") != "step_finish":
            continue
        part = event.get("part") or {}
        t = part.get("tokens") or {}
        cache = t.get("cache") or {}
        rows.append(
            {
                "step": step,
                "timestamp": event.get("timestamp"),
                "model": None,
                "input_tokens": int(t.get("input") or 0),
                "output_tokens": int(t.get("output") or 0),
                "cache_read_tokens": int(cache.get("read") or 0),
                "cache_write_tokens": int(cache.get("write") or 0),
                "cost_usd": float(part.get("cost") or 0.0),
            }
        )
        step += 1
    return rows


AGENT = Agent(
    name="opencode",
    build_argv=build_argv,
    parse_telemetry=parse_telemetry,
    trajectory_source="opencode",
    step_usage=step_usage,
    session_dir=SESSION_DIR,
    pre_run=pre_run,
    version_argv=("opencode", "--version"),
)
