"""Run-validity: keep infrastructure failures out of capability numbers.

Borrowed from airflow-bench's outcome taxonomy (runner/outcome.py there), built
after a run where 50 expired-auth trials were scored as agents trying and
failing. Every trial resolves to `scored` or `infra_failed`; infra rows are
excluded from cost aggregates and reported separately.
"""

from __future__ import annotations

import re

# Markers an agent cannot produce by trying the task and failing it.
#
# Every alternative here has to carry its own auth context. A bare `\b401\b` used to
# be enough, and it matched the ticket number in `Ticket UP-401`, which appears in
# the prompt echo at the head of the transcript — so 13 trials that did the task were
# filed as infrastructure failures. A number on its own says nothing.
_INFRA_RE = re.compile(
    r"\b(401|403)\b[^\n]{0,60}(unauthorized|forbidden|authentication|invalid)"
    r"|(unauthorized|forbidden)[^\n]{0,20}\b(401|403)\b"
    r"|authentication[_ ]failed"
    r"|invalid (api key|authentication credentials)"
    r"|(api|auth)[_ ]?key.{0,40}(missing|not set|invalid)"
    # Only the harness's own binary going missing is infrastructure. Agents probe for
    # tools that were never promised (`jq`, `docker`) and echo their own `command -v`
    # scripts into stderr, and plain `command not found` read both as failure.
    r"|\b(pi|claude|codex|opencode|otto|astro|uv): command not found"
    r"|startup_failed",
    re.IGNORECASE,
)

# A harness that stops to ask permission cannot be answered here, so it gives up
# having done nothing. That is the container being wrong, not the agent failing
# the task — and it hides well, because the harness still streams a large event
# log on its way to doing nothing.
_BLOCKED_RE = re.compile(
    r"permission requested"
    r"|permission denied by user"
    r"|awaiting (user )?approval"
    r"|auto-reject"
    r"|rejected permission to use"
    r"|approval needed",
    re.IGNORECASE,
)


# The provider errored and the harness ended the run there. Each harness
# spells it differently, and most exit 0 having said nothing to stderr:
#   pi           "stopReason": "error" (+ errorMessage)
#   claude-code  "isApiErrorMessage":true / "error":"server_error"
#   codex        "error":{"message":"stream disconnected before completion..."}
# The first version of this rule spoke only pi's dialect and required an empty
# patch — but a run the gateway kills after it wrote files keeps its partial
# patch and is still a truncated run, not a capability sample. 155 such rows
# in one sweep scored as real attempts (codex-luna read 21 points low). The
# error must sit at the session's END: a run that hit an error, recovered, and
# kept working is a real attempt and stays scored.
_SESSION_ERROR_RE = re.compile(
    r'"stopReason"\s*:\s*"error"'
    r'|"errorMessage"\s*:'
    r'|"isApiErrorMessage"\s*:\s*true'
    r'|"error"\s*:\s*"(server_error|overloaded_error|api_error)"'
    r'|"error"\s*:\s*\{[^}]{0,200}(stream disconnected|response\.failed|server_error)'
    r'|stream disconnected before completion'
)


def _session_ended_in_error(session: str) -> bool:
    """True when the session's final events carry a provider error."""
    tail_lines = (session or "").strip().splitlines()[-3:]
    return bool(_SESSION_ERROR_RE.search("\n".join(tail_lines)[-6000:]))


def classify_outcome(status: str, transcript: str, stderr: str, patch_bytes: int,
                     session: str = "") -> tuple[str, list[str]]:
    """(outcome, reasons) for one trial. Precedence: infra beats everything.

    wall_cap and nonzero-exit-after-real-work stay `scored` — spending the budget
    or erroring after substantive work is capability evidence, not infra.

    Both screens read stderr AND the transcript, because which of the two a harness
    writes to is the harness's business: pi and claude-code write no stderr at all,
    so a stderr-only permission screen covered half the matrix in name only. The one
    that mattered — opencode auto-rejecting every path outside /work, which killed 31
    trials — announced itself in stderr, but nothing says the next one will.
    """
    reasons: list[str] = []
    tail = (stderr or "")[-4000:] + "\n" + (transcript or "")[:2000]
    if _INFRA_RE.search(tail):
        reasons.append("infra_marker")
    if len(transcript or "") < 200 and patch_bytes == 0 and status != "wall_cap":
        reasons.append("no_output")
    # Blocked on a prompt and left nothing behind. The empty patch is the tell:
    # a harness that asked, was ignored, and gave up did not attempt the task.
    blocked = (stderr or "")[-8000:] + "\n" + (transcript or "")[-8000:]
    if patch_bytes == 0 and _BLOCKED_RE.search(blocked):
        reasons.append("blocked_on_permission")
    # A session whose last words are an API error. Whether files were already
    # written does not matter — the run was truncated by the environment
    # either way. Only the final events count, so an error the harness
    # recovered from still scores.
    if _session_ended_in_error(session):
        reasons.append("api_error_ended_run")
    if reasons:
        return "infra_failed", reasons
    return "scored", []
