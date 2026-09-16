"""The prose judge: grades a RESPONSE.md file_contains check from the task's
judge_facts.yaml instead of running its regex patterns.

Why a judge. Three trace audits (findings 010-012) each found a fresh crop of
correct answers convicted by phrasing — wrapped lines, en-dashes, unpredicted
vocabulary — and the prosecution audit found the mirror image, wrong answers
slipping through broadened patterns. Regex cannot read negation or attribution;
a reader can. The judge grades substance from an authored fact key and never
sees a regex.

What keeps it honest, in order of importance:
- The fact key is committed and reviewed (`judge_facts.yaml` beside each
  checks.yaml), calibrated so every task's solution passes it and the
  wrong-answer batteries fail it. The judge decides "does this text state
  fact X", never "is this a good answer".
- The judge is blind: it sees the rubric and the answer, never which agent,
  model, or harness wrote the answer.
- A conviction takes two agreeing verdicts. A single pass verdict stands (the
  measured failure mode is stochastic false convictions, not false grants:
  0 false grants across every calibrated evaluation run); a fail verdict is
  re-judged, and a split goes to a third call, majority rules.
- Every verdict ships its per-group findings into score.json, so a scored run
  carries the judge's reasoning the way it carries a regex's matched pattern.

Model access goes through the LLM gateway the trial containers already use
(ANTHROPIC_BASE_URL + ANTHROPIC_API_KEY), with no SDK dependency — one urllib
POST per call, so any scoring image can judge.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

#: Bump when the system prompt or tool schema changes meaning — recorded in
#: every verdict's detail so two runs' verdicts can be compared honestly.
#: v3, the lean contract, measured 188/188 on the hand-verified evaluation
#: set with zero errors: fact groups only, read positionally. The convicts
#: lists stay in judge_facts.yaml as authoring documentation but never reach
#: the judge — every channel that carried them at runtime misfired (reported
#: as satisfied=false rows they read as missing groups; as a dedicated field
#: the model over-applied them to solutions), and the measured wrong answers
#: are all convicted by group failures plus the hedge rule alone.
PROMPT_VERSION = 3

#: Primary judge. Finding 016 measured opus primary at 3.4x lower re-score
#: variance than sonnet primary (±0.64 vs ±2.19 pts over 4 passes, same
#: mean) — swapped on that evidence, same cost, no code path changed.
JUDGE_MODEL = os.environ.get("DE_BENCH_JUDGE_MODEL", "claude-opus-5")

#: A conviction is confirmed by a DIFFERENT model. Same-model confirmation
#: proved worthless against correlated misfires — a boolean-inversion noise
#: mode (finding quotes the fact, satisfied says false) repeated across two
#: sonnet calls on the same input; opus does not share the correlation.
CONFIRM_MODEL = os.environ.get("DE_BENCH_JUDGE_CONFIRM_MODEL", "claude-sonnet-5")

SYSTEM = """You are the grader for a data-engineering benchmark. You receive the \
grading rubric for one check — background from the check author, then numbered fact \
groups — and one written answer (a RESPONSE.md an agent delivered for a ticket).

Decide strictly:
- The check PASSES only if, for EVERY numbered group, the answer states at least ONE \
of that group's facts. Any wording, layout, table, or line-wrapping counts; the \
substance must be there.
- A figure counts only at its exact value (formatting free: commas, spaces, units, \
sign-as-liability). A close figure is a different figure.
- A fact counts only with its stated polarity and attribution: a number attached to \
the wrong thing, or a mechanism assigned to the wrong side, is not the fact.
- The fact must be committed: a guess, a "most likely", or a statement inside a \
sentence that denies it does not count.
- Judge only what is written in the answer. Decide each group first, then report; \
your verdict must equal what your group decisions imply."""

_TOOL = {
    "name": "verdict",
    "description": (
        "Deliver the grading verdict, one entry per numbered group. For each group, "
        "first write `finding` — the quote from the answer that states one of the "
        "group's facts, or a statement of what is missing — and only then set "
        "`satisfied` to match the finding."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "patterns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "n": {"type": "integer"},
                        "finding": {
                            "type": "string",
                            "description": "quote from the answer that states one of this group's facts, or what is missing; write this before deciding",
                        },
                        "satisfied": {
                            "type": "boolean",
                            "description": "must follow from the finding just written",
                        },
                    },
                    "required": ["n", "finding", "satisfied"],
                },
            },
            "verdict": {"type": "string", "enum": ["pass", "fail"]},
        },
        "required": ["patterns", "verdict"],
    },
}


def _build_user(facts: dict, text: str) -> str:
    groups = []
    for i, g in enumerate(facts["groups"]):
        alts = g["any_of"] if isinstance(g, dict) else [g]
        if isinstance(alts, str):
            alts = [alts]
        lines = "\n".join(f"   - {a}" for a in alts)
        groups.append(f"{i+1}. At least one of:\n{lines}")
    background = facts.get("background") or ""
    return (
        f"## Background (the check author's derivation)\n{background}\n\n"
        f"## Required fact groups (ALL groups; ANY one fact within a group)\n"
        + "\n".join(groups)
        + f"\n\n## The answer to grade\n<answer>\n{text}\n</answer>"
    )


def _post(body: dict, timeout: int = 120) -> dict:
    base = (os.environ.get("ANTHROPIC_BASE_URL") or "").rstrip("/")
    key = os.environ.get("ANTHROPIC_API_KEY") or ""
    if not base or not key:
        raise RuntimeError("judge: ANTHROPIC_BASE_URL/ANTHROPIC_API_KEY not set")
    req = urllib.request.Request(
        f"{base}/v1/messages",
        data=json.dumps(body).encode(),
        headers={
            "content-type": "application/json",
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _one_verdict(
    user: str, n_groups: int, model: str = None, rejects: list | None = None
) -> tuple[str, list] | None:
    """One judge call. Returns (verdict, findings) or None on a malformed reply.

    `rejects` collects a one-line reason per discarded attempt. Without it a
    no-verdict is undiagnosable: the reply is dropped and the caller only
    learns that four attempts failed, never what came back. Measured once at
    4 trials in 107,851 gradings, all reproducible, none explicable from the
    stored score — see RCA-judge-no-structured-verdict.md.
    """
    body = {
        "model": model or JUDGE_MODEL,
        "max_tokens": 3000,
        "system": SYSTEM,
        "tools": [_TOOL],
        "tool_choice": {"type": "tool", "name": "verdict"},
        "messages": [{"role": "user", "content": user}],
    }
    last_err = None
    for attempt in range(4):
        try:
            resp = _post(body)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
            last_err = exc
            if rejects is not None:
                rejects.append(f"a{attempt}: {type(exc).__name__}")
            time.sleep(2 * (attempt + 1))
            continue
        if rejects is not None:
            kinds = [b.get("type") for b in (resp.get("content") or [])]
            rejects.append(
                f"a{attempt}: stop={resp.get('stop_reason')} blocks={kinds}"
            )
        for block in resp.get("content") or []:
            if block.get("type") != "tool_use":
                continue
            inp = block.get("input") or {}
            # The model sometimes double-encodes: `patterns` arrives as a JSON
            # string holding the whole verdict object. Unwrap it.
            if isinstance(inp.get("patterns"), str):
                try:
                    decoded = json.loads(inp["patterns"])
                    if isinstance(decoded, dict):
                        inp = decoded
                    elif isinstance(decoded, list):
                        inp = {**inp, "patterns": decoded}
                except Exception:  # noqa: BLE001
                    pass
            detail = inp.get("patterns")
            if (
                detail
                and isinstance(detail, list)
                and all(isinstance(p, dict) and "satisfied" in p for p in detail)
            ):
                # Only the fact groups decide; the model sometimes appends
                # CONVICTS rows with satisfied=false meaning "not asserted",
                # which must never read as a missing group. The read is
                # positional — groups come first, in order — because the
                # model's numbering is unreliable (an n-based filter turned
                # 0-based numbering into empty groups and mass convictions).
                if len(detail) < n_groups:
                    if rejects is not None:
                        rejects[-1] += f" short:{len(detail)}<{n_groups}"
                    continue  # short structure — retry
                groups = detail[:n_groups]
                verdict = ("fail" if not all(p.get("satisfied") for p in groups)
                           else "pass")
                findings = [
                    f"group {p.get('n')}: {'ok' if p.get('satisfied') else 'MISSING'} — {str(p.get('finding'))[:200]}"
                    for p in groups
                ]
                return verdict, findings
        else:
            # No tool_use block yielded a usable structure. Record what the
            # reply actually carried so the next occurrence is diagnosable.
            if rejects is not None and not rejects[-1].endswith(">"):
                shape = "no tool_use block"
                for block in resp.get("content") or []:
                    if block.get("type") == "tool_use":
                        inp = block.get("input") or {}
                        pats = inp.get("patterns")
                        shape = (
                            f"patterns={type(pats).__name__}"
                            f"/{len(pats) if isinstance(pats, (list, str)) else '?'}"
                            f" keys={sorted(inp)[:4]}"
                        )
                        break
                rejects[-1] += f" {shape}"
    if last_err is not None:
        raise RuntimeError(f"judge: gateway unreachable ({last_err})")
    return None


def judge_check(facts: dict, text: str) -> dict:
    """Grade one prose check. A pass verdict stands alone; a conviction needs
    the CONFIRM_MODEL to agree, and a split goes to a third call (the primary
    model again), majority rules."""
    user = _build_user(facts, text)
    n_groups = len(facts["groups"])
    calls = []
    rejects: list[str] = []
    v = _one_verdict(user, n_groups, rejects=rejects)
    if v is None:
        # A grading failure, not an agent failure. `judge_error` marks it so a
        # harness fault is never silently read as the agent getting it wrong;
        # the rejects trail says what each attempt actually returned.
        return {
            "passed": False,
            "judge_error": "no_structured_verdict",
            "detail": "judge: no structured verdict after retries — " + "; ".join(rejects),
        }
    calls.append(v)
    if v[0] == "fail":
        v2 = _one_verdict(user, n_groups, model=CONFIRM_MODEL)
        if v2 is not None:
            calls.append(v2)
            if v2[0] == "pass":
                v3 = _one_verdict(user, n_groups)
                if v3 is not None:
                    calls.append(v3)
    fails = sum(1 for c in calls if c[0] == "fail")
    passed = not (fails >= 2 or (fails == 1 and len(calls) == 1))
    # The findings shipped are the deciding call's: the last fail for a
    # conviction, the last pass otherwise.
    want = "fail" if not passed else "pass"
    findings = next(c[1] for c in reversed(calls) if c[0] == want)
    tag = f"judge {JUDGE_MODEL}+{CONFIRM_MODEL} v{PROMPT_VERSION}, {len(calls)} call(s)"
    return {
        "passed": passed,
        "detail": f"{tag}: {'; '.join(findings)[:900]}",
    }
