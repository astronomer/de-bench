"""Generate and calibrate a task's judge_facts.yaml from its checks.yaml.

The prose judge (src/de_bench/judge.py) grades RESPONSE.md file_contains
checks from an authored fact key instead of the regex patterns. This tool
writes that key for a task:

1. TRANSLATE — one model call per prose check turns the patterns plus the
   rubric comment into plain fact groups (one group per pattern, ALL groups
   required, ANY alternate within a group) and a convicts list. Opus first;
   the call falls back to sonnet when the classifier refuses a rubric, which
   happens on a few money-heavy ones.
2. CALIBRATE — the production judge grades the task's solution/RESPONSE.md
   against the draft key. A key the solution fails is retranslated with the
   judge's findings shown, up to three rounds.
3. REVIEW — the output is a committed answer key. Read it before committing,
   the way a wrong-answer battery gets read: the known failure modes are
   attribution over-specification ("the figure AS the finance team's count"
   when the check only pins the figure), frame-blindness (facts about the
   pre-fix world convicting statements about the post-fix world), and
   AND-ifying a pattern's alternation. Finding 015 records worked examples.

Needs the LLM gateway env (ANTHROPIC_BASE_URL, ANTHROPIC_API_KEY). Usage:

    PYTHONPATH=src python tools/gen_judge_facts.py tasks/copperline/task-copperline-<id>
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import yaml

from de_bench.judge import SYSTEM as JUDGE_SYSTEM  # noqa: F401  (kept close on purpose)
from de_bench.judge import _post, judge_check

TRANSLATE_SYSTEM = """You translate a benchmark check from regex form into plain graded facts.

You receive the check author's rubric comment and the check's regex patterns. The
grading semantics: ALL patterns must match for a pass; within one pattern, the `|`
alternates mean ANY ONE suffices.

Write the facts a grader should require, preserving those semantics exactly: one
group per pattern, in order; inside a group, one bullet per distinct alternate fact.
Rules:
- State each fact as a checkable claim with its exact figures, names, polarity and
  attribution — and nothing more than the pattern actually pins: never add whose
  figure it is, or which file it comes from, unless the pattern or comment grades that.
- Collapse spelling variants into one bullet — the judge accepts any wording.
- Keep an alternation an alternation: alternates in one pattern become bullets in
  ONE group, never separate groups.
- Carry over the rubric's exclusions and its wrong answers as a `convicts` list;
  scope each convict to the world as the ticket found it, so a statement about the
  answer's own fix is never convicted by a fact about the broken state.
- Never mention regex, patterns, or matching. Facts only."""

TOOL = {
    "name": "facts",
    "description": "The translated fact groups.",
    "input_schema": {
        "type": "object",
        "properties": {
            "groups": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"any_of": {"type": "array", "items": {"type": "string"}}},
                    "required": ["any_of"],
                },
            },
            "convicts": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["groups", "convicts"],
    },
}


def prose_checks(task_dir: Path) -> list[dict]:
    lines = (task_dir / "checks.yaml").read_text().splitlines()
    starts = [i for i, ln in enumerate(lines) if ln.startswith("- kind:")]
    out, seq = [], 0
    for n, s in enumerate(starts):
        end = starts[n + 1] if n + 1 < len(starts) else len(lines)
        body = "\n".join(lines[s:end])
        if not body.startswith("- kind: file_contains") or "path: RESPONSE.md" not in body:
            continue
        com, i = [], s - 1
        while i >= 0 and (lines[i].startswith("#") or not lines[i].strip()):
            if lines[i].startswith("#"):
                com.append(lines[i].lstrip("# ").rstrip())
            i -= 1
        com.reverse()
        pats = [m.group(1) for ln in lines[s:end]
                if (m := re.match(r"\s+- ['\"](.*)['\"]\s*$", ln))]
        out.append({"seq": seq, "comment": "\n".join(com), "patterns": pats})
        seq += 1
    return out


def translate(check: dict, feedback: str = "") -> dict | None:
    pats = "\n".join(f"{i+1}. `{p}`" for i, p in enumerate(check["patterns"]))
    user = f"## Rubric comment\n{check['comment']}\n\n## Patterns\n{pats}{feedback}"
    for model in ("claude-opus-5", "claude-sonnet-5"):
        resp = _post({
            "model": model, "max_tokens": 4000, "system": TRANSLATE_SYSTEM,
            "tools": [TOOL], "tool_choice": {"type": "tool", "name": "facts"},
            "messages": [{"role": "user", "content": user}],
        })
        for b in resp.get("content") or []:
            if b.get("type") == "tool_use" and b.get("input", {}).get("groups"):
                return b["input"]
    return None


def main() -> int:
    task_dir = Path(sys.argv[1])
    checks = prose_checks(task_dir)
    if not checks:
        print(f"{task_dir.name}: no RESPONSE.md prose checks")
        return 0
    solution = task_dir / "solution/RESPONSE.md"
    sol_text = solution.read_text() if solution.exists() else None
    entries = []
    for c in checks:
        facts = translate(c)
        for _round in range(3):
            if facts is None or sol_text is None:
                break
            verdict = judge_check({**facts, "background": c["comment"]}, sol_text)
            if verdict["passed"]:
                break
            feedback = (
                "\n\n## Your previous translation FAILED the task's known-correct solution\n"
                f"Findings: {verdict['detail']}\n\n## The solution (must pass)\n"
                f"<answer>\n{sol_text}\n</answer>\n"
                "Rewrite so the solution passes on substance; do not copy its phrasing."
            )
            facts = translate(c, feedback) or facts
        if facts is None:
            print(f"  seq {c['seq']}: TRANSLATION FAILED — write it by hand")
            continue
        entries.append({"seq": c["seq"], "background": c["comment"],
                        "groups": facts["groups"], "convicts": facts.get("convicts") or []})
        print(f"  seq {c['seq']}: ok ({len(facts['groups'])} groups)")
    out = task_dir / "judge_facts.yaml"
    out.write_text(
        "# The judge's answer key for this task's RESPONSE.md prose checks — reviewed\n"
        "# before commit; see tools/gen_judge_facts.py for the method and the known\n"
        "# translation failure modes.\n"
        + yaml.safe_dump({"checks": entries}, sort_keys=False, width=88, allow_unicode=True)
    )
    print(f"wrote {out} ({len(entries)}/{len(checks)} checks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
