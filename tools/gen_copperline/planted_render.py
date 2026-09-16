"""Render PLANTED.md's generated half from the two config files.

The generated half cannot disagree with the data, because it is the same
input rendered a second way. A repo-side test regenerates it and fails on
any difference. The hand-written half (defect pairs, trap-conservation
measurements, the flaw inventory) lives below the marker and is never
touched here.
"""

from __future__ import annotations

MARKER = "<!-- generated-half-ends: everything below is hand-written -->"

_HEADER = """# The copperline answer key

This file never ships. The half above the marker is rendered by
`python -m gen_copperline --render-planted`; edit timeline.yaml or
planted.yaml instead of editing it.
"""


def render(cfg: dict, planted: list[dict]) -> str:
    lines = [_HEADER]

    lines.append("## Eras\n")
    lines.append("| era | when | note |")
    lines.append("|---|---|---|")
    for era_id, era in (cfg.get("eras") or {}).items():
        when = era.get("at") or f"{era.get('from', '')} to {era.get('to', 'open')}"
        lines.append(f"| {era_id} | {when} | {era.get('note', '')} |")

    lines.append("\n## Clause-to-row map\n")
    if planted:
        lines.append("| planted row | clause | tasks | table | ds | action |")
        lines.append("|---|---|---|---|---|---|")
        for row in planted:
            lines.append(
                f"| {row['id']} | {row['clause']} | {', '.join(row['tasks'])} "
                f"| {row['table']} | {row['ds']} | {row.get('action', 'insert')} |"
            )
    else:
        lines.append("No rows planted yet.")

    lines.append("")
    lines.append(MARKER)
    return "\n".join(lines) + "\n"


def write(path: str, cfg: dict, planted: list[dict]) -> None:
    from pathlib import Path

    target = Path(path)
    hand_written = ""
    if target.exists():
        text = target.read_text(encoding="utf-8")
        if MARKER in text:
            hand_written = text.split(MARKER, 1)[1]
    target.write_text(render(cfg, planted) + hand_written, encoding="utf-8")
