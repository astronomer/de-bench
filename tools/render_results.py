"""Render RESULTS.md and RESULTS.png from results/sweep-2026-09.json.

The JSON holds one row per config for each view (copperline, upgrade-v2,
combined), summed from every trial's summary.json, plus one row per task with
its min, mean and max pass rate across configs.

Two adjustments are applied while rendering:

* GPT cache-write repricing. de-bench priced cache writes at $0 on the gpt-5.6
  family. OpenAI has no cache-write line item, but the tokens are billed as
  ordinary input, so the otto configs on gpt models under-reported cost by
  cache_write_tokens * input_rate. codex computed its own cost and was never
  affected; the Anthropic tiers were never affected.
* The harness that ran as `otto-dev` is the current build of otto and is
  labelled `otto`.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "results" / "sweep-2026-09.json"

GPT_INPUT_RATE = {"gpt-5.6-sol": 5.0, "gpt-5.6-terra": 2.5, "gpt-5.6-luna": 1.0}
VIEWS = (
    ("combined", "COMBINED_CONFIGS", "Copperline and upgrade-v2 together"),
    ("copperline", "CONFIGS", "Copperline"),
    ("upgrade-v2", "UV2_CONFIGS", "Upgrade-v2"),
)
THINKING_ORDER = ["minimal", "low", "medium", "high", "xhigh"]
MODEL_ORDER = ["claude-opus-5", "claude-sonnet-5", "claude-haiku-4-5", "gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"]
HARNESS_COLOR = {"otto": "#5f2fa6", "claude-code": "#c2410c", "codex": "#0f766e"}
MODEL_MARKER = {
    "claude-opus-5": "o", "claude-sonnet-5": "s", "claude-haiku-4-5": "^",
    "gpt-5.6-sol": "D", "gpt-5.6-terra": "P", "gpt-5.6-luna": "X",
}


def adjust(c: dict) -> dict:
    c = dict(c)
    if c["agent"] == "otto-dev":
        c["agent"] = "otto"
        c["name"] = c["name"].replace("otto-dev", "otto", 1)
    if c["agent"] == "otto" and c["model"] in GPT_INPUT_RATE and c["cost"] and c["total"]:
        trials = c["total"] / c["cost"]
        fix_total = (c.get("cw") or 0) * GPT_INPUT_RATE[c["model"]]
        c["cost"] = c["cost"] + fix_total / trials
        c["total"] = c["total"] + fix_total
        c["repriced"] = True
    c["k"] = 5 if c.get("trials5") else 3
    return c


def frontier(rows: list[dict]) -> set[str]:
    return {
        c["name"] for c in rows
        if not any(o["cost"] <= c["cost"] and o["p1"] >= c["p1"] and o is not c for o in rows)
    }


def money(c: dict) -> str:
    return f"${c['cost']:.2f}" + ("\\*" if c.get("repriced") else "")


def pct(x: float) -> str:
    return f"{x:.1f}%"


def signed(x: float) -> str:
    return f"{x:+.1f}"


def standings(rows: list[dict]) -> list[str]:
    rows = sorted(rows, key=lambda c: (-c["p1"], c["cost"]))
    front = frontier(rows)
    out = [
        "| # | config | model | thinking | k | Pass@1 | Pass@k | Pass^k | $/trial | median wall | frontier |",
        "|--:|---|---|---|--:|------:|------:|------:|-------:|-----------:|:--:|",
    ]
    for i, c in enumerate(rows, 1):
        out.append(
            f"| {i} | {c['name']} | {c['model']} | {c['thinking']} | {c['k']} | {pct(c['p1'])} | {pct(c['p3'])} "
            f"| {pct(c['pc'])} | {money(c)} | {c['w50']}s | {'yes' if c['name'] in front else ''} |"
        )
    return out


def by_model(rows: list[dict]) -> list[str]:
    out = ["| model | configs | mean Pass@1 | best Pass@1 | mean $/trial |", "|---|--:|------:|------:|-------:|"]
    for m in MODEL_ORDER:
        rs = [c for c in rows if c["model"] == m]
        out.append(f"| {m} | {len(rs)} | {pct(sum(c['p1'] for c in rs) / len(rs))} | {pct(max(c['p1'] for c in rs))} "
                   f"| ${sum(c['cost'] for c in rs) / len(rs):.2f} |")
    return out


def matched_tiers(rows: list[dict], rival: str) -> list[str]:
    by = {(c["agent"], c["model"], c["thinking"]): c for c in rows}
    out = [
        f"| model | thinking | otto Pass@1 | {rival} Pass@1 | gap | otto Pass^k | {rival} Pass^k | otto Pass@k | {rival} Pass@k | otto $/trial | {rival} $/trial |",
        "|---|---|------:|------:|----:|------:|------:|------:|------:|-------:|-------:|",
    ]
    for m in MODEL_ORDER:
        for t in THINKING_ORDER:
            o, r = by.get(("otto", m, t)), by.get((rival, m, t))
            if o and r:
                out.append(f"| {m} | {t} | {pct(o['p1'])} | {pct(r['p1'])} | {signed(o['p1'] - r['p1'])} | {pct(o['pc'])} | {pct(r['pc'])} "
                           f"| {pct(o['p3'])} | {pct(r['p3'])} | {money(o)} | {money(r)} |")
    return out


def matched_stats(rows: list[dict], rival: str) -> dict:
    by = {(c["agent"], c["model"], c["thinking"]): c for c in rows}
    pairs = [(by[("otto", m, t)], by[(rival, m, t)]) for (a, m, t) in by if a == "otto" and (rival, m, t) in by]
    gaps = [o["p1"] - r["p1"] for o, r in pairs]
    return {
        "n": len(pairs),
        "wins": sum(g > 0 for g in gaps),
        "min": min(gaps), "max": max(gaps),
        "pk_wins": sum(o["p3"] > r["p3"] for o, r in pairs),
        "pck_wins": sum(o["pc"] > r["pc"] for o, r in pairs),
        "cost_ratio_min": min(r["cost"] / o["cost"] for o, r in pairs),
        "cost_ratio_max": max(r["cost"] / o["cost"] for o, r in pairs),
        "dearer": sum(r["cost"] > o["cost"] for o, r in pairs),
        "dearer_min": min([((r["cost"] / o["cost"]) - 1) * 100 for o, r in pairs if r["cost"] > o["cost"]] or [0.0]),
    }


def sweeps(rows: list[dict]) -> list[str]:
    out = ["| harness | model | lowest level | highest level | Pass@1 change | cost change |", "|---|---|---|---|----:|----:|"]
    for a in ("otto", "claude-code", "codex"):
        for m in MODEL_ORDER:
            sw = sorted([c for c in rows if c["agent"] == a and c["model"] == m], key=lambda c: THINKING_ORDER.index(c["thinking"]))
            if len(sw) < 2:
                continue
            lo, hi = sw[0], sw[-1]
            out.append(f"| {a} | {m} | {lo['thinking']}: {pct(lo['p1'])} at {money(lo)} | {hi['thinking']}: {pct(hi['p1'])} at {money(hi)} "
                       f"| {signed(hi['p1'] - lo['p1'])} | x{hi['cost'] / lo['cost']:.1f} |")
    return out


def matched_price(rows: list[dict], floor: float = 53.0, within: float = 2.0) -> list[str]:
    out = [
        f"| {'rival config':<26} | Pass@1 | $/trial | cheapest otto within {within:g} points | Pass@1 | $/trial | rival cost / otto cost |",
        "|---|------:|-------:|---|------:|-------:|----:|",
    ]
    for r in sorted([c for c in rows if c["agent"] != "otto" and c["p1"] >= floor], key=lambda c: -c["p1"]):
        cands = [c for c in rows if c["agent"] == "otto" and c["p1"] >= r["p1"] - within]
        if not cands:
            continue
        o = min(cands, key=lambda c: c["cost"])
        out.append(f"| {r['name']} | {pct(r['p1'])} | {money(r)} | {o['name']} | {pct(o['p1'])} | {money(o)} | {r['cost'] / o['cost']:.2f}x |")
    return out


def chart(rows: list[dict], path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    front = frontier(rows)
    fig, ax = plt.subplots(figsize=(10, 6.5), dpi=150)
    for c in rows:
        on = c["name"] in front
        ax.scatter(c["cost"], c["p1"], s=140 if on else 55, alpha=1.0 if on else 0.4,
                   c=HARNESS_COLOR[c["agent"]], marker=MODEL_MARKER[c["model"]],
                   edgecolors="white" if on else "none", linewidths=0.8, zorder=3 if on else 2)
        if on:
            ax.annotate(c["name"], (c["cost"], c["p1"]), textcoords="offset points", xytext=(6, 4), fontsize=7.5)
    steps = sorted((c["cost"], c["p1"]) for c in rows if c["name"] in front)
    ax.step([x for x, _ in steps], [y for _, y in steps], where="post", color="#888", linewidth=1, zorder=1)
    ax.set_xscale("log")
    ax.set_xlabel("cost per trial, USD (log)")
    ax.set_ylabel("Pass@1, %")
    ax.set_title("Copperline and upgrade-v2, 139 tasks, 39 configs. Large marks are on the cost frontier.")
    ax.grid(True, which="both", alpha=0.25)
    handles = [Line2D([], [], marker="o", linestyle="", color=v, label=k) for k, v in HARNESS_COLOR.items()]
    handles += [Line2D([], [], marker=v, linestyle="", color="#444", label=k) for k, v in MODEL_MARKER.items()]
    ax.legend(handles=handles, fontsize=7.5, loc="lower right", ncol=2, framealpha=0.9)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def tasks_table(d: dict) -> list[str]:
    rows = sorted(d["COMBINED_TASKS"], key=lambda r: (r[2], r[1]))
    out = ["| task | world | min | mean | max | title |", "|---|---|--:|--:|--:|---|"]
    for tid, lo, mean, hi, title, world in rows:
        title = "" if title == tid else title
        out.append(f"| task-{world}-{tid} | {world} | {lo}% | {mean}% | {hi}% | {title} |")
    return out


def main() -> None:
    d = json.loads(DATA.read_text())
    views = {key: [adjust(c) for c in d[cfg]] for key, cfg, _ in VIEWS}
    combined, uv2 = views["combined"], views["upgrade-v2"]
    chart(combined, ROOT / "RESULTS.png")

    total_trials = d["COMBINED_TOTALS"]["trials"]
    total_cost = sum(c["total"] for c in combined)
    ranked = sorted(combined, key=lambda c: -c["p1"])
    best, worst = ranked[0], ranked[-1]
    best_haiku = max(c["p1"] for c in combined if c["model"] == "claude-haiku-4-5")
    opus = [c["p1"] for c in combined if c["model"] == "claude-opus-5"]
    front = frontier(combined)
    front_by_agent = {a: sorted(c["name"] for c in combined if c["name"] in front and c["agent"] == a) for a in HARNESS_COLOR}
    cc, cx = matched_stats(combined, "claude-code"), matched_stats(combined, "codex")
    ucc, ucx = matched_stats(uv2, "claude-code"), matched_stats(uv2, "codex")
    n_sweeps = sum(1 for line in sweeps(combined)[2:])
    up_sweeps = sum(1 for line in sweeps(combined)[2:] if float(line.split("|")[-3]) > 0)

    md = [
        "# Results",
        "",
        "One sweep, run in September 2026: every config against every task in the",
        "copperline and upgrade-v2 worlds.",
        "",
        "- 139 tasks: 77 copperline, 62 upgrade-v2",
        "- 39 configs across three harnesses (otto, claude-code, codex) and six models",
        f"- {total_trials:,} scored trials, three per task and config, five for the four configs marked k=5",
        f"- about ${total_cost:,.0f} of model spend",
        f"- best config: {best['name']} at {pct(best['p1'])} Pass@1; lowest: {worst['name']} at {pct(worst['p1'])}",
        "",
        "## What the sweep shows",
        "",
        "**The frontier models handle most of this work.** The best configs pass",
        f"three in four tasks on the two worlds together, and {pct(ranked[0]['p1'])} at the top.",
        f"Model choice is the largest lever: scores span {ranked[-1]['p1']:.0f} to {ranked[0]['p1']:.0f} points",
        f"across the sweep, and the best Haiku config sits {best['p1'] - best_haiku:.0f} points below the best Opus one.",
        f"Within Opus 5, across two harnesses and five thinking levels, the spread is {max(opus) - min(opus):.1f} points.",
        "",
        "**The harness sets what a model costs, and part of what it scores.** Holding",
        "model and thinking level fixed and changing only the harness, otto scores",
        f"above claude-code on all {cc['n']} shared tiers, by {cc['min']:.1f} to {cc['max']:.1f} points of Pass@1.",
        f"claude-code costs more per trial on {cc['dearer']} of those {cc['n']} tiers, by {cc['dearer_min']:.0f}% to",
        f"{(cc['cost_ratio_max'] - 1) * 100:.0f}%; on Sonnet 5 at high thinking it costs {(1 - cc['cost_ratio_min']) * 100:.0f}% less.",
        f"Against codex on the GPT models, otto scores above on {cx['wins']} of {cx['n']} tiers, by up to",
        f"{cx['max']:.1f} points, and loses two Terra tiers by {abs(cx['min']):.1f} points or less. codex is the",
        f"cheaper harness there: {(1 - cx['cost_ratio_max']) * 100:.0f}% to {(1 - cx['cost_ratio_min']) * 100:.0f}% less per trial.",
        f"On Pass@k, tasks solved at least once, otto leads on all {cc['n'] + cx['n']} tiers.",
        "",
        "**Thinking buys accuracy, and cost rises faster than the score.** Of the",
        f"{n_sweeps} harness-and-model pairs run at more than one thinking level, {up_sweeps} end",
        "higher than they start. The cost of the highest level is two to three times",
        "the lowest on most of them. otto on Opus 5 pays 2.4 times more from minimal",
        "to high thinking for three points of Pass@1.",
        "",
        "**On upgrade work the harness gap is widest.** On upgrade-v2, where the task",
        "is knowing what changed between Airflow releases, otto scores above both",
        f"other harnesses on all {ucc['n'] + ucx['n']} shared tiers, by {min(ucc['min'], ucx['min']):.1f} to {max(ucc['max'], ucx['max']):.1f} points.",
        "Cheap otto configs beat expensive rivals outright there, not only at matched",
        "price. otto's harness carries Airflow migration knowledge the model was not",
        "trained on, and more reasoning does not substitute for it.",
        "",
        "## How to read the numbers",
        "",
        "A config is one harness, one model, one thinking level. Each config ran every",
        "task k times. **Pass@1** is the mean fraction of the k trials that passed, and",
        "is the headline. **Pass@k** is the share of tasks solved at least once. **Pass^k**",
        "is the share solved every time, and is the stability floor. Cost is dollars of",
        "model spend per trial at the gateway's rates. Median wall is the median trial",
        "duration in seconds. Pass@1 gaps of about two points are within the movement",
        "a config shows against itself between runs.",
        "",
        "A cost marked with `*` has been repriced. The harness priced cache writes at $0",
        "on the gpt-5.6 models, but OpenAI bills those tokens as ordinary input, so the",
        "otto configs on GPT models are corrected by cache-write volume times the input",
        "rate. codex and the Claude models were not affected.",
        "",
        "The harness that ran as `otto-dev` in the raw data is the current build of otto,",
        "Astronomer's data-engineering agent, and is labelled `otto` here. Its adapter is",
        "not part of this repo. The results were measured inside Astronomer.",
        "",
        "## Cost against accuracy",
        "",
        "![Cost per trial against Pass@1](RESULTS.png)",
        "",
        "The frontier is the set of configs that nothing else beats on both cost and",
        f"pass rate at once. On the two worlds together it holds {len(front)} configs:",
        f"{len(front_by_agent['otto'])} otto, {len(front_by_agent['codex'])} codex, {len(front_by_agent['claude-code'])} claude-code.",
        "otto holds every seat on Opus 5 and Sonnet 5. Below about 55% Pass@1 the",
        "frontier is mostly codex on the smaller GPT models, Terra and Luna, which get",
        "there by giving up accuracy.",
        "",
        "### Same accuracy, different price",
        "",
        "For every claude-code and codex config at 53% Pass@1 or higher, the cheapest",
        "otto config within two points of it.",
        "",
    ] + matched_price(combined) + [
        "",
        "## Harness at a fixed model and thinking level",
        "",
        "Both worlds together. Same tasks, same trials, same grading; only the harness",
        "changes.",
        "",
        "### otto against claude-code",
        "",
    ] + matched_tiers(combined, "claude-code") + [
        "",
        "### otto against codex",
        "",
    ] + matched_tiers(combined, "codex") + [
        "",
        "## Thinking level",
        "",
        "Each harness-and-model pair from its lowest thinking level to its highest,",
        "both worlds together.",
        "",
    ] + sweeps(combined) + [
        "",
        "## Upgrade-v2 at a fixed model and thinking level",
        "",
        "The upgrade world alone. Every shared tier goes to otto.",
        "",
        "### otto against claude-code",
        "",
    ] + matched_tiers(uv2, "claude-code") + [
        "",
        "### otto against codex",
        "",
    ] + matched_tiers(uv2, "codex") + [
        "",
        "## Standings",
        "",
    ]
    for key, cfg, heading in VIEWS:
        md += [f"### {heading}", ""] + standings(views[key]) + [""]
    md += [
        "Regenerate this page with `python tools/render_results.py`; the data is",
        "`results/sweep-2026-09.json`.",
    ]
    (ROOT / "RESULTS.md").write_text("\n".join(md) + "\n")
    print(f"wrote RESULTS.md and RESULTS.png ({len(combined)} configs, ${total_cost:,.0f})")


if __name__ == "__main__":
    main()
