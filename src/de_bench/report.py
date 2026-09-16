"""Turn a stored run into a price-performance view: cost against pass rate.

Joins what `run` recorded (cost, tokens, wall) with what `score` decided (pass,
tier detail) and aggregates per config. The headline is the Pareto frontier —
the configs no other config beats on both axes at once. Everything else exists
to explain a frontier point: where trials die, which tasks nobody solves.

Emits markdown plus a PNG of the chart beside it, so the numbers stay readable
anywhere text goes and the plot can be pasted into a doc or a Slack thread.

Trials classed `infra_failed` are excluded throughout and counted separately;
folding an expired token into a capability number is how a run starts lying.
"""

from __future__ import annotations

import collections
import json
import statistics
from dataclasses import dataclass, field
from pathlib import Path

# Purple and amber on a warm
# surface with its near-black navy ink.
#
# Agent identity is carried twice, a hue and a marker shape assigned together,
# because a scatter compares every pair of colours at once rather than only
# neighbours in a legend. Checked with the palette validator rather than by eye,
# all pairs: worst separation ΔE 14.2 under protanopia and 19.9 for normal
# vision, against floors of 8 and 15. The set this replaced failed that second
# one outright — its pink and orange sat at 12.9, hard to tell apart with full
# colour vision, and dropped to 6.1 under deutan.
#
# Amber is the one brand value not used as published: #ffb32d sits at lightness
# 0.819, above the 0.77 band a mark needs to hold its weight against the surface,
# so the chart uses a step down from it. Teal and amber fall under 3:1 contrast,
# which the direct labels and the table beneath the chart are the relief for.
MARKERS = ["o", "s", "^", "D", "P"]
HUES = ["#5f2fa6", "#3080dd", "#4fbcae", "#d99b18", "#b83a67"]
INK = "#1d1d2c"
MUTED = "#555261"
GRID = "#d0c9c2"
SERIES = "#5f2fa6"
SURFACE = "#f0ece5"

@dataclass
class ConfigStats:
    name: str
    agent: str
    model: str
    thinking: str
    trials: int = 0
    passes: int = 0
    infra: int = 0
    costs: list[float] = field(default_factory=list)
    walls: list[float] = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    tier_fails: dict[str, int] = field(default_factory=dict)
    scored: int = 0  # trials that reached the scorer

    @property
    def pass_rate(self) -> float:
        return self.passes / self.scored if self.scored else 0.0

    @property
    def mean_cost(self) -> float:
        return statistics.fmean(self.costs) if self.costs else 0.0

    @property
    def total_cost(self) -> float:
        return sum(self.costs)

    @property
    def cost_per_pass(self) -> float | None:
        return self.total_cost / self.passes if self.passes else None

    @property
    def median_wall(self) -> float:
        return statistics.median(self.walls) if self.walls else 0.0


@dataclass
class TaskStats:
    task_id: str
    attempts: int = 0
    passes: int = 0

    @property
    def pass_rate(self) -> float:
        return self.passes / self.attempts if self.attempts else 0.0


def load_run(run_dir: Path) -> tuple[list[dict], list[dict], dict]:
    """(results, scores, meta) for a run dir, tolerating an unscored run."""
    results_dir = run_dir if (run_dir / "results.json").exists() else None
    if results_dir is None:
        candidates = [d for d in sorted(run_dir.iterdir()) if (d / "results.json").exists()]
        if len(candidates) != 1:
            raise FileNotFoundError(f"no results.json in {run_dir} or exactly one dir below it")
        results_dir = candidates[0]
    results = json.loads((results_dir / "results.json").read_text())
    scores_path = results_dir / "scores.json"
    scores = json.loads(scores_path.read_text()) if scores_path.exists() else []
    meta_path = results_dir / "meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    return results, scores, meta


def aggregate(results: list[dict], scores: list[dict], meta: dict) -> dict:
    """Per-config and per-task rollups from one run's stored artifacts."""
    verdicts = {(s["config"], s["task_id"], s.get("trial", 0)): s for s in scores}
    declared = {c["name"]: c for c in (meta.get("configs") or [])}

    configs: dict[str, ConfigStats] = {}
    tasks: dict[str, TaskStats] = {}
    for r in results:
        name = r["config"]
        spec = declared.get(name, {})
        cfg = configs.setdefault(
            name,
            ConfigStats(
                name=name,
                agent=r.get("agent", spec.get("agent", "")),
                model=r.get("model", spec.get("model", "")),
                thinking=r.get("thinking", spec.get("thinking", "")),
            ),
        )
        cfg.trials += 1
        if r.get("outcome") == "infra_failed":
            cfg.infra += 1
            continue

        tel = r.get("telemetry") or {}
        cfg.costs.append(float(tel.get("cost_usd") or 0.0))
        cfg.walls.append(float(r.get("wall_seconds") or 0.0))
        cfg.input_tokens += int(tel.get("input_tokens") or 0)
        cfg.output_tokens += int(tel.get("output_tokens") or 0)

        verdict = verdicts.get((name, r["task_id"], r.get("trial", 0)))
        if verdict is None:
            continue
        cfg.scored += 1
        task = tasks.setdefault(r["task_id"], TaskStats(r["task_id"]))
        task.attempts += 1
        if verdict.get("pass"):
            cfg.passes += 1
            task.passes += 1
        else:
            # Blame the first check that failed outright — the stage the agent
            # fell off, not every stage marked not-applicable below it. The
            # scorer stores them in stage order, so first-False is that stage.
            executed = verdict.get("checks") or verdict.get("tiers") or {}
            culprit = next(
                (name for name, r in executed.items() if (r or {}).get("passed") is False),
                verdict.get("score_outcome", "unknown"),
            )
            cfg.tier_fails[culprit] = cfg.tier_fails.get(culprit, 0) + 1

    ranked = sorted(configs.values(), key=lambda c: (-c.pass_rate, c.mean_cost))
    return {
        "configs": ranked,
        "tasks": sorted(tasks.values(), key=lambda t: (t.pass_rate, t.task_id)),
        "frontier": pareto_frontier(ranked),
        "scored_any": any(c.scored for c in ranked),
    }


def _cost(c: ConfigStats) -> float:
    return c.mean_cost


def _wall(c: ConfigStats) -> float:
    return c.median_wall


def pareto_frontier(configs: list[ConfigStats], spend=_cost) -> set[str]:
    """Configs nothing else beats on both axes — cheaper and at least as good.

    `spend` is whatever is being traded against pass rate: money by default, or
    wall-clock, which is a different question with a different answer. A config
    that buys its score with time can be the cheapest and the slowest at once.

    Ties matter: two configs at the same cost and pass rate are both on the
    frontier, so neither silently disappears from the chart.
    """
    frontier = set()
    for c in configs:
        if not c.scored:
            continue
        dominated = any(
            o is not c
            and o.scored
            and spend(o) <= spend(c)
            and o.pass_rate >= c.pass_rate
            and (spend(o) < spend(c) or o.pass_rate > c.pass_rate)
            for o in configs
        )
        if not dominated:
            frontier.add(c.name)
    return frontier


def _money(x: float) -> str:
    return f"${x:.3f}" if x < 1 else f"${x:.2f}"


def _table(headers: list[str], rows: list[list[str]], right: set[int] = frozenset()) -> list[str]:
    """A markdown table padded to read straight, before any renderer touches it."""
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    def line(cells: list[str]) -> str:
        out = []
        for i, cell in enumerate(cells):
            out.append(cell.rjust(widths[i]) if i in right else cell.ljust(widths[i]))
        return "| " + " | ".join(out) + " |"
    rule = "|" + "|".join(("-" * (w + 2)) if i not in right else ("-" * (w + 1) + ":") for i, w in enumerate(widths)) + "|"
    return [line(headers), rule, *(line(r) for r in rows)]


def chart_png(agg: dict, path: Path, axis: str = "cost", title: str | None = None) -> bool:
    """What a config spends against what it gets, with its Pareto staircase.

    `axis` picks what is being spent: "cost" (mean dollars per task) or "wall"
    (median seconds per task). They are separate questions and have separate
    frontiers — a config can be the cheapest way to a score and the slowest.

    `title` names the slice of the corpus being plotted. The whole-corpus chart
    leaves it off — its heading already says so — but two charts that sit side
    by side have to say which is which without the surrounding text.

    Returns False when there is nothing worth plotting — fewer than two scored
    configs is a stat, not a scatter.
    """
    import matplotlib

    matplotlib.use("Agg")  # no display in CI or on Modal
    import matplotlib.pyplot as plt

    pts = [c for c in agg["configs"] if c.scored]
    if len(pts) < 2:
        return False

    spend = _wall if axis == "wall" else _cost
    frontier = pareto_frontier(pts, spend)
    x_label = "median wall-clock per task" if axis == "wall" else "mean cost per task (USD, log scale)"
    tick = (lambda v, _: f"{v:,.0f}s") if axis == "wall" else (lambda v, _: f"${v:,.2f}")

    agents = sorted({c.agent for c in pts})
    markers = {a: MARKERS[i % len(MARKERS)] for i, a in enumerate(agents)}
    hues = {a: HUES[i % len(HUES)] for i, a in enumerate(agents)}

    fig, ax = plt.subplots(figsize=(9, 5.2), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    # The frontier staircase: at any budget, the best pass rate bought so far. A
    # step rather than a line between points, because that is what the frontier
    # claims — the best score reachable at or below a budget, which does not
    # improve until the next config on it.
    front = sorted((c for c in pts if c.name in frontier), key=spend)
    if len(front) > 1:
        xs, ys = [spend(front[0])], [front[0].pass_rate * 100]
        for prev, nxt in zip(front, front[1:]):
            xs += [spend(nxt), spend(nxt)]
            ys += [prev.pass_rate * 100, nxt.pass_rate * 100]
        ax.plot(xs, ys, color=MUTED, linewidth=2, alpha=0.55, solid_joinstyle="round", zorder=1,
                label="best at this budget")

    for agent in agents:
        for on_front in (False, True):
            group = [
                c for c in pts
                if c.agent == agent and (c.name in frontier) == on_front
            ]
            if not group:
                continue
            ax.scatter(
                [spend(c) for c in group],
                [c.pass_rate * 100 for c in group],
                marker=markers[agent],
                s=95 if on_front else 55,
                color=hues[agent],
                alpha=1.0 if on_front else 0.4,
                # A surface-coloured ring keeps overlapping marks legible.
                edgecolors=SURFACE if on_front else "none",
                linewidths=1.6,
                zorder=3 if on_front else 2,
                label=agent if not on_front or agent not in
                {c.agent for c in pts if c.name not in agg["frontier"]} else None,
            )

    # Name the frontier only — a label on every point is noise, and the table
    # under the chart carries the rest. Neighbouring frontier points can sit a
    # couple of points apart, so nudge a label clear rather than let it collide.
    span = max(spend(c) for c in pts) or 1.0
    last_rate, drop = None, 0.0
    for c in sorted(front, key=lambda c: -c.pass_rate):
        rate = c.pass_rate * 100
        drop = drop - 10.0 if last_rate is not None and last_rate - rate < 5.0 else 0.0
        last_rate = rate
        right = spend(c) < span * 0.72
        ax.annotate(
            c.name,
            (spend(c), rate),
            textcoords="offset points",
            xytext=(9 if right else -9, drop),
            ha="left" if right else "right",
            va="center",
            fontsize=9,
            fontweight="bold",
            color=INK,
        )

    if title:
        ax.set_title(title, fontsize=11, fontweight="bold", color=INK, loc="left", pad=12)
    ax.set_xlabel(x_label, fontsize=10, color=MUTED, labelpad=8)
    ax.set_ylabel("pass rate", fontsize=10, color=MUTED, labelpad=8)
    # 0% sits on the axis floor: pass rate has a real zero, and padding below it
    # invents room for scores that cannot exist.
    ax.set_ylim(0, 104)
    if axis == "wall":
        ax.set_xlim(left=0)
    else:
        # Cost spans 25x across the matrix — half the configs sit under a fifth of
        # the dearest one. On a linear axis twelve of twenty-four crowd into the
        # leftmost tenth of the plot and the cheap end is unreadable, which is why
        # every published cost-versus-score chart uses a log axis for price.
        # Wall-clock keeps its linear axis: it spans 10x, and seconds read plainly.
        ax.set_xscale("log")
        lo = min(spend(c) for c in pts)
        hi = max(spend(c) for c in pts)
        ax.set_xlim(lo / 1.6, hi * 1.6)
        ax.xaxis.set_minor_formatter(plt.NullFormatter())
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0f}%")
    ax.xaxis.set_major_formatter(tick)
    ax.grid(axis="y", color=GRID, linewidth=1)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color("#c3c2b7")
    ax.tick_params(colors=MUTED, labelsize=9, length=0)

    legend = ax.legend(
        loc="lower right", frameon=False, fontsize=9, labelcolor=MUTED,
        handletextpad=0.4, borderpad=0.2,
    )
    # The legend is the key to the hues, so its swatches show at full strength
    # even though most points on the plot are faded. Matched by label, not by
    # position: the staircase is drawn before any scatter, so it holds the first
    # legend handle and pairing them off in order would paint it as a harness.
    for handle, text in zip(legend.legend_handles, legend.get_texts()):
        agent = text.get_text()
        if agent in hues:
            handle.set_alpha(1.0)
            handle.set_color(hues[agent])

    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    return True


def render(agg: dict, run_name: str, meta: dict, chart_file: str | None = None,
           time_chart_file: str | None = None) -> str:
    """The whole report as markdown, pointing at the chart if one was drawn."""
    cfgs = agg["configs"]
    scored = sum(c.scored for c in cfgs)
    infra = sum(c.infra for c in cfgs)
    total_cost = sum(c.total_cost for c in cfgs)
    best = max((c for c in cfgs if c.scored), key=lambda c: c.pass_rate, default=None)
    sweep = min((c for c in cfgs if c.scored and c.pass_rate == 1.0), key=lambda c: c.mean_cost, default=None)

    worlds = ", ".join((meta.get("worlds") or {}).keys())
    out = [f"# de-bench — {run_name}", ""]
    context = [f"{len(cfgs)} config(s)", f"{len(agg['tasks'])} task(s)", f"{scored} scored trial(s)"]
    if worlds:
        context.append(f"world {worlds}")
    if meta.get("date"):
        context.append(f"run {meta['date']}")
    out += [" · ".join(context), ""]
    out += [f"- **{_money(total_cost)}** total spend"]
    if best:
        out.append(f"- best pass rate **{best.pass_rate * 100:.0f}%** ({best.name})")
    out.append(
        f"- cheapest config passing everything: **{sweep.name}** at {_money(sweep.mean_cost)} per task"
        if sweep
        else "- no config passed every task"
    )
    if infra:
        out.append(f"- {infra} trial(s) failed on infrastructure, excluded from every number here")
    out.append("")

    out += ["## Cost against pass rate", ""]
    plotted = [c for c in cfgs if c.scored]
    if not agg["scored_any"]:
        out += ["Nothing scored yet — run `de-bench score` on this run to fill in pass rates.", ""]
    elif len(plotted) < 2:
        only = plotted[0]
        out += [
            f"Only `{only.name}` has been measured ({only.passes}/{only.scored} at "
            f"{_money(only.mean_cost)} per task), so there is no frontier to plot. Run a second "
            "config — or the whole matrix with `--matrix configs/matrix.yaml` — and this becomes "
            "the chart.",
            "",
        ]
    elif chart_file:
        out += [
            f"![Mean cost per task against pass rate, by config]({chart_file})",
            "",
            "Colour and shape both carry the harness, so neither has to be read alone. "
            "Solid marks and the step line are the Pareto frontier: nothing else is both "
            "cheaper and better. Faded marks are configs something else already beats on "
            "both counts.",
            "",
        ]
        if time_chart_file:
            out += [
                "## Wall-clock against pass rate",
                "",
                f"![Median wall-clock per task against pass rate, by config]({time_chart_file})",
                "",
                "The same trials priced in time rather than money, with its own frontier — "
                "the cheapest way to a score and the quickest are not the same config.",
                "",
            ]

    out += ["## Configs", ""]
    rows = [
        [
            c.name,
            c.model,
            c.thinking,
            f"{c.passes}/{c.scored}",
            f"{c.pass_rate * 100:.0f}%" if c.scored else "—",
            _money(c.mean_cost),
            _money(c.cost_per_pass) if c.cost_per_pass is not None else "—",
            f"{c.median_wall:.0f}s",
            "yes" if c.name in agg["frontier"] else "",
        ]
        for c in cfgs
    ]
    out += _table(
        ["config", "model", "thinking", "pass", "rate", "cost/task", "cost/pass", "median wall", "frontier"],
        rows,
        right={3, 4, 5, 6, 7},
    )
    out.append("")

    fails: dict[str, int] = {}
    for c in cfgs:
        for tier, n in c.tier_fails.items():
            fails[tier] = fails.get(tier, 0) + n
    if fails:
        out += ["## Where trials fail", ""]
        out += _table(
            ["first failing tier", "trials"],
            [[tier, str(n)] for tier, n in sorted(fails.items(), key=lambda kv: -kv[1])],
            right={1},
        )
        out.append("")

    if agg["tasks"]:
        out += ["## Tasks by pass rate", ""]
        out += _table(
            ["task", "passed", "rate"],
            [[t.task_id, f"{t.passes}/{t.attempts}", f"{t.pass_rate * 100:.0f}%"] for t in agg["tasks"]],
            right={1, 2},
        )
        out.append("")

    return "\n".join(out)


def collect(runs_dir: Path, current_worlds: dict[str, str],
            current_workspaces: dict[str, str] | None = None,
            task_worlds: dict[str, str] | None = None,
            published: set | None = None) -> tuple[list[dict], list[dict], dict, dict]:
    """Fold every stored run into one picture: the newest scored result per cell.

    A cell is (agent, model, thinking, task, trial, world) — what the trial cache
    keys on, and what actually identifies a sample. The world belongs in it
    because one task runs in several: a world variant is the same tickets in a
    bigger estate, so the same agent on the same task at two sizes is two
    samples, and keying without the world would let the newer size quietly
    overwrite the older one. The config name is only a label:
    an ad-hoc `--agents pi --model claude-sonnet-5` run calls itself "pi", which
    is the same cell as the matrix's "pi-sonnet" and a different cell from a
    "pi" run on haiku. Keying by name would double-count the first and merge the
    second. Each identity is labelled with the name its newest run gave it.

    Runs are read oldest first so a later one overwrites an earlier one —
    re-running two configs updates just those, instead of a small run replacing
    a whole matrix.

    Only scored cells count: a snapshot is about cost against pass rate, and an
    unscored trial has no pass rate to contribute. Cells produced against a
    world whose content has since changed are dropped, not silently mixed in.
    """
    runs = []
    for d in sorted(runs_dir.iterdir()) if runs_dir.is_dir() else []:
        if not d.is_dir():
            continue
        try:
            results, scores, meta = load_run(d)
        except (FileNotFoundError, json.JSONDecodeError, NotADirectoryError):
            continue
        runs.append((meta.get("date") or d.name, d.name, results, scores, meta))
    runs.sort(key=lambda r: (r[0], r[1]))

    merged_results: dict[tuple, dict] = {}
    merged_scores: dict[tuple, dict] = {}
    configs: dict[str, dict] = {}
    provenance: dict[str, int] = {}
    stale: dict[str, int] = {}
    worlds: dict[str, str] = {}
    labels: dict[tuple, str] = {}
    unscored = 0
    current_workspaces = current_workspaces or {}
    changed_tasks: dict[str, int] = collections.Counter()
    stale_worlds: dict[tuple[str, str], int] = collections.Counter()
    # Tasks held out of the published view — status "saturated", retired for
    # being passed by nearly every config. Their rows stay in the stored runs
    # and come back the moment the status flips; the snapshot only counts them.
    held_out: dict[str, int] = collections.Counter()
    # Which sizes of each world contributed rows. One size is the ordinary case;
    # two means the folded table is averaging a task's small-world result with
    # its big-world one, which is a comparison, not a rate.
    sizes_seen: dict[str | None, set[str | None]] = {}

    for _, name, results, scores, meta in runs:
        drifted = {
            w for w, sha in (meta.get("worlds") or {}).items()
            if w in current_worlds and current_worlds[w] != sha
        }
        # Drop the drifted world's rows, not the whole run. A matrix commonly
        # covers two worlds — one early run held 1584 trials across two —
        # and retiring both because one moved throws away results that are still
        # exactly as valid as the day they were sampled. Same principle the task
        # check below already follows.
        #
        # Without a task -> world map there is no way to tell the rows apart, so
        # the old whole-run drop stands.
        if drifted and not task_worlds:
            stale[name] = len(results)
            continue
        # A task that ships its own project (the upgrade world) can change under a
        # run without its world hash moving, so those are checked per task and the
        # task is dropped rather than the run — one repinned requirement should not
        # retire a whole matrix.
        moved = {
            task for task, sha in (meta.get("task_workspaces") or {}).items()
            if task in current_workspaces and current_workspaces[task] != sha
        }
        # Which world each task ran in for this run. A task binds to one world by
        # name, but a run may have rebound it to a variant of that world, and only
        # the run knows. Without this the rows would be filed under the size the
        # task binds to rather than the size it ran at.
        world_as = meta.get("world_as") or {}
        world_as_root = {variant: root for root, variant in world_as.items()}
        verdicts = {(s["config"], s["task_id"], s.get("trial", 0)): s for s in scores}
        used = 0
        for r in results:
            if published is not None and r["task_id"] not in published:
                held_out[r["task_id"]] += 1
                continue
            world = (task_worlds or {}).get(r["task_id"])
            world = world_as.get(world, world)
            if world in drifted:
                stale_worlds[(name, world)] += 1
                continue
            if r["task_id"] in moved:
                changed_tasks[r["task_id"]] += 1
                continue
            by_name = (r["config"], r["task_id"], r.get("trial", 0))
            identity = (r.get("agent", ""), r.get("model", ""), r.get("thinking", ""))
            key = (*identity, r["task_id"], r.get("trial", 0), world or "")
            sizes_seen.setdefault(world_as_root.get(world, world), set()).add(world)
            verdict = verdicts.get(by_name)
            if verdict is None and r.get("outcome") != "infra_failed":
                unscored += 1
                continue
            merged_results[key] = r
            if verdict is not None:
                merged_scores[key] = {**verdict, "config": r["config"]}
            labels[identity] = r["config"]  # newest run names the column
            used += 1
        if used:
            provenance[name] = used
            worlds.update(meta.get("worlds") or {})
        for c in meta.get("configs") or []:
            configs.setdefault(c["name"], c)

    # Relabel every row with its identity's canonical name, so two runs that
    # spelled the same cell differently land in one column.
    rows, verdict_rows = [], []
    for key, row in merged_results.items():
        name = labels.get(key[:3], row["config"])
        rows.append({**row, "config": name})
        verdict = merged_scores.get(key)
        if verdict is not None:
            verdict_rows.append({**verdict, "config": name})

    meta = {"configs": list(configs.values()), "worlds": worlds}
    info = {"runs": provenance, "stale": stale, "unscored": unscored,
            "changed_tasks": dict(changed_tasks),
            "held_out": dict(held_out),
            "stale_worlds": {f"{run}/{world}": n for (run, world), n in stale_worlds.items()},
            "mixed_sizes": {str(root): sorted(str(w) for w in sizes)
                            for root, sizes in sizes_seen.items() if len(sizes) > 1}}
    return rows, verdict_rows, meta, info


def build(run_dir: Path, out: Path,
          task_worlds: dict[str, str] | None = None) -> tuple[str, Path | None, dict]:
    """Write the chart beside `out`; return (markdown, chart path, aggregate)."""
    results, scores, meta = load_run(run_dir)
    agg = aggregate(results, scores, meta)
    png = out.with_suffix(".png")
    time_png = out.with_name(out.stem + "-time.png")
    drawn = chart_png(agg, png)
    drawn_time = chart_png(agg, time_png, axis="wall")
    markdown = render(agg, run_dir.name, meta, png.name if drawn else None,
                      time_png.name if drawn_time else None)
    return markdown, (png if drawn else None), agg


def snapshot(runs_dir: Path, out: Path, current_worlds: dict[str, str],
             current_workspaces: dict[str, str] | None = None,
             task_worlds: dict[str, str] | None = None,
             published: set | None = None) -> tuple[str, Path | None, dict, dict]:
    """The committed view: every run folded together, newest result per cell."""
    results, scores, meta, info = collect(runs_dir, current_worlds, current_workspaces, task_worlds,
                                          published=published)
    agg = aggregate(results, scores, meta)
    png = out.with_suffix(".png")
    time_png = out.with_name(out.stem + "-time.png")
    drawn = chart_png(agg, png)
    drawn_time = chart_png(agg, time_png, axis="wall")
    markdown = render(agg, "results", meta, png.name if drawn else None,
                      time_png.name if drawn_time else None)

    lines = ["---", ""]
    if info["runs"]:
        contributors = ", ".join(f"`{name}` ({n})" for name, n in sorted(info["runs"].items()))
        lines.append(
            f"Built from {len(info['runs'])} run(s), newest result winning per "
            f"(config, task, trial): {contributors}."
        )
    else:
        lines.append("No scored runs found under `runs/`.")
    if info["unscored"]:
        lines.append(f"{info['unscored']} trial(s) skipped — not scored yet.")
    if info["stale"]:
        names = ", ".join(f"`{n}`" for n in sorted(info["stale"]))
        lines.append(
            f"{len(info['stale'])} run(s) skipped because their world has changed since: {names}. "
            "Re-run and re-score them to fold them back in."
        )
    if info.get("stale_worlds"):
        names = ", ".join(f"`{k}` ({n})" for k, n in sorted(info["stale_worlds"].items()))
        lines.append(
            f"{len(info['stale_worlds'])} run/world pair(s) dropped because that world has "
            f"changed since: {names}. The same runs' other worlds still count. "
            "Re-run and re-score to fold them back in."
        )
    if info.get("changed_tasks"):
        names = ", ".join(f"`{t}` ({n})" for t, n in sorted(info["changed_tasks"].items()))
        lines.append(
            f"{len(info['changed_tasks'])} task(s) dropped because their own workspace has "
            f"changed since the run: {names}. Re-run those tasks to fold them back in."
        )
    if info.get("held_out"):
        lines.append(
            f"{len(info['held_out'])} task(s) held out as saturated — passed by nearly every "
            "config, so they price nothing. Their trials stay stored; flip a task's status "
            "back to `ready` and it returns with its history."
        )
    lines += ["", "Regenerate with `de-bench snapshot`.", ""]
    return markdown + "\n".join(lines), (png if drawn else None), agg, info
