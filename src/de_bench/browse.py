"""A local, offline view of every stored run — one HTML file, plus CSVs.

This is the diagnostic view, and it is deliberately not `snapshot`. A snapshot is
the published picture: newest result per cell, stale rows dropped, one number per
config. This shows everything ever sampled and marks what is stale, so it answers
the awkward questions — is this task broken or is the agent bad, what does nobody
solve, which half of that run still counts, what did the harness spoil.

    uv run de-bench browse                  # writes browse.html
    uv run de-bench browse --csv            # …and tasks.csv / trials.csv

Runs live outside any checkout (see `tasks.runs_root`), so this shows the same
picture from the repo, a worktree, or anywhere else.

The page embeds its data and renders client-side, so filtering and drilling into
a task cost nothing and work with no network and no server.
"""

from __future__ import annotations

import csv
import html
import json
from collections import Counter, defaultdict
from pathlib import Path

from de_bench.report import load_run


def _cost(rec: dict) -> float:
    t = rec.get("telemetry") or {}
    return float(t.get("cost_usd") or t.get("cost") or 0.0)


def _phase(result: dict, verdict: dict | None) -> str:
    """The one phase a trial is in. These are different things, and a rollup that
    conflates them tells the wrong story: an infra failure is not a trial waiting
    to be graded, it is one deliberately kept out of the capability numbers."""
    if result.get("outcome") == "infra_failed":
        return "infra_failed"
    if verdict is None:
        return "unscored"
    if verdict.get("score_outcome") == "apply_failed":
        return "apply_failed"
    return "pass" if verdict.get("pass") else "fail"


def gather(runs_dir: Path, task_worlds: dict[str, str],
           current_worlds: dict[str, str]) -> dict:
    trials: list[dict] = []
    runs: list[dict] = []

    for d in sorted(runs_dir.iterdir()) if runs_dir.is_dir() else []:
        if not d.is_dir():
            continue
        try:
            results, scores, meta = load_run(d)
        except Exception:  # noqa: BLE001 - a half-written run must not stop the view
            continue
        verdicts = {(s["config"], s["task_id"], s.get("trial", 0)): s for s in scores}
        drifted = {w for w, sha in (meta.get("worlds") or {}).items()
                   if w in current_worlds and current_worlds[w] != sha}
        # A run may have rebound its tasks to a variant of the world they name —
        # the same tickets at a bigger size. Only the run knows, so without this
        # every trial is filed under the size the task binds to rather than the
        # size it ran at, and the two sizes are indistinguishable on the page.
        world_as = meta.get("world_as") or {}
        worlds_here: Counter = Counter()
        stale_n = 0
        for r in results:
            world = task_worlds.get(r["task_id"], "?")
            world = world_as.get(world, world)
            v = verdicts.get((r["config"], r["task_id"], r.get("trial", 0)))
            phase = _phase(r, v)
            stale = world in drifted
            stale_n += stale
            worlds_here[world] += 1
            failed = []
            if v:
                for name, c in (v.get("checks") or {}).items():
                    if isinstance(c, dict) and c.get("passed") is False:
                        failed.append({"check": name, "detail": str(c.get("detail", ""))[:300]})
            trials.append({
                "run": d.name, "task": r["task_id"], "world": world,
                "config": r["config"], "trial": r.get("trial", 0),
                "agent": r.get("agent", ""), "model": r.get("model", ""),
                "thinking": r.get("thinking", ""),
                "phase": phase,
                "passed": None if phase not in ("pass", "fail") else phase == "pass",
                "blocked_by": failed[0]["check"].split(":")[0] if failed else "",
                "failed": failed,
                "reasons": ", ".join(r.get("outcome_reasons") or []),
                "status": r.get("status", ""),
                "capped": r.get("status") == "wall_cap",
                "cached": bool(r.get("cached")),
                "wall": round(float(r.get("wall_seconds") or 0), 1),
                "cost": round(_cost(r), 4),
                "stale": stale,
                "trace": str((d / r["config"] / r["task_id"] / f"t{r.get('trial', 0)}").resolve()),
            })
        runs.append({
            "run": d.name, "trials": len(results),
            "scored": len(scores), "stale_trials": stale_n,
            "worlds": ", ".join(f"{w}{' (stale)' if w in drifted else ''}"
                                for w in sorted(worlds_here)),
            "cost": round(sum(_cost(r) for r in results), 2),
        })
    return {"trials": trials, "runs": runs}


def summarise(trials: list[dict], catalogue: list[dict]) -> dict:
    """Rollups, starting from the task catalogue rather than the trials — a task
    nobody ran has to show up as a row saying so, and a trials-first rollup
    cannot say it at all."""
    by_task: dict[str, list[dict]] = defaultdict(list)
    by_config: dict[str, list[dict]] = defaultdict(list)
    for t in trials:
        by_task[t["task"]].append(t)
        if t["passed"] is not None:
            by_config[t["config"]].append(t)

    tasks = []
    for meta in catalogue:
        ts = by_task.get(meta["id"], [])
        scored = [t for t in ts if t["passed"] is not None]
        passes = sum(t["passed"] for t in scored)
        ph = Counter(t["phase"] for t in ts)
        blocks = Counter(t["blocked_by"] for t in scored if not t["passed"] and t["blocked_by"])
        # Keyed by (config, world): one config passing in the small world and
        # failing in the medium one is a size effect, which is the thing being
        # measured, not the flakiness this column exists to surface.
        per_config = defaultdict(set)
        for t in scored:
            per_config[(t["config"], t["world"])].add(t["passed"])
        newest = sorted(ts, key=lambda t: (t["run"], t["trial"]))[-1] if ts else None
        tasks.append({
            "task": meta["id"], "title": meta["title"], "world": meta["world"],
            "difficulty": meta["difficulty"], "status": meta["status"],
            "ran": "yes" if ts else "NO",
            "trials": len(ts), "scored": len(scored), "passes": passes,
            "rate": round(100 * passes / len(scored)) if scored else "",
            "unscored": ph["unscored"], "infra": ph["infra_failed"],
            "apply_failed": ph["apply_failed"],
            "capped": sum(1 for t in ts if t["capped"]),
            "live": sum(1 for t in scored if not t["stale"]),
            "last_run": newest["run"] if newest else "",
            "blocked_by": ", ".join(f"{k} x{v}" for k, v in blocks.most_common(3)),
            "flips": ", ".join(sorted(f"{c} ({w})" for (c, w), v in per_config.items() if len(v) > 1)),
            "cost": round(sum(t["cost"] for t in ts), 2),
            "trace": newest["trace"] if newest else "",
        })

    configs = []
    for config, ts in sorted(by_config.items()):
        passes = sum(t["passed"] for t in ts)
        walls = sorted(t["wall"] for t in ts)
        configs.append({
            "config": config, "agent": ts[0]["agent"], "model": ts[0]["model"],
            "thinking": ts[0]["thinking"], "trials": len(ts), "passes": passes,
            "rate": round(100 * passes / len(ts)) if ts else 0,
            "cost": round(sum(t["cost"] for t in ts), 2),
            "cost_per_trial": round(sum(t["cost"] for t in ts) / len(ts), 4) if ts else 0,
            "median_wall": walls[len(walls) // 2] if walls else 0,
        })

    blocks = Counter(t["blocked_by"] for t in trials if t["passed"] is False and t["blocked_by"])
    return {
        "tasks": tasks, "configs": configs,
        "never_run": [t for t in tasks if t["trials"] == 0 and t["status"] == "ready"],
        "drafts": [t for t in tasks if t["status"] != "ready"],
        "never_passed": [t for t in tasks if t["scored"] and t["passes"] == 0],
        "unscored": [t for t in tasks if t["unscored"]],
        "infra": [t for t in tasks if t["infra"]],
        "flipping": [t for t in tasks if t["flips"]],
        "blocks": blocks.most_common(),
        "phases": Counter(t["phase"] for t in trials).most_common(),
    }


_CSS = """
:root{--bg:#fff;--fg:#111;--mut:#666;--line:#e3e3e3;--sel:#eef3fb;--bad:#c0392b;--ok:#177245;--warn:#9a6700}
@media(prefers-color-scheme:dark){:root{--bg:#14161a;--fg:#e8e8e8;--mut:#9aa0a6;--line:#2a2e35;--sel:#1c2430;--bad:#ff6b6b;--ok:#4cc38a;--warn:#e0b341}}
*{box-sizing:border-box}
body{margin:0;padding:20px 24px;background:var(--bg);color:var(--fg);
     font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
h1{font-size:19px;margin:0 0 2px} h2{font-size:15px;margin:26px 0 6px}
.sub,.note{color:var(--mut);font-size:12px} .sub{margin:0 0 16px}
.cards{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px}
.card{border:1px solid var(--line);border-radius:8px;padding:8px 12px;min-width:104px}
.card b{display:block;font-size:19px} .card span{color:var(--mut);font-size:11px}
.bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:10px 0}
select,input{padding:5px 7px;border:1px solid var(--line);border-radius:6px;
  background:var(--bg);color:var(--fg);font:inherit;font-size:13px}
input[type=search]{min-width:240px}
.wrap{overflow:auto;max-height:70vh;border:1px solid var(--line);border-radius:8px}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{padding:5px 9px;text-align:left;border-bottom:1px solid var(--line);white-space:nowrap}
th{position:sticky;top:0;background:var(--bg);cursor:pointer;user-select:none;z-index:1}
th:hover{color:var(--mut)} tr:last-child td{border-bottom:0}
tbody tr{cursor:pointer} tbody tr:hover{background:var(--sel)}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
.bad{color:var(--bad)} .ok{color:var(--ok)} .warn{color:var(--warn)}
.pill{display:inline-block;padding:0 6px;border-radius:10px;font-size:11px;border:1px solid var(--line)}
details{margin:6px 0} summary{cursor:pointer;font-size:14px;font-weight:600;margin:14px 0 4px}
#detail{border:1px solid var(--line);border-radius:8px;padding:12px;margin:10px 0;display:none}
#detail h3{margin:0 0 6px;font-size:14px}
code{font-size:12px} a{color:inherit}
"""

_JS = r"""
const $ = s => document.querySelector(s);
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const PHASE_CLASS = {pass:'ok', fail:'bad', infra_failed:'warn', apply_failed:'warn', unscored:'mut'};

function uniq(rows, key){ return [...new Set(rows.map(r => r[key]).filter(Boolean))].sort(); }

function fill(sel, values, label){
  sel.innerHTML = `<option value="">${label}</option>` +
    values.map(v => `<option>${esc(v)}</option>`).join('');
}

function table(el, cols, rows, opts = {}){
  const num = new Set(opts.numeric || []);
  el.innerHTML = `<div class="wrap"><table><thead><tr>` +
    cols.map(c => `<th class="${num.has(c[0]) ? 'n' : ''}">${esc(c[1])}</th>`).join('') +
    `</tr></thead><tbody>` +
    rows.map(r => `<tr data-k="${esc(r[opts.key] ?? '')}">` + cols.map(c => {
      const v = r[c[0]];
      let cell = esc(v);
      if (c[0] === 'trace' && v) cell = `<a href="file://${esc(v)}" onclick="event.stopPropagation()">open</a>`;
      else if (c[0] === 'ran' && v === 'NO') cell = '<span class="bad">not run</span>';
      else if (c[0] === 'phase') cell = `<span class="pill ${PHASE_CLASS[v] || ''}">${esc(v)}</span>`;
      else if (c[0] === 'rate' && v !== '') cell = `<span class="${v === 0 ? 'bad' : v === 100 ? 'ok' : ''}">${esc(v)}</span>`;
      return `<td class="${num.has(c[0]) ? 'n' : ''}" data-v="${esc(v)}">${cell}</td>`;
    }).join('') + `</tr>`).join('') +
    `</tbody></table></div>`;
  el.querySelectorAll('th').forEach((th, i) => th.onclick = () => {
    const body = el.querySelector('tbody'), rs = [...body.rows];
    const dir = th.dataset.d === '1' ? -1 : 1;
    el.querySelectorAll('th').forEach(h => h.dataset.d = '');
    th.dataset.d = dir === 1 ? '1' : '0';
    rs.sort((a, b) => {
      const x = a.cells[i].dataset.v, y = b.cells[i].dataset.v;
      const nx = parseFloat(x), ny = parseFloat(y);
      return (!isNaN(nx) && !isNaN(ny)) ? (nx - ny) * dir : String(x).localeCompare(String(y)) * dir;
    });
    rs.forEach(r => body.appendChild(r));
  });
  if (opts.onPick) el.querySelectorAll('tbody tr').forEach(tr =>
    tr.onclick = () => opts.onPick(tr.dataset.k));
}

function filtered(){
  const w = $('#fWorld').value, c = $('#fConfig').value, p = $('#fPhase').value,
        d = $('#fDiff').value, q = $('#fSearch').value.toLowerCase();
  let trials = DATA.trials.filter(t =>
    (!w || t.world === w) && (!c || t.config === c) && (!p || t.phase === p));
  const keep = new Set(trials.map(t => t.task));
  let tasks = DATA.tasks.filter(t =>
    (!w || t.world === w) && (!d || t.difficulty === d) &&
    (!(c || p) || keep.has(t.task)) &&
    (!q || (t.task + ' ' + t.title + ' ' + t.world).toLowerCase().includes(q)));
  return {trials, tasks};
}

function draw(){
  const {tasks} = filtered();
  $('#count').textContent = `${tasks.length} of ${DATA.tasks.length} tasks`;
  table($('#tasks'),
    [['task','task'],['title','ticket'],['world','world'],['difficulty','difficulty'],
     ['status','status'],['ran','run?'],['trials','trials'],['scored','scored'],
     ['passes','passes'],['rate','rate %'],['infra','infra'],['unscored','unscored'],
     ['capped','hit cap'],['last_run','last run'],['blocked_by','stopped by'],
     ['cost','spend $'],['trace','trace']],
    tasks,
    {numeric:['trials','scored','passes','rate','infra','unscored','capped','cost'],
     key:'task', onPick:showTask});
}

function showTask(id){
  const t = DATA.tasks.find(x => x.task === id);
  const ts = DATA.trials.filter(x => x.task === id)
                        .sort((a,b) => (a.run+a.config).localeCompare(b.run+b.config));
  const d = $('#detail');
  d.style.display = 'block';
  const fails = ts.flatMap(x => (x.failed || []).map(f => `${x.config}: ${f.check} — ${f.detail}`));
  d.innerHTML = `<h3>${esc(id)} — ${esc(t.title)}</h3>
    <p class="note">${esc(t.world)} · ${esc(t.difficulty)} · ${t.trials} trial(s) ·
      ${t.passes}/${t.scored} passed${t.flips ? ` · <span class="warn">disagreed: ${esc(t.flips)}</span>` : ''}</p>
    <div id="dtrials"></div>
    ${fails.length ? `<p class="note" style="margin-top:8px">Failed checks</p><div class="wrap"><table><tbody>` +
      [...new Set(fails)].map(f => `<tr><td>${esc(f)}</td></tr>`).join('') + `</tbody></table></div>` : ''}`;
  table($('#dtrials'),
    [['run','run'],['config','config'],['trial','#'],['phase','phase'],['reasons','why'],
     ['blocked_by','stopped by'],['wall','wall s'],['cost','$'],['cached','cached'],
     ['stale','stale'],['trace','trace']],
    ts, {numeric:['trial','wall','cost']});
  d.scrollIntoView({behavior:'smooth', block:'nearest'});
}

fill($('#fWorld'), uniq(DATA.tasks, 'world'), 'all worlds');
fill($('#fConfig'), uniq(DATA.trials, 'config'), 'all configs');
fill($('#fPhase'), uniq(DATA.trials, 'phase'), 'all phases');
fill($('#fDiff'), uniq(DATA.tasks, 'difficulty'), 'all difficulties');
['#fWorld','#fConfig','#fPhase','#fDiff','#fSearch'].forEach(s => $(s).oninput = draw);
$('#clear').onclick = () => { ['#fWorld','#fConfig','#fPhase','#fDiff','#fSearch']
  .forEach(s => $(s).value = ''); draw(); };

table($('#configs'),
  [['config','config'],['agent','agent'],['model','model'],['thinking','thinking'],
   ['trials','trials'],['passes','passes'],['rate','rate %'],['cost_per_trial','$/trial'],
   ['median_wall','median s'],['cost','spend $']],
  DATA.configs, {numeric:['trials','passes','rate','cost_per_trial','median_wall','cost']});
table($('#runs'),
  [['run','run'],['worlds','worlds'],['trials','trials'],
   ['scored','scored'],['stale_trials','stale'],['cost','spend $']],
  DATA.runs, {numeric:['trials','scored','stale_trials','cost']});
for (const [id, rows, cols] of DATA.lists)
  table($('#' + id), cols, rows, {key:'task', onPick:showTask});
draw();
"""


def _list_section(sid: str, heading: str, note: str) -> str:
    return (f"<details open><summary>{html.escape(heading)}</summary>"
            f'<p class="note">{note}</p><div id="{sid}"></div></details>')


def render(data: dict, summary: dict, current_worlds: dict[str, str], runs_dir: Path) -> str:
    trials = data["trials"]
    scored = [t for t in trials if t["passed"] is not None]
    cards = [
        ("tasks in repo", len(summary["tasks"])),
        ("drafts", len(summary["drafts"])),
        ("never run", len(summary["never_run"])),
        ("trials stored", len(trials)),
        ("gave a verdict", len(scored)),
        ("never passed", len(summary["never_passed"])),
        ("flipping", len(summary["flipping"])),
        ("spend", f"${sum(t['cost'] for t in trials):,.2f}"),
    ]
    card_html = "".join(f'<div class="card"><b>{v}</b><span>{k}</span></div>' for k, v in cards)
    phase_note = " · ".join(f"{n} {p}" for p, n in summary["phases"])
    read_from = f"<code>{html.escape(str(runs_dir))}</code>"
    worlds = ", ".join(f"{w} <code>{sha[:12]}</code>" for w, sha in sorted(current_worlds.items()))

    tcols = [("task", "task"), ("title", "ticket"), ("world", "world"),
             ("trials", "trials"), ("last_run", "last run"), ("trace", "trace")]
    lists = [
        ("never_run", summary["never_run"], "Never run",
         "In the repo, never sampled. Nothing is known about these."),
        ("infra", summary["infra"], "Spoiled by the harness",
         "Not the agent failing — expired auth, a permission prompt nobody could answer, "
         "a provider error that ended the run. Read the reason before re-running."),
        ("unscored", summary["unscored"], "Run but not scored",
         "Trials exist and a verdict does not. <code>de-bench score &lt;run&gt;</code> "
         "settles them and costs no agent spend."),
        ("never_passed", summary["never_passed"], "Run, scored, never passed",
         "The hardest tasks, or broken ones. A task every config fails on the same check "
         "is worth reading before believing."),
        ("flipping", summary["flipping"], "Same config, different answer",
         "One config both passed and failed this task. Either the agent is unreliable here "
         "or the task is — LF-812 looked exactly like this."),
        ("drafts", summary["drafts"], "Drafts",
         "Authored and deliberately kept out of runs by <code>status: draft</code>."),
    ]
    sections = "".join(_list_section(sid, head, note) for sid, rows, head, note in lists if rows)

    payload = {
        "tasks": summary["tasks"], "trials": trials, "configs": summary["configs"],
        "runs": data["runs"],
        "lists": [[sid, rows, tcols] for sid, rows, _, _ in lists if rows],
    }
    return f"""<!doctype html><meta charset=utf-8><title>de-bench state</title>
<style>{_CSS}</style>
<h1>de-bench — what is in <code>runs/</code></h1>
<p class="sub">Everything ever sampled, stale rows marked rather than dropped. The diagnostic
view; <code>RESULTS.md</code> is the published one. Read from {read_from}.</p>
<div class="cards">{card_html}</div>
<p class="note">Every trial sits in one phase: <b>{html.escape(phase_note)}</b>.
<code>pass</code> and <code>fail</code> are capability samples. <code>infra_failed</code> is one
the harness spoiled and is excluded on purpose, not waiting to be graded.
<code>apply_failed</code> means the stored patch would not apply to a fresh workspace.
<code>unscored</code> is the only one that just needs <code>de-bench score</code>.</p>
{sections}
<h2>Every task</h2>
<div class="bar">
  <select id="fWorld"></select><select id="fConfig"></select><select id="fPhase"></select>
  <select id="fDiff"></select>
  <input type="search" id="fSearch" placeholder="task, ticket, world…">
  <button id="clear">clear</button><span class="note" id="count"></span>
</div>
<p class="note">Click a row for its trials, why each ended where it did, and the failed checks.</p>
<div id="tasks"></div>
<div id="detail"></div>
<h2>Configs</h2><div id="configs"></div>
<h2>Runs</h2>
<p class="note">Current worlds: {worlds}. A run is stale for a world whose content changed
since; the other worlds in that run still count.</p>
<div id="runs"></div>
<script>const DATA = {json.dumps(payload)};</script>
<script>{_JS}</script>"""


def write_csvs(data: dict, summary: dict, out_dir: Path) -> list[Path]:
    written = []
    for name, rows in (("tasks.csv", summary["tasks"]), ("trials.csv", data["trials"])):
        if not rows:
            continue
        flat = [{k: (json.dumps(v) if isinstance(v, (list, dict)) else v) for k, v in r.items()}
                for r in rows]
        path = out_dir / name
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(flat[0]))
            w.writeheader()
            w.writerows(flat)
        written.append(path)
    return written


def build(runs_dir: Path, out: Path, catalogue: list[dict],
          current_worlds: dict[str, str], csv_too: bool = False) -> tuple[Path, list[Path], dict]:
    task_worlds = {t["id"]: t["world"] for t in catalogue}
    data = gather(runs_dir, task_worlds, current_worlds)
    summary = summarise(data["trials"], catalogue)
    out.write_text(render(data, summary, current_worlds, runs_dir), encoding="utf-8")
    csvs = write_csvs(data, summary, out.parent) if csv_too else []
    return out, csvs, summary
