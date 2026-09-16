"""de-bench CLI: run agents on the repo's tasks in Modal and collect what they left behind."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import shutil
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

from de_bench.agents import AGENTS
from de_bench.tasks import (
    Task,
    evidence_sha,
    generated_tar,
    load_tasks,
    load_world,
    oracle_tar,
    repo_root,
    runs_root,
    stray_runs,
    system_prompt,
    verifier_tar,
    workspace_sha,
    workspace_tar,
    world_sha,
)
from de_bench.traces import emit_traces
from de_bench.validity import classify_outcome

# Cheap defaults while the harness itself is under development; pass
# --model/--thinking for real price-performance sweeps.
DEFAULT_MODEL = "claude-haiku-4-5"
DEFAULT_THINKING = "low"


def _cmd_list(args: argparse.Namespace) -> int:
    for t in load_tasks(include_drafts=True):
        seeded = " [history]" if t.history else ""
        draft = " [draft]" if t.status != "ready" else ""
        print(f"{t.id}  {t.world.name:<10} {t.difficulty:<8} {t.max_wall_seconds:>5}s  {t.title}{draft}{seeded}")
    return 0


def _load_matrix(path: str) -> list[dict]:
    """Named (agent, model, thinking) configs from a matrix YAML."""
    import yaml

    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    configs = doc.get("configs") or []
    names = [c["name"] for c in configs]
    if len(names) != len(set(names)):
        raise ValueError(f"duplicate config names in {path}")
    return configs



CACHE_DIR = Path("cache") / "trials"
CACHE_VOLUME = "de-bench-trial-cache"


def _cache_volume():
    """The shared trial cache: a Modal volume as source of truth, with
    cache/trials/ as a local read-through copy. Created on first use."""
    import modal

    return modal.Volume.from_name(CACHE_VOLUME, create_if_missing=True)


def _volume_keys(vol) -> set[str]:
    """Keys present in the shared cache — one listdir for the whole run."""
    try:
        return {e.path.strip("/") for e in vol.listdir("/")}
    except Exception:  # noqa: BLE001 - offline/unauthed falls back to local-only
        return set()


def _volume_download(vol, key: str, attempts: int = 3) -> bool:
    """Transient download failures re-run a trial (agent spend), so retry."""
    for i in range(attempts):
        if _volume_download_once(vol, key):
            return True
        print(f"  (cache download attempt {i + 1} failed for {key})", file=sys.stderr)
    return False


def _volume_download_once(vol, key: str) -> bool:
    dest = CACHE_DIR / key
    tmp = dest.with_suffix(".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    try:
        for entry in vol.listdir(f"/{key}", recursive=True):
            rel = entry.path.split("/", 1)[1]
            target = tmp / rel
            if entry.type.name == "DIRECTORY":
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with open(target, "wb") as f:
                for chunk in vol.read_file(entry.path):
                    f.write(chunk)
    except Exception:  # noqa: BLE001
        shutil.rmtree(tmp, ignore_errors=True)
        return False
    shutil.rmtree(dest, ignore_errors=True)
    tmp.rename(dest)
    return True


def _volume_upload(vol, key: str, trial_dir: Path) -> None:
    try:
        with vol.batch_upload(force=True) as batch:
            batch.put_directory(str(trial_dir), f"/{key}")
    except Exception as exc:  # noqa: BLE001 - sharing is best-effort, the run must not die
        print(f"  (cache upload failed for {key}: {exc})", file=sys.stderr)


#: Bump when the trial container changes in a way that changes what an agent can
#: do — a package added to the image, a harness pinned to a new version, the way
#: the workspace is captured. It is part of every cache key, so a bump retires the
#: cells produced by the old container instead of serving them beside the new ones.
#:
#: 2: procps and friends installed; codex's login shells keep the trial PATH; the
#:    end-state diff taken against the baseline commit rather than HEAD.
#: 3: workspaces ship an Astro project marker, and the CLI's .astro directory is kept
#:    out of the diff.
#: 4: the prompt says per task whether an Airflow is running, and opencode exports its
#:    session from a signal handler so a capped trial keeps its trace.
#: 5: claude-code pinned to 2.1.224. Generation 4 built it unpinned and got 2.1.226,
#:    which loads bundled skills into the prompt — 700KB of one of them ended four
#:    haiku trials at "Prompt is too long".
#: 6: the Astro project marker ships only in the four workspaces that are Astro
#:    projects, not in all 48. Generations 3 to 5 told 44 pip-managed Airflow repos
#:    they were Astro projects, and the agents that knew the convention dropped the
#:    `apache-airflow` pin that 28 of those tasks grade on.
HARNESS_GENERATION = 6


def _trial_key(agent: str, model: str, thinking: str, task, trial: int, sys_prompt: str,
               wall: int | None = None) -> str:
    """Content address of one trial cell. Trials are stochastic samples, so the
    cache never claims determinism — it claims "this exact cell was already
    sampled". Anything that changes the cell (model, thinking, prompt, the
    shipped workspace's content hash, wall cap, trial index) changes the key. Task
    checks are NOT in the key — they shape scoring, not the trial itself.

    HARNESS_GENERATION stands in for the container. Leaving it out meant a fixed
    image and a broken one shared a key, so a re-run silently mixed the two and the
    results said nothing about which was which — that is how codex ran two whole
    matrices without the `af` shim on its PATH. Per-version pinning would be finer
    grained, but the point is to be able to retire a generation on purpose.
    """
    fields = {
        "agent": agent,
        "model": model,
        "thinking": thinking,
        "task": task.id,
        "trial": trial,
        "harness_generation": HARNESS_GENERATION,
        # Key name predates per-task workspaces; the value is the hash of
        # whatever tree ships, which for world-backed tasks is unchanged.
        "world_sha": workspace_sha(task),
        "evidence_sha": evidence_sha(task),
        "prompt": hashlib.sha256(task.prompt.encode()).hexdigest(),
        "system_prompt": hashlib.sha256(sys_prompt.encode()).hexdigest(),
        "wall": wall or task.max_wall_seconds,
    }
    # Only when the world declares them, so every pre-existing key stays put.
    if task.env or task.world.cache_salt or task.setup_sql:
        fields["runtime"] = {
            "env": task.env,
            "salt": task.world.cache_salt,
            "setup_sql": list(task.setup_sql),
        }
    # Same rule: written run history changes what the agent sees, so it is in
    # the key — but only for tasks that declare it. The field keeps its stored
    # name, "seed_runs", because renaming it would move every keyed trial of
    # every task that declares history. Executed pairs keep the [dag_id, ds]
    # serialization they always had, so keys stored before the manifest form
    # existed stay put; a manifest entry is a dict, and the sort_keys dump
    # below gives it one canonical form, so any change to it — a state, a try
    # count, a log path — moves the key.
    if task.history:
        fields["seed_runs"] = [list(e) if isinstance(e, tuple) else e for e in task.history]
    blob = json.dumps(fields, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:24]


def _infra_failed(entry: Path) -> bool:
    """True when a cache entry holds an infra_failed husk (rate-limit death,
    auth failure) rather than a capability sample."""
    try:
        return json.loads((entry / "summary.json").read_text()).get("outcome") == "infra_failed"
    except Exception:  # noqa: BLE001
        return False


def _cache_store(key: str, trial_dir: Path, vol=None) -> None:
    dest = CACHE_DIR / key
    # A husk is replaceable: a rerun that resampled the cell and got a real
    # attempt overwrites the rate-limited corpse it is healing.
    if dest.exists() and _infra_failed(dest) and not _infra_failed(trial_dir):
        shutil.rmtree(dest)
    if not dest.exists():
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(trial_dir, dest)
    if vol is not None:
        _volume_upload(vol, key, dest)


def _cache_fetch(key: str, trial_dir: Path, config: str, vol=None, volume_keys: set[str] | None = None) -> dict | None:
    src = CACHE_DIR / key
    if not (src / "summary.json").exists():
        if vol is None or volume_keys is None or key not in volume_keys or not _volume_download(vol, key):
            return None
    # An infra_failed entry is not a sample of the cell — serving it back
    # would freeze a rate-limit death into every future run. Treat it as a
    # miss so the rerun resamples; the store above replaces the husk.
    if _infra_failed(src):
        return None
    trial_dir.parent.mkdir(parents=True, exist_ok=True)
    if trial_dir.exists():
        shutil.rmtree(trial_dir)
    shutil.copytree(src, trial_dir)
    summary = json.loads((trial_dir / "summary.json").read_text())
    # A cell may be reused under a new config name. Rewrite the copy so the trial
    # directory describes itself — the cache entry keeps its original name, but a
    # run directory that disagrees with its own results.json misleads anyone
    # reading the artifacts directly.
    summary["config"] = config
    summary["cached"] = True
    (trial_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def _runtime_spec_fields(task) -> dict:
    """The world/task runtime extras a Modal container applies at setup.
    Only present when declared, so specs for existing worlds are unchanged."""
    fields = {}
    if task.env:
        fields["env"] = task.env
    if task.workspace_data:
        fields["workspace_data"] = [list(pair) for pair in task.workspace_data]
    if task.setup_sql:
        fields["setup_sql"] = list(task.setup_sql)
    if task.world.preflight_tools:
        fields["preflight_tools"] = list(task.world.preflight_tools)
    if task.history:
        fields["history"] = [list(e) if isinstance(e, tuple) else e for e in task.history]
    if task.world.generated and task.world.generated.get("baked"):
        # The image carries the generated world; the container asserts its
        # bake against this sha instead of running the generator.
        from de_bench.tasks import generated_base_sha

        fields["baked_sha"] = generated_base_sha(task.world.name)
    elif (generated := generated_tar(task)) is not None:
        fields["generated_tar"] = generated
    return fields


def _cmd_run(args: argparse.Namespace) -> int:
    import modal

    from de_bench.modal_app import app, run_trial, run_trial_copperline

    # A run is a list of named configs. --matrix supplies many; otherwise the
    # --agents/--model/--thinking flags define one config per agent, named after it.
    if args.matrix:
        configs = _load_matrix(args.matrix)
    else:
        configs = [
            {"name": agent, "agent": agent, "model": args.model, "thinking": args.thinking}
            for agent in args.agents
        ]
    for c in configs:
        if c["agent"] not in AGENTS:
            print(f"unknown agent {c['agent']!r} — available: {', '.join(AGENTS)}", file=sys.stderr)
            return 2

    tasks = load_tasks()
    if args.world:
        tasks = _bind_world(tasks, args.world)
    if args.tag:
        tasks = [t for t in tasks if args.tag in t.tags]
    if args.tasks:
        wanted = set(args.tasks)
        tasks = [t for t in tasks if t.id in wanted or t.id.split("-")[-1] in wanted]
    if args.limit:
        tasks = tasks[: args.limit]
    if not tasks:
        print("no tasks to run", file=sys.stderr)
        return 1

    # Per task, not per world: the prompt says whether a live Airflow is running, and
    # that is a property of the task. Keyed by world, the last task in each world set
    # the paragraph for every other task in it.
    no_system_prompt = getattr(args, "no_system_prompt", False)
    sys_prompts = {t.id: ("" if no_system_prompt else system_prompt(t)) for t in tasks}
    vol = None if args.no_cache else _cache_volume()
    volume_keys = _volume_keys(vol) if vol is not None else set()
    run_dir = runs_root() / f"{dt.date.today().isoformat()}-{args.name}"
    # One tar per distinct (workspace, evidence) pair (usually one per world).
    tar_cache: dict[tuple, bytes] = {}
    specs, cached_results = [], []
    spec_keys: dict[tuple, str] = {}
    for c in configs:
      for trial in range(args.trials):
        for t in tasks:
            model = AGENTS[c["agent"]].resolve_model(c["model"])
            ckey = _trial_key(c["agent"], model, c["thinking"], t, trial, sys_prompts[t.id], args.wall)
            if not args.no_cache:
                hit = _cache_fetch(ckey, run_dir / c["name"] / t.id / f"t{trial}", c["name"], vol, volume_keys)
                if hit is not None:
                    cached_results.append(hit)
                    continue
            key = (t.workspace_dirs, t.evidence_dir, t.overlay)
            if key not in tar_cache:
                tar_cache[key] = workspace_tar(t)
            spec_keys[(c["name"], t.id, trial)] = ckey
            specs.append(
                (
                    t.image,
                    {
                        "config": c["name"],
                        "trial": trial,
                        "agent": c["agent"],
                        "task_id": t.id,
                        "prompt": t.prompt,
                        "system_prompt": sys_prompts[t.id],
                        # An agent that can't serve the requested model maps it to its
                        # nearest tier (e.g. codex only speaks the gateway's OpenAI
                        # routes); the resolved model is what summary.json records.
                        "model": model,
                        "thinking": c["thinking"],
                        "max_wall_seconds": args.wall or t.max_wall_seconds,
                        "workspace_tar": tar_cache[key],
                        "live_env": t.live_env,
                        "start_airflow": t.start_airflow,
                        **_runtime_spec_fields(t),
                    },
                )
            )

    world_as = {t.world.root_name: t.world.name for t in tasks if t.world.extends}
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "meta.json").write_text(
        json.dumps(
            {
                "date": dt.datetime.now().isoformat(timespec="seconds"),
                "worlds": {name: world_sha(name) for name in sorted({t.world.name for t in tasks})},
                # A run in a world variant binds tasks to a world they do not name.
                # Recorded so `score` rebuilds the same tree, and so a report files
                # the rows under the size they were sampled at. Absent for every
                # ordinary run, which keeps existing meta.json files exactly as they are.
                **({"world_as": world_as} if world_as else {}),
                # Own-workspace tasks (the whole upgrade world) ship their own tree,
                # which no world hash covers. Without these, repinning one task's
                # requirements left every stored row for it in the published file,
                # scored against a project that no longer exists, saying nothing.
                "task_workspaces": {t.id: workspace_sha(t) for t in sorted(tasks, key=lambda t: t.id)},
                "configs": [{k: v for k, v in c.items()} for c in configs],
                "tasks": [t.id for t in tasks],
            },
            indent=2,
        )
    )
    print(f"{len(cached_results)} trial(s) from cache, {len(specs)} to run on Modal")

    results = list(cached_results)
    if not specs:
        _finish_run(run_dir, results)
        return 0
    # One map per image family: a world with its own baked-in data runs on its
    # own container, and the rest keep the image their cached cells were made on.
    runners = {"default": run_trial, "copperline": run_trial_copperline}
    groups: dict[str, list[dict]] = {}
    for image, spec in specs:
        if image not in runners:
            print(f"unknown world image {image!r}", file=sys.stderr)
            return 2
        groups.setdefault(image, []).append(spec)
    # detach: a client disconnect (laptop sleep, network blip) must not make
    # Modal cancel every queued trial — it did once, 426 finished agent runs
    # died unconsumed. Detached, the server keeps working; the client still
    # consumes results and a rerun cache-hits whatever it managed to store.
    remote_failures = 0
    with modal.enable_output():
        with app.run(detach=True):
          for image, group in groups.items():
            # return_exceptions: a single container death (preemption, OOM)
            # otherwise raises through the map and kills the whole client
            # loop — it ended two 2472-cell sweeps at 683 and 965 trials.
            # A failed cell is logged and resampled by the next run; every
            # consumed cell is already banked in the cache.
            for result in runners[image].map(group, order_outputs=False, return_exceptions=True):
                if isinstance(result, Exception):
                    remote_failures += 1
                    print(f"  trial lost to a remote failure ({result}); rerun to resample", file=sys.stderr)
                    continue
                artifacts = result.pop("artifacts")
                trial_dir = run_dir / result["config"] / result["task_id"] / f"t{result['trial']}"
                trial_dir.mkdir(parents=True, exist_ok=True)
                for filename, data in artifacts.items():
                    if data:
                        (trial_dir / filename).write_bytes(data)
                result.update(
                    emit_traces(trial_dir, AGENTS[result["agent"]], artifacts["transcript.jsonl"].decode())
                )
                outcome, reasons = classify_outcome(
                    result["status"],
                    artifacts["transcript.jsonl"].decode(errors="replace"),
                    artifacts["stderr.txt"].decode(errors="replace"),
                    len(artifacts["changes.patch"]),
                    session=artifacts["session.jsonl"].decode(errors="replace"),
                )
                result["outcome"] = outcome
                if reasons:
                    result["outcome_reasons"] = reasons
                (trial_dir / "summary.json").write_text(json.dumps(result, indent=2))
                ckey = None if args.no_cache else spec_keys.get(
                    (result["config"], result["task_id"], result["trial"])
                )
                if ckey:
                    _cache_store(ckey, trial_dir, vol)
                results.append(result)
                # Written every time rather than at the end: a long run that is
                # interrupted — Modal preempts containers — otherwise leaves a
                # directory full of finished trials and no results file to score.
                _write_results(run_dir, results)
                t = result["telemetry"]
                print(
                    f"  {result['task_id']} [{result['config']}] {result['status']} "
                    f"in {result['wall_seconds']}s  "
                    f"tokens {t['input_tokens']}/{t['output_tokens']}  ${t['cost_usd']}"
                )

    if remote_failures:
        print(f"\nWARNING: {remote_failures} trial(s) lost to remote failures — "
              "rerun the same command to resample them (finished cells replay from cache)")
    _finish_run(run_dir, results)
    return 1 if remote_failures else 0


def _cmd_rebuild(args: argparse.Namespace) -> int:
    """Rebuild results.json from the trial directories, which are the real record.

    A run that was interrupted, or whose results file an older build overwrote,
    still has every trial on disk. Identity comes from the path rather than the
    file, because a trial reused from the cache used to keep the config name it
    was first run under.

    `outcome` is recomputed from the stored artifacts rather than copied, so a fix
    to the validity rules reaches runs already on disk. That matters because the
    rules have been wrong: a bare `401` in a ticket number filed 13 finished trials
    as infrastructure failures, and re-running them to clear the label would have
    cost real money for a verdict the artifacts already answer.
    """
    run_dir = Path(args.run_dir)
    rows, relabelled = [], []
    for summary in sorted(run_dir.glob("*/*/t*/summary.json")):
        trial_dir = summary.parent
        row = json.loads(summary.read_text())
        row["config"] = trial_dir.parent.parent.name
        row["task_id"] = trial_dir.parent.name
        row["trial"] = int(trial_dir.name.lstrip("t") or 0)
        was = row.get("outcome")
        now, reasons = _classify_stored(trial_dir, row)
        if now != was:
            relabelled.append((row["config"], row["task_id"], was, now, reasons))
            # The trial dir describes itself; leaving the old outcome there
            # made summary.json disagree with results.json after a rules fix.
            row["outcome"], row["outcome_reasons"] = now, reasons
            summary.write_text(json.dumps(row, indent=2))
        row["outcome"], row["outcome_reasons"] = now, reasons
        rows.append(row)
    if not rows:
        print(f"no trial directories under {run_dir}", file=sys.stderr)
        return 1
    merged = _write_results(run_dir, rows)
    (run_dir / "results.md").write_text(_render_table(merged))
    configs = len({r["config"] for r in merged})
    print(f"rebuilt {run_dir}/results.json from {len(rows)} trial dir(s): {len(merged)} row(s), {configs} config(s)")
    for config, task, was, now, reasons in relabelled:
        print(f"  {config}/{task}: {was} -> {now} {reasons}")
    if relabelled:
        print(f"{len(relabelled)} trial(s) reclassified — re-run `de-bench score` to refresh the verdicts")
    return 0


def _classify_stored(trial_dir: Path, row: dict) -> tuple[str, list[str]]:
    """Re-run the validity rules over one stored trial's artifacts."""
    from de_bench.validity import classify_outcome

    def read(name: str) -> str:
        path = trial_dir / name
        return path.read_text(errors="replace") if path.exists() else ""

    patch = trial_dir / "changes.patch"
    return classify_outcome(
        row.get("status", ""),
        read("transcript.jsonl"),
        read("stderr.txt"),
        patch.stat().st_size if patch.exists() else 0,
        session=read("session.jsonl"),
    )


def _write_results(run_dir: Path, results: list[dict]) -> list[dict]:
    """Merge this run's rows into whatever the directory already holds.

    Re-running one column into an existing run — fix a harness bug, sample that
    agent again — used to replace the file with just those rows, throwing away
    every other config while their trial directories sat right there. Rows are
    keyed by cell, so a re-run replaces its own and leaves the rest alone.
    """
    path = run_dir / "results.json"
    merged: dict[tuple, dict] = {}
    if path.exists():
        try:
            for row in json.loads(path.read_text()):
                merged[(row["config"], row["task_id"], row.get("trial", 0))] = row
        except (json.JSONDecodeError, KeyError, TypeError):
            pass  # unreadable history is not worth losing this run over
    for row in results:
        merged[(row["config"], row["task_id"], row.get("trial", 0))] = row
    out = sorted(merged.values(), key=lambda r: (r["config"], r["task_id"], r.get("trial", 0)))
    path.write_text(json.dumps(out, indent=2))
    return out


def _finish_run(run_dir: Path, results: list[dict]) -> None:
    results = _write_results(run_dir, results)
    infra = [r for r in results if r.get("outcome") == "infra_failed"]
    if infra:
        print(f"\nWARNING: {len(infra)} trial(s) classified infra_failed (excluded from totals):")
        for r in infra:
            print(f"  {r['config']}/{r['task_id']}/t{r.get('trial', 0)}: {', '.join(r.get('outcome_reasons', []))}")
    (run_dir / "results.md").write_text(_render_table(results))
    print(f"\nwrote {run_dir}/results.json and results.md")
    print(_render_table(results))



def _cmd_score(args: argparse.Namespace) -> int:
    """Score a stored run: rebuild each trial's workspace, apply its patch, run the
    checks in Modal. No agent, no model spend — works retroactively on any run."""
    import modal

    from de_bench.modal_app import app, score_trial, score_trial_copperline

    run_dir = Path(args.run_dir)
    results_dir = run_dir if (run_dir / "results.json").exists() else None
    if results_dir is None:
        # Runs made before worlds/ moved in kept results one level down, per suite.
        candidates = [d for d in run_dir.iterdir() if (d / "results.json").exists()]
        if len(candidates) != 1:
            print(f"expected results.json in {run_dir} or exactly one dir below it", file=sys.stderr)
            return 2
        results_dir = candidates[0]

    meta = json.loads((results_dir / "meta.json").read_text()) if (results_dir / "meta.json").exists() else {}
    results = json.loads((results_dir / "results.json").read_text())
    for name, sha in (meta.get("worlds") or {}).items():
        if world_sha(name) != sha:
            print(f"WARNING: world {name!r} differs from the run's recorded hash — "
                  "the workspace or its generated data may differ")

    tasks = {t.id: t for t in load_tasks(include_drafts=True)}
    # Rebind whatever the run rebound, so a patch made against the medium world is
    # replayed against the medium world. Scoring the wrong tree fails checks that
    # never had anything to do with the agent.
    for root, variant in (meta.get("world_as") or {}).items():
        world = load_world(variant)
        tasks = {i: (replace(t, world=world) if t.world.name == root else t) for i, t in tasks.items()}
    if args.tasks:
        wanted = set(args.tasks)
        results = [r for r in results if r["task_id"] in wanted or r["task_id"].split("-")[-1] in wanted]
        if not results:
            print("no matching trials to score", file=sys.stderr)
            return 1
    tar_cache: dict[tuple, bytes] = {}
    specs, rows = [], []
    for r in results:
        task = tasks.get(r["task_id"])
        if task is None:
            continue
        trial = r.get("trial", 0)
        trial_dir = results_dir / r["config"] / r["task_id"] / f"t{trial}"
        if not trial_dir.exists():
            trial_dir = results_dir / r["config"] / r["task_id"]  # pre-trials layout
        patch_file = trial_dir / "changes.patch"
        patch = patch_file.read_bytes() if patch_file.exists() else b""
        key = (task.workspace_dirs, task.evidence_dir, task.overlay)
        if key not in tar_cache:
            tar_cache[key] = workspace_tar(task)
        specs.append(
            (
                task.image,
                {
                    "config": r["config"],
                    "task_id": r["task_id"],
                    "trial": trial,
                    "workspace_tar": tar_cache[key],
                    "patch": patch,
                    "checks": [dict(c) for c in task.checks],
                    "replay_dags": None if task.replay_dags is None else list(task.replay_dags),
                    "do_not_modify": list(task.do_not_modify),
                    "live_env": task.live_env,
                    "verifier_tar": verifier_tar(task),
                    "oracle_tar": oracle_tar(task),
                    **_runtime_spec_fields(task),
                },
            )
        )
        rows.append((r, trial_dir))
    dirs = {(s["config"], s["task_id"], s["trial"]): d for (_, s), (_, d) in zip(specs, rows)}

    scorers = {"default": score_trial, "copperline": score_trial_copperline}
    groups: dict[str, list[dict]] = {}
    for image, spec in specs:
        groups.setdefault(image, []).append(spec)

    rounds = max(1, args.rounds)
    for round_n in range(1, rounds + 1):
        label = f" (round {round_n}/{rounds})" if rounds > 1 else ""
        print(f"scoring {len(specs)} trial(s) from {results_dir}{label}")
        scores = []
        lost = 0
        with modal.enable_output():
            with app.run():
              for image, group in groups.items():
                for score in scorers[image].map(group, order_outputs=False, return_exceptions=True):
                    if isinstance(score, Exception):
                        lost += 1
                        print(f"  scoring lost to a remote failure ({score}); rerun to rescore", file=sys.stderr)
                        continue
                    d = dirs[(score["config"], score["task_id"], score["trial"])]
                    (d / "score.json").write_text(json.dumps(score, indent=2))
                    scores.append(score)
                    mark = "PASS" if score.get("pass") else score.get("score_outcome", "fail")
                    print(f"  {score['config']}/{score['task_id']}/t{score['trial']}: {mark}")

        if lost:
            print(f"WARNING: {lost} trial(s) not scored (remote failures) — rerun to rescore them")

        # Round N of a multi-round score goes straight to scores.rN.json — the
        # manual `de-bench score && mv scores.json scores.rN.json` sequence
        # this replaces, done N times so a partial run never leaves a bare
        # scores.json for the caller to accidentally treat as a finished round.
        scores_path = results_dir / (f"scores.r{round_n}.json" if rounds > 1 else "scores.json")
        md_path = results_dir / (f"scores.r{round_n}.md" if rounds > 1 else "scores.md")
        if args.tasks and scores_path.exists():
            # Partial rescore: keep every existing score not in this run, replace the rest.
            existing = json.loads(scores_path.read_text())
            rescored_keys = {(s["config"], s["task_id"], s["trial"]) for s in scores}
            scores = [s for s in existing if (s["config"], s["task_id"], s["trial"]) not in rescored_keys] + scores

        scores.sort(key=lambda s: (s["config"], s["task_id"], s["trial"]))
        scores_path.write_text(json.dumps(scores, indent=2))
        print(_render_scores(scores))
        md_path.write_text(_render_scores(scores))
        print(f"wrote {scores_path} and {md_path.name}")

    if rounds > 1:
        print(f"\n{rounds} round(s) written as scores.r1.json..scores.r{rounds}.json — "
              f"vote and tiebreak are a separate step, not done here.")
    return 0


def _solution_patch(task) -> bytes:
    """The patch the shipped solution would have delivered: solution/ over the
    workspace, expressed exactly as trials express their work (git diff --binary
    against the extracted tar, so excludes match what the scorer rebuilds)."""
    import io
    import tarfile
    import tempfile

    solution = task.dir / "solution"
    with tempfile.TemporaryDirectory() as td:
        with tarfile.open(fileobj=io.BytesIO(workspace_tar(task)), mode="r:gz") as tar:
            tar.extractall(td)
        run = lambda *argv: subprocess.run(argv, cwd=td, check=True, capture_output=True)  # noqa: E731
        run("git", "init", "-q")
        run("git", "add", "-A")
        run("git", "-c", "user.email=bench@localhost", "-c", "user.name=bench", "commit", "-qm", "baseline")
        # A refactor's solution deletes as well as writes, and an overlay can
        # only write. solution/.deleted lists workspace-relative paths or
        # globs, one per line, removed before the overlay lands — the diff
        # then carries the deletions the way a trial's patch does.
        manifest = solution / ".deleted"
        if manifest.exists():
            for line in manifest.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                for victim in sorted(Path(td).glob(line)):
                    if victim.is_file():
                        victim.unlink()
        for src in solution.rglob("*"):
            if not src.is_file() or src == manifest:
                continue
            dest = Path(td) / src.relative_to(solution)
            dest.parent.mkdir(parents=True, exist_ok=True)
            # copyfile, not copy2: a preserved mtime plus an equal size lets
            # `git add` trust its stat cache and skip a changed file entirely
            # (bit us on a Dockerfile whose old and new FROM lines matched in
            # length). A fresh mtime forces the content read.
            shutil.copyfile(src, dest)
        run("git", "add", "-A")
        return subprocess.run(
            ["git", "diff", "--cached", "--binary", "HEAD"], cwd=td, check=True, capture_output=True
        ).stdout


def _cmd_prove(args: argparse.Namespace) -> int:
    """Prove every task that ships a solution/ is measurable: the solution patch
    must pass all checks in Modal, and a delivered-but-unchanged workspace must
    fail at least one. Covers own-workspace tasks and overlay tasks alike —
    what qualifies is a solution/ beside the task. The local test suite covers
    the file-fact half of this; here the runtime checks get the same treatment."""
    import modal

    from de_bench.modal_app import app, score_trial, score_trial_copperline

    tasks = [
        t
        for t in load_tasks(include_drafts=args.include_drafts)
        if (t.dir / "solution").is_dir()
    ]
    if args.world:
        tasks = _bind_world(tasks, args.world)
    if args.tasks:
        wanted = set(args.tasks)
        tasks = [t for t in tasks if t.id in wanted or t.id.split("-")[-1] in wanted]
    if not tasks:
        print("no tasks with a solution/ to validate", file=sys.stderr)
        return 1

    # An untouched-but-delivered tree: the marker file makes `delivered` pass so
    # every real check runs against what the agent was handed.
    baseline_patch = (
        "diff --git a/NOTES.txt b/NOTES.txt\nnew file mode 100644\nindex 0000000..bd42003\n"
        "--- /dev/null\n+++ b/NOTES.txt\n@@ -0,0 +1 @@\n+reviewed\n"
    ).encode()

    groups: dict[str, list[dict]] = {}
    for t in tasks:
        tar = workspace_tar(t)
        common = {
            "trial": 0,
            "checks": [dict(c) for c in t.checks],
            "replay_dags": None if t.replay_dags is None else list(t.replay_dags),
            "do_not_modify": list(t.do_not_modify),
            "workspace_tar": tar,
            "live_env": t.live_env,
            "verifier_tar": verifier_tar(t),
            "oracle_tar": oracle_tar(t),
            **_runtime_spec_fields(t),
        }
        group = groups.setdefault(t.image, [])
        group.append({**common, "config": "solution", "task_id": t.id, "patch": _solution_patch(t)})
        group.append({**common, "config": "baseline", "task_id": t.id, "patch": baseline_patch})

    scorers = {"default": score_trial, "copperline": score_trial_copperline}
    print(f"validating {len(tasks)} task(s) in Modal (solution must pass, baseline must fail)")
    verdicts: dict[str, dict] = {}
    with modal.enable_output():
        with app.run():
          for image, group in groups.items():
            for score in scorers[image].map(group, order_outputs=False, return_exceptions=True):
                if isinstance(score, Exception):
                    print(f"  verdict lost to a remote failure ({score}); rerun those tasks", file=sys.stderr)
                    continue
                verdicts[f"{score['task_id']}/{score['config']}"] = score

    bad = 0
    for t in tasks:
        solution = verdicts.get(f"{t.id}/solution") or {}
        base = verdicts.get(f"{t.id}/baseline") or {}
        problems = []
        if not solution.get("pass"):
            failed = {k: v.get("detail", "") for k, v in (solution.get("checks") or {}).items() if v.get("passed") is False}
            blame = "; ".join(f"{k} ({d[:160]})" for k, d in failed.items())
            problems.append(f"solution fails: {blame or solution.get('detail', solution.get('score_outcome', '?'))}")
        if base.get("pass"):
            problems.append("untouched workspace passes every check")
        if problems:
            bad += 1
            print(f"  BAD  {t.id}: " + "; ".join(problems))
        else:
            failed = [k for k, v in (base.get("checks") or {}).items() if v.get("passed") is False]
            print(f"  ok   {t.id} (baseline dies on: {', '.join(failed[:3])})")
    print(f"{len(tasks) - bad}/{len(tasks)} task(s) prove out")
    return 1 if bad else 0


def _task_worlds() -> dict[str, str]:
    """Which world each task belongs to, so a report can split by task origin."""
    return {t.id: t.world.name for t in load_tasks(include_drafts=True)}


def _cmd_report(args: argparse.Namespace) -> int:
    """Build the price-performance report for a stored run. Reads artifacts only."""
    from de_bench.report import build

    run_dir = Path(args.run_dir)
    out = Path(args.out) if args.out else run_dir / "report.md"
    markdown, chart, _ = build(run_dir, out, _task_worlds())
    out.write_text(markdown, encoding="utf-8")

    print(markdown)
    print(f"wrote {out}" + (f" and {chart}" if chart else ""))
    return 0


def _cmd_snapshot(args: argparse.Namespace) -> int:
    """Refresh the committed results file from every stored run."""
    from de_bench.report import snapshot

    out = Path(args.out)
    # A directory without world.yaml is not a world — sparse checkouts drop
    # whole worlds and leave the dir behind, and hashing one crashed here.
    worlds = {d.name: world_sha(d.name)
              for d in sorted((repo_root() / "worlds").iterdir())
              if d.is_dir() and (d / "world.yaml").exists()}
    workspaces = {t.id: workspace_sha(t) for t in load_tasks(include_drafts=True)}
    # Only ready tasks publish. A saturated task's rows are held out, not lost.
    published = {t.id for t in load_tasks()}
    markdown, chart, _, info = snapshot(_runs_dir(args), out, worlds, workspaces, _task_worlds(),
                                        published=published)
    out.write_text(markdown, encoding="utf-8")

    print(markdown)
    if info["stale"]:
        print(f"WARNING: skipped {len(info['stale'])} run(s) built against a world that has since changed")
    if info.get("changed_tasks"):
        dropped = sum(info["changed_tasks"].values())
        print(f"WARNING: dropped {dropped} row(s) across {len(info['changed_tasks'])} task(s) "
              "whose own workspace has changed since the run")
    for root, sizes in (info.get("mixed_sizes") or {}).items():
        print(f"WARNING: {root} contributed rows at {len(sizes)} sizes ({', '.join(sizes)}) — "
              "this table averages them into one rate, which is not what they measure")
    print(f"wrote {out}" + (f" and {chart}" if chart else ""))
    return 0


def _bind_world(tasks: list[Task], name: str) -> list[Task]:
    """The tasks `--world name` asks for, bound to that world.

    A plain world filters, the way it always has. A variant — a world that
    extends another — takes its parent's tasks and rebinds them to itself, so the
    same tickets run in a bigger estate without a second copy of every task
    directory. Nothing about grading moves: a variant inherits its parent's
    checks, seed DAGs and protected paths, and only the tree around them grows.
    """
    if not (repo_root() / "worlds" / name / "world.yaml").exists():
        raise SystemExit(f"no such world: {name}")
    world = load_world(name)
    if world.extends is None:
        return [t for t in tasks if t.world.name == name]
    return [replace(t, world=world) for t in tasks if t.world.name == world.root_name]


def _runs_dir(args: argparse.Namespace) -> Path:
    """Where runs are read from, and a word if some are somewhere else.

    Runs used to live in the checkout, so a machine that has been through that
    era can still have a `runs/` sitting there. Saying so beats silently
    reporting on half the evidence.
    """
    if getattr(args, "runs", None):
        return Path(args.runs)
    stray = stray_runs()
    if stray is not None:
        print(f"note: {stray} still holds runs; move them into {runs_root()} to include them",
              file=sys.stderr)
    return runs_root()


def _cmd_browse(args: argparse.Namespace) -> int:
    """One local HTML page over every stored run — coverage, not the published view."""
    from de_bench.browse import build

    catalogue = [
        {"id": t.id, "title": t.title, "world": t.world.name,
         "difficulty": t.difficulty, "status": t.status}
        for t in load_tasks(include_drafts=True)
    ]
    # A directory without world.yaml is not a world — sparse checkouts drop
    # whole worlds and leave the dir behind, and hashing one crashed here.
    worlds = {d.name: world_sha(d.name)
              for d in sorted((repo_root() / "worlds").iterdir())
              if d.is_dir() and (d / "world.yaml").exists()}
    runs_dir = _runs_dir(args)
    out, csvs, summary = build(runs_dir, Path(args.out), catalogue, worlds, args.csv)

    print(f"read {runs_dir}")
    print(f"{len(summary['tasks'])} task(s): "
          f"{len(summary['never_run'])} never run, "
          f"{len(summary['never_passed'])} never passed, "
          f"{len(summary['unscored'])} with unscored trials, "
          f"{len(summary['infra'])} with harness failures, "
          f"{len(summary['flipping'])} flipping")
    print(f"wrote {out}" + ("".join(f" and {c}" for c in csvs) if csvs else ""))
    return 0


def _render_scores(scores: list[dict]) -> str:
    """Per config: how many trials passed, and which check stopped the rest.

    Check names carry their target (`dag_runs:some_dag`), so this counts by kind
    — naming stages here would silently report zero the next time one is added.
    """
    import collections

    agg: dict[str, dict] = collections.defaultdict(lambda: {"n": 0, "ok": 0})
    blame: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for s in scores:
        a = agg[s["config"]]
        a["n"] += 1
        a["ok"] += bool(s.get("pass"))
        if s.get("pass"):
            continue
        executed = s.get("checks") or s.get("tiers") or {}
        culprit = next(
            (n for n, r in executed.items() if (r or {}).get("passed") is False),
            s.get("score_outcome", "unknown"),
        )
        blame[s["config"]][culprit.split(":")[0]] += 1

    lines = ["| config | pass | stopped by |", "|---|---:|---|"]
    for k in sorted(agg):
        a = agg[k]
        why = ", ".join(f"{kind} x{n}" for kind, n in blame[k].most_common(4)) or "—"
        lines.append(f"| {k} | {a['ok']}/{a['n']} | {why} |")
    return "\n".join(lines) + "\n"



def _cmd_backfill(args: argparse.Namespace) -> int:
    """Seed the trial cache from a stored run (for runs made before the cache
    existed, or copied from another machine). Keys are recomputed from the run's
    meta.json + the current worlds, so those must match what the run saw."""
    run_dir = Path(args.run_dir)
    results_dir = run_dir if (run_dir / "results.json").exists() else next(
        d for d in run_dir.iterdir() if (d / "results.json").exists()
    )
    meta = json.loads((results_dir / "meta.json").read_text())
    for name, sha in (meta.get("worlds") or {}).items():
        if world_sha(name) != sha:
            print(f"world {name!r} differs from the run's recorded hash; refusing", file=sys.stderr)
            return 2
    tasks = {t.id: t for t in load_tasks(include_drafts=True)}
    vol = _cache_volume()
    volume_keys = _volume_keys(vol)
    configs = {c["name"]: c for c in meta["configs"]}
    results = json.loads((results_dir / "results.json").read_text())
    stored = skipped = 0
    recorded = meta.get("task_workspaces") or {}
    for r in results:
        c, t = configs.get(r["config"]), tasks.get(r["task_id"])
        if not c or not t:
            skipped += 1
            continue
        # The world-level refusal above misses what only the task's own hash
        # sees: an overlay edit, or an edited planted row. A key recomputed
        # from the current tree would then file the stored trial under data
        # it never ran on, so skip the task instead.
        if r["task_id"] in recorded and workspace_sha(t) != recorded[r["task_id"]]:
            skipped += 1
            continue
        trial = r.get("trial", 0)
        trial_dir = results_dir / r["config"] / r["task_id"] / f"t{trial}"
        if not trial_dir.exists():
            trial_dir = results_dir / r["config"] / r["task_id"]
        if not (trial_dir / "summary.json").exists():
            skipped += 1
            continue
        model = AGENTS[c["agent"]].resolve_model(c["model"])
        ckey = _trial_key(c["agent"], model, c["thinking"], t, trial, system_prompt(t))
        if (CACHE_DIR / ckey).exists() and ckey in volume_keys:
            skipped += 1
            continue
        _cache_store(ckey, trial_dir, vol)
        stored += 1
    print(f"backfilled {stored} trial(s) into {CACHE_DIR} ({skipped} skipped)")
    return 0


def _render_table(results: list[dict]) -> str:
    lines = [
        "| task | config | status | wall s | turns | tools | in tok | out tok | cost $ |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in results:
        t = r["telemetry"]
        status = r["status"] if r.get("outcome") != "infra_failed" else f"{r['status']} (INFRA)"
        lines.append(
            f"| {r['task_id']} | {r['config']} | {status} | {r['wall_seconds']} "
            f"| {t['turns']} | {t['tool_calls']} | {t['input_tokens']} | {t['output_tokens']} "
            f"| {t['cost_usd']:.4f} |"
        )
    ok = [r for r in results if r.get("outcome") != "infra_failed"]
    total_cost = sum(r["telemetry"]["cost_usd"] for r in ok)
    n_infra = len(results) - len(ok)
    suffix = f" ({n_infra} infra_failed excluded)" if n_infra else ""
    lines.append(f"\ntotal cost: ${total_cost:.4f} across {len(ok)} scored trial(s){suffix}\n")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(prog="de-bench")
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="list all tasks")
    p_list.set_defaults(func=_cmd_list)

    p_run = sub.add_parser("run", help="run agents on tasks in Modal")
    p_run.add_argument("--agents", default="pi", type=lambda s: s.split(","))
    p_run.add_argument("--world", help="only tasks bound to this world")
    p_run.add_argument("--tag", help="only tasks carrying this tag")
    p_run.add_argument("--name", required=True, help="run label, used in the output dir")
    p_run.add_argument("--tasks", type=lambda s: s.split(","), help="task ids (or hex suffixes)")
    p_run.add_argument("--limit", type=int)
    p_run.add_argument("--model", default=DEFAULT_MODEL)
    p_run.add_argument("--thinking", default=DEFAULT_THINKING)
    p_run.add_argument("--trials", type=int, default=1, help="repeats per config x task")
    p_run.add_argument("--no-cache", action="store_true",
                       help="sample every cell fresh and leave the cache untouched (no read, no write)")
    p_run.add_argument("--wall", type=int, help="override every task's wall cap, in seconds "
                                                "(for exercising the cap itself — scores from a "
                                                "shortened run are not comparable)")
    p_run.add_argument("--no-system-prompt", action="store_true",
                       help="skip the world's system_prompt_text injection (ablation only — "
                            "scores from a run like this are not comparable to a normal run)")
    p_run.add_argument("--matrix", help="YAML of named (agent, model, thinking) configs; overrides --agents/--model/--thinking")
    p_run.set_defaults(func=_cmd_run)

    p_score = sub.add_parser("score", help="score a stored run's patches (no agent spend)")
    p_score.add_argument("run_dir", help="runs/<date>-<name> (older runs kept results one level down)")
    p_score.add_argument("--tasks", type=lambda s: s.split(","),
                         help="only rescore these task ids (or hex suffixes) — merges into "
                              "the existing scores.json/scores.md instead of replacing them")
    p_score.add_argument("--rounds", type=int, default=1,
                         help="score N independent rounds back to back, writing "
                              "scores.r1.json..scores.rN.json instead of a single scores.json "
                              "(the judge has its own noise; multiple rounds measure it). "
                              "Does not vote or tiebreak — that stays a separate, deliberate step.")
    p_score.set_defaults(func=_cmd_score)

    p_prove = sub.add_parser("prove", help="validate tasks that ship a solution/: it passes, baseline fails")
    p_prove.add_argument("--tasks", type=lambda s: s.split(","), help="task ids (or hex suffixes)")
    p_prove.add_argument("--world", help="only tasks bound to this world")
    p_prove.add_argument("--include-drafts", action="store_true")
    p_prove.set_defaults(func=_cmd_prove)

    p_report = sub.add_parser("report", help="build the cost-vs-pass-rate report for a stored run")
    p_report.add_argument("run_dir", help="runs/<date>-<name>")
    p_report.add_argument("--out", help="output path (default <run_dir>/report.md)")
    p_report.set_defaults(func=_cmd_report)

    p_snap = sub.add_parser("snapshot", help="refresh the committed RESULTS.md from every stored run")
    p_snap.add_argument("--runs", default=None,
                        help="directory of stored runs (default: see de-bench where)")
    p_snap.add_argument("--out", default="RESULTS.md", help="output path (default RESULTS.md)")
    p_snap.set_defaults(func=_cmd_snapshot)

    p_browse = sub.add_parser("browse", help="one local HTML page over every stored run")
    p_browse.add_argument("--runs", default=None,
                          help="directory of stored runs (default: ~/.de-bench/runs, "
                               "or $DE_BENCH_RUNS)")
    p_browse.add_argument("--out", default="browse.html")
    p_browse.add_argument("--csv", action="store_true", help="also write tasks.csv and trials.csv")
    p_browse.set_defaults(func=_cmd_browse)

    p_rebuild = sub.add_parser("rebuild-results", help="rebuild results.json from a run's trial directories")
    p_rebuild.add_argument("run_dir")
    p_rebuild.set_defaults(func=_cmd_rebuild)

    p_backfill = sub.add_parser("cache-backfill", help="seed the trial cache from a stored run")
    p_backfill.add_argument("run_dir")
    p_backfill.set_defaults(func=_cmd_backfill)

    args = parser.parse_args()
    sys.exit(args.func(args))
