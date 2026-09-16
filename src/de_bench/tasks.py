"""Load the benchmark's own tasks and worlds.

The corpus lives in this repo: `worlds/<name>/` is a world — metadata in
`world.yaml`, the agent's working tree in `workspace/` — and `tasks/<world>/<id>/`
is a task: `task.yaml` (spec), `prompt.md`, `checks.yaml` (grading), optional
read-only `evidence/`. A task binds to its world by name. A task may instead
carry its own `workspace/` (with a known-good `solution/` beside it), in which
case the world supplies only defaults and the system prompt.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import tarfile
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path

import yaml

DEFAULT_WALL_SECONDS = 1800

# What a check must carry to mean anything. Kept here rather than in the scorer
# so a malformed checks.yaml fails at load, not an hour into a run.
CHECK_KINDS: dict[str, tuple[str, ...]] = {
    "dag_parses": ("dag_id",),
    "dag_runs": ("dag_id",),
    "dag_fails": ("dag_id",),
    "dag_structure": ("dag_id", "task_ids"),
    "no_dag": ("dag_id",),
    "no_dbt_at_parse": (),
    "idempotent": ("dag_id",),
    "partition_csv": ("layer", "name"),
    "warehouse_table": ("table",),
    "no_warehouse_table": ("table",),
    "file_contains": ("path",),
    "file_lacks": ("path", "patterns"),
    "file_absent": (),
    "json_matches": ("path",),
    "pytest_verifier": (),
    # The untouched world is the expected result: the scorer runs both trees in
    # one container and compares the relations named here. Nobody types a value.
    # Optional: `ds`, `run` (dag_ids to execute on each arm; the world's seed
    # DAGs by default), `db`, `allow_columns`.
    "unchanged": ("relations",),
    # One of the primitives vendored in de_bench.upgrade_v2_scoring — static
    # analysis (AST/regex/YAML) over the patched tree, some of them also
    # reading the task's own solution/ + task.yaml via oracle_dir.
    "scoring_primitive": ("name",),
}

# Never ship these into a workspace tar: run junk, or state the world must
# bootstrap itself (worlds hold source only). "solution" is the known-good fix a
# task keeps beside its workspace, and "verifier" is the grading pytest a
# task keeps there too — belt and braces here in case one is ever nested
# inside the tree that ships.
_EXCLUDE_NAMES = {".git", "__pycache__", ".pytest_cache", "target", "logs", "solution", "verifier"}


def repo_root() -> Path:
    if env := os.environ.get("DE_BENCH_ROOT"):
        return Path(env)
    root = Path(__file__).resolve().parents[2]
    if not (root / "worlds").is_dir():  # non-editable install; assume cwd is the repo
        root = Path.cwd()
    return root


def runs_root() -> Path:
    """Where stored runs live — outside any checkout, on purpose.

    A run is local state about work you did, not repo content, and it is
    git-ignored. Keeping it inside a checkout made it the checkout's property: a
    worktree got its own, `snapshot` saw whichever it was standing in, and
    artifacts had to be copied out by hand before a worktree could be removed or
    they were lost. One directory outside every checkout removes all of that.

    `DE_BENCH_RUNS` wins, for keeping two unrelated experiments apart.

    One location, no fallback to a checkout-local `runs/`. A fallback that fires
    when the directory exists would resolve to the *worktree's* runs inside a
    worktree, which is the bug this is here to remove. `stray_runs()` finds a
    leftover directory instead and says so, and moving it is a one-line job.
    """
    if env := os.environ.get("DE_BENCH_RUNS"):
        return Path(env).expanduser()
    return Path.home() / ".de-bench" / "runs"


def stray_runs() -> Path | None:
    """A checkout-local `runs/` holding results, from before they moved out."""
    legacy = repo_root() / "runs"
    if legacy.is_dir() and legacy.resolve() != runs_root().resolve():
        if any(p.is_dir() for p in legacy.iterdir()):
            return legacy
    return None


@dataclass
class World:
    name: str
    dir: Path
    # The trees that make up this world's workspace, base first. A world with no
    # parent has exactly one. A variant has its parent's tree and then its own,
    # which is laid over the top, so what the parent holds is byte-identical in
    # both by construction rather than by two copies anyone has to keep in step.
    workspace_dirs: tuple[Path, ...]
    default_wall_seconds: int
    do_not_modify: tuple[str, ...]
    seed_defaults: tuple[str, ...] | None = None  # None = replay every baseline DAG
    # Which Modal image family this world's trials run on. "default" is the
    # Airflow image every existing world uses; a world that needs extra baked-in
    # state (a prepared warehouse baked into the image) names its own.
    image: str = "default"
    # Environment the trial and the scorer both export before anything runs —
    # how a world tells dbt where its project and warehouse live.
    env: tuple[tuple[str, str], ...] = ()
    # Files the image carries that must land inside the workspace at setup:
    # (absolute path in the image, path relative to the workspace root). For
    # data too large to ship in the repo or the tar, such as a warehouse.
    workspace_data: tuple[tuple[str, str], ...] = ()
    # Suffix the system prompt with the airflow_state paragraph. Worlds whose
    # tasks never touch Airflow switch it off rather than open the prompt with
    # a disclaimer about a tool the ticket never mentions.
    airflow_note: bool = True
    # Folded into the trial-cache key. Worlds whose image bakes in data the
    # workspace hash cannot see (a baked-in warehouse) bump this when that
    # data changes, so stale cells retire instead of being served.
    cache_salt: str = ""
    # Generated data: the generator package and the config that drives it, as
    # resolved paths {"generator": ..., "timeline": ..., "planted": ...}.
    # All three live beside workspace/, never inside it — the generator is an
    # answer key, so it reaches the trial container at setup and is deleted
    # before the agent starts. None for worlds that generate no data.
    # Declared as a top-level `generated:` block in world.yaml: `generator` is
    # repo-root-relative, `timeline`/`planted` are relative to the world's dir.
    # Folded into world_sha and workspace_sha, which is what keeps a stored
    # trial from being reused against data it never ran on.
    generated: dict | None = None
    # Extra binaries preflight must resolve for this world's trials — the
    # default probe list is Airflow-shaped.
    preflight_tools: tuple[str, ...] = ()
    # The system prompt, written into world.yaml. Every world sets it; a test
    # asserts no task renders an empty one. Anything the agent should know about
    # the project itself belongs in the workspace's AGENTS.md, not here.
    system_prompt_text: str = ""
    # The world this one grows out of, or None. A variant is the same world at a
    # different size: same tasks, same grading, more estate around them.
    extends: str | None = None
    # The world at the bottom of the extends chain — the name tasks bind to.
    # Its own name when it extends nothing.
    root_name: str = ""
    # This world's world.yaml with everything it inherits already folded in. Kept
    # so a world that extends this one merges against what this one effectively
    # is, rather than against a file that may say almost nothing.
    spec: dict = field(default_factory=dict)


@dataclass
class Task:
    id: str
    dir: Path  # the task's own directory, tasks/<world>/<id>/
    world: World
    status: str
    title: str
    difficulty: str
    prompt: str
    evidence_dir: Path | None
    max_wall_seconds: int
    checks: tuple[dict, ...]
    replay_dags: tuple[str, ...] | None = None  # None = replay every baseline DAG
    dag_ids: tuple[str, ...] = ()
    do_not_modify: tuple[str, ...] = ()
    boot_airflow: bool = True
    # Historical DagRuns written into the metadata database before the agent
    # starts and again before scoring. This is what a diagnosis task stands on:
    # the evidence is run history, not a file. The logical date may differ from
    # any check's ds, which is what lets an upstream run sit at midnight while
    # the graded DAG runs at its own cron time. Two entry shapes, both written
    # by scoring.write_history so the agent and the grader see the same history:
    #
    #   ("dag_id", "ds")            executed in order with `airflow dags test`,
    #                               failures tolerated — a failed run is usually
    #                               the point.
    #   {"dag_id": ..., "runs": []} synthesized: each run states its own state,
    #                               task-instance states, tries and log files,
    #                               and several runs may share one date. A run
    #                               with no state is executed, as above.
    #
    # See _parse_history for the accepted YAML and scoring.write_history for
    # what each shape writes.
    history: tuple[tuple[str, str] | dict, ...] = ()
    tags: tuple[str, ...] = field(default_factory=tuple)
    # A task may ship its own workspace/ beside task.yaml, replacing the world's
    # tree wholesale. The world then contributes only defaults and a system
    # prompt. A solution/ next to it never ships — see workspace_tar.
    own_workspace: Path | None = None
    # live_env: the trial and the scorer each build a virtualenv from the
    # workspace's requirements.txt — the trial from the tree as shipped (the
    # project's own, usually 2.x, world), the scorer from the tree as patched
    # (the future state the agent pinned). start_airflow seeds the trial env
    # when the workspace carries no apache-airflow pin (astro projects pin
    # through their Dockerfile); a requirements.txt pin wins over it.
    live_env: bool = False
    start_airflow: str | None = None
    # A world-backed task may lay a few files of its own over the shared tree
    # (a buggy model to fix, a data file to consolidate) without carrying the
    # whole workspace. Overlay files win over the world's on extraction.
    overlay: Path | None = None
    # SQL files (workspace-relative) run against the task's DuckDB warehouse at
    # setup, after workspace_data lands — per-task tables the shared warehouse
    # does not carry.
    setup_sql: tuple[str, ...] = ()
    # The grading pytest beside the task (tasks/<id>/verifier/). Never ships to
    # the agent; the scorer extracts it next to the workspace and runs it.
    verifier_dir: Path | None = None
    # The task's own directory, when it ships a solution/ beside task.yaml —
    # the oracle a scoring_primitive check reads via oracle_dir (the known-good
    # fix under solution/, and target.airflow etc. from this same task.yaml).
    # Never ships to the agent, same as verifier_dir; the scorer extracts it
    # next to the workspace, outside workdir, so `ls` can't find it either.
    oracle_dir: Path | None = None

    @property
    def workspace_dirs(self) -> tuple[Path, ...]:
        """The trees that make up the tree the agent gets, base first.

        A task that carries its own workspace replaces the world's outright; the
        world then contributes only defaults and a system prompt. Otherwise this
        is the world's chain, which is longer than one only for a variant.
        """
        return (self.own_workspace,) if self.own_workspace else self.world.workspace_dirs

    def shipped(self, rel: str) -> Path | None:
        """The file that lands at `rel` in the shipped tree, or None if none
        does. Later trees win, the same way extraction resolves them — and the
        task's own overlay lands last of all."""
        if self.overlay is not None and (self.overlay / rel).exists():
            return self.overlay / rel
        for base in reversed(self.workspace_dirs):
            path = base / rel
            if path.exists():
                return path
        return None

    @property
    def image(self) -> str:
        return self.world.image

    @property
    def env(self) -> dict[str, str]:
        return dict(self.world.env)

    @property
    def workspace_data(self) -> tuple[tuple[str, str], ...]:
        return self.world.workspace_data


def _load_yaml(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _merge_world(parent_spec: dict, child_spec: dict) -> dict:
    """The parent's world.yaml with the child's keys laid over it.

    One level deep under `runtime` and `task_defaults`, and a plain replace
    everywhere else. Deeper merging would let a variant change grading by
    accident, which is the one thing a variant must not do.
    """
    merged = dict(parent_spec)
    for key, value in child_spec.items():
        if key == "extends":
            continue
        if key in ("runtime", "task_defaults") and isinstance(value, dict):
            merged[key] = {**(parent_spec.get(key) or {}), **value}
        else:
            merged[key] = value
    return merged


def load_world(name: str, root: Path | None = None, _seen: tuple[str, ...] = ()) -> World:
    """A world, and everything it inherits from the world it extends.

    A variant states only what it adds: `extends: <parent>` plus a `workspace/`
    holding the projects that size brings with it. Everything else — the system
    prompt, the seed DAGs, the protected paths, the image — comes from the parent,
    because a variant that graded differently would not be the same world at a
    different size, it would be a different world.
    """
    world_dir = (root or repo_root()) / "worlds" / name
    spec = _load_yaml(world_dir / "world.yaml")
    # The world's own declaration, before the parent's spec folds in — a
    # variant inherits its parent's generated block with paths already
    # resolved against the directory that declared them.
    own_generated = spec.get("generated")
    parent = None
    if extends := spec.get("extends"):
        if extends in _seen or extends == name:
            raise ValueError(f"world {name!r}: extends loops back on itself")
        parent = load_world(str(extends), root, _seen + (name,))
        # The parent's *effective* spec, not the file on disk. A middle world that
        # states only `extends` has an almost empty file, so merging against the
        # file drops everything the grandparent set — which cost the messy world
        # its seed DAGs, and `seed_defaults: None` means "replay every baseline
        # DAG", so it would have graded differently while looking fine.
        spec = _merge_world(parent_spec=parent.spec, child_spec=spec)
    defaults = spec.get("task_defaults") or {}
    runtime = spec.get("runtime") or {}
    if own_generated:
        generated = {
            "generator": (root or repo_root()) / str(own_generated["generator"]),
            "timeline": world_dir / str(own_generated.get("timeline", "timeline.yaml")),
            "planted": world_dir / str(own_generated.get("planted", "planted.yaml")),
            # baked: the Modal image ran the generator at build and carries
            # the output; trials copy it in via workspace_data instead of
            # generating, and assert the image's bake sha against the config.
            "baked": bool(own_generated.get("baked")),
        }
    else:
        generated = parent.generated if parent else None
    return World(
        name=name,
        dir=world_dir,
        spec=spec,
        workspace_dirs=(parent.workspace_dirs if parent else ()) + (world_dir / "workspace",),
        extends=parent.name if parent else None,
        root_name=parent.root_name if parent else name,
        seed_defaults=None if spec.get("seed_defaults") is None else tuple(spec["seed_defaults"]),
        default_wall_seconds=int(defaults.get("max_wall_seconds") or DEFAULT_WALL_SECONDS),
        do_not_modify=tuple(defaults.get("do_not_modify") or ()),
        image=runtime.get("image", "default"),
        env=tuple(sorted((str(k), str(v)) for k, v in (runtime.get("env") or {}).items())),
        workspace_data=tuple(
            (str(d["from"]), str(d["to"])) for d in (runtime.get("workspace_data") or ())
        ),
        airflow_note=bool(runtime.get("airflow_note", True)),
        cache_salt=str(runtime.get("cache_salt", "")),
        preflight_tools=tuple(runtime.get("preflight_tools") or ()),
        system_prompt_text=str(runtime.get("system_prompt_text") or ""),
        generated=generated,
    )


HISTORY_DS_DEFAULT = "2026-03-01"


def _parse_history_run(run: dict) -> dict:
    """One run inside a history manifest entry, with its keys made explicit.

    The parse names every key it keeps, so the cache key does not move when a
    task author reorders the YAML.

      ds           the run's logical date. The first run on a date keeps it;
                   a later run on the same date is a manual rerun with none,
                   which is all Airflow's unique index allows.
      state        the DagRun's state. A run without one is executed instead.
      task_states  {task_id: state}. Without it the harness gives every task in
                   the DAG the run's own state, so state that is not uniform —
                   a failed run — should name its tasks.
      tries        attempts for every task in the run; task_tries overrides it
                   per task. Attempt n carries the task's state, and the ones
                   before it are recorded as up_for_retry.
      logs         {task_id: path} for the task's last attempt, or
                   {task_id: {try number: path}} to write an earlier one too.
                   Paths are workspace-relative, so a task's evidence/ ships
                   them.
      run_id       overrides the run id the harness builds from the date.
      run_type     scheduled, manual, backfill or asset_triggered.
    """
    out: dict = {"ds": str(run.get("ds") or HISTORY_DS_DEFAULT)}
    for key in ("state", "run_id", "run_type"):
        if run.get(key) is not None:
            out[key] = str(run[key])
    if run.get("tries") is not None:
        out["tries"] = int(run["tries"])
    if run.get("task_states"):
        out["task_states"] = {str(k): str(v) for k, v in run["task_states"].items()}
    if run.get("task_tries"):
        out["task_tries"] = {str(k): int(v) for k, v in run["task_tries"].items()}
    if run.get("logs"):
        out["logs"] = {
            str(task_id): (
                {str(try_number): str(path) for try_number, path in value.items()}
                if isinstance(value, dict)
                else str(value)
            )
            for task_id, value in run["logs"].items()
        }
    return out


def _parse_history(raw) -> tuple:
    """Seeded run history, in two forms that sit side by side.

    Executed form — a bare dag_id, or `{dag_id, ds}` — stays a (dag_id, ds)
    pair. The harness runs it with `airflow dags test`, and it keeps the
    cache-key serialization it always had, so trials stored before the manifest
    existed keep their keys.

    Manifest form — `{dag_id, runs: [...]}` — states history the harness writes
    into the metadata database instead of running: the state of each run,
    several runs on one date, task-instance states, try counts and log files. A
    run inside it that gives neither `state` nor `task_states` is executed, the
    same as the pair form.
    """
    out: list = []
    for entry in raw or ():
        if not isinstance(entry, dict):
            out.append((str(entry), HISTORY_DS_DEFAULT))
        elif "runs" in entry:
            out.append(
                {
                    "dag_id": str(entry["dag_id"]),
                    "runs": [_parse_history_run(r) for r in (entry.get("runs") or ())],
                }
            )
        else:
            out.append((str(entry["dag_id"]), str(entry.get("ds") or HISTORY_DS_DEFAULT)))
    return tuple(out)


def load_task(task_dir: Path, worlds: dict[str, World], root: Path | None = None) -> Task:
    spec = _load_yaml(task_dir / "task.yaml")
    world_name = spec["world"]
    if world_name not in worlds:
        worlds[world_name] = load_world(world_name, root)
    world = worlds[world_name]

    prompt_ref = spec.get("prompt", "prompt.md")
    prompt_file = task_dir / str(prompt_ref)
    prompt = prompt_file.read_text(encoding="utf-8") if prompt_file.exists() else str(prompt_ref)

    checks_path = task_dir / "checks.yaml"
    checks_doc = _load_yaml(checks_path) if checks_path.exists() else {}
    checks = tuple(checks_doc.get("checks") or ())
    # Which of the world's own DAGs the scorer replays before grading. Omit for
    # all of them; give an explicit list (often empty) when the task is about
    # what its own DAG builds — replaying would otherwise build it for the
    # agent. The key is `replay:`; `seed:` is the same thing under the name the
    # existing checks.yaml files use.
    replay = checks_doc.get("replay", checks_doc.get("seed"))
    replay_dags = world.seed_defaults if replay is None else tuple(replay)
    for c in checks:
        required = CHECK_KINDS.get(c.get("kind"))
        if required is None:
            raise ValueError(f"{task_dir.name}: unknown check kind {c.get('kind')!r}")
        missing = [f for f in required if not c.get(f)]
        if missing:
            raise ValueError(f"{task_dir.name}: {c['kind']} check missing {', '.join(missing)}")
        if c["kind"] == "unchanged":
            # A bare string would grade every character of the name as a
            # relation, so say so at load rather than an hour into a run.
            for field_name in ("relations", "run", "allow_columns"):
                value = c.get(field_name)
                if value is not None and not isinstance(value, list):
                    raise ValueError(f"{task_dir.name}: unchanged {field_name} must be a list")

    # The prose judge's fact key rides in the check spec, so scoring needs no
    # access to the task directory. `seq` is the check's ordinal among this
    # task's file_contains checks on RESPONSE.md, in file order.
    judge_path = task_dir / "judge_facts.yaml"
    if judge_path.exists():
        entries = {e["seq"]: e for e in (_load_yaml(judge_path).get("checks") or [])}
        seq = 0
        for c in checks:
            if c.get("kind") == "file_contains" and c.get("path") == "RESPONSE.md":
                if seq in entries:
                    e = entries[seq]
                    c["judge_facts"] = {
                        "background": e.get("background") or "",
                        "groups": e.get("groups") or [],
                        "convicts": e.get("convicts") or [],
                    }
                seq += 1
        uncovered = seq - len(entries)
        if uncovered:
            raise ValueError(
                f"{task_dir.name}: judge_facts.yaml covers {len(entries)} of {seq} "
                f"RESPONSE.md prose checks — every prose check needs a fact entry"
            )

    budget = spec.get("budget") or {}
    focus = spec.get("focus_area") or {}
    runtime_spec = spec.get("runtime") or {}
    evidence = task_dir / "evidence"
    own_workspace = task_dir / "workspace"
    overlay = task_dir / "overlay"
    verifier = task_dir / "verifier"
    oracle_solution = task_dir / "solution"
    if own_workspace.is_dir() and overlay.is_dir():
        raise ValueError(f"{task_dir.name}: workspace/ and overlay/ are mutually exclusive")
    return Task(
        id=spec.get("id", task_dir.name),
        dir=task_dir,
        world=world,
        status=spec.get("status", "ready"),
        title=spec.get("title", ""),
        difficulty=spec.get("difficulty", ""),
        prompt=prompt,
        evidence_dir=evidence if evidence.is_dir() else None,
        max_wall_seconds=int(budget.get("max_wall_seconds") or world.default_wall_seconds),
        checks=checks,
        replay_dags=replay_dags,
        dag_ids=tuple(focus.get("dag_ids") or ()),
        do_not_modify=tuple(focus.get("do_not_modify") or ()) + world.do_not_modify,
        boot_airflow=bool(spec.get("boot_airflow", True)),
        # `history:` is the key; `seed_runs:` is the same thing under the name
        # the existing task.yaml files use.
        history=_parse_history(spec.get("history", spec.get("seed_runs"))),
        tags=tuple(spec.get("tags") or ()),
        own_workspace=own_workspace if own_workspace.is_dir() else None,
        live_env=bool(runtime_spec.get("live_env")),
        start_airflow=runtime_spec.get("start_airflow"),
        overlay=overlay if overlay.is_dir() else None,
        setup_sql=tuple(runtime_spec.get("setup_sql") or ()),
        verifier_dir=verifier if verifier.is_dir() else None,
        oracle_dir=task_dir if oracle_solution.is_dir() else None,
    )


def load_tasks(root: Path | None = None, include_drafts: bool = False) -> list[Task]:
    """Every task under tasks/<world>/<id>/. The subdir is navigation, not truth —
    task.yaml still names the world — so a task filed under the wrong subdir is
    an error, not a rebinding."""
    root = root or repo_root()
    worlds: dict[str, World] = {}
    tasks = []
    for world_dir in sorted((root / "tasks").iterdir()):
        if not world_dir.is_dir():
            continue
        for task_dir in sorted(world_dir.iterdir()):
            if not (task_dir / "task.yaml").exists():
                continue
            task = load_task(task_dir, worlds, root)
            if task.world.root_name != world_dir.name:
                raise ValueError(
                    f"{task.id} sits under tasks/{world_dir.name}/ but binds to "
                    f"world {task.world.name!r}"
                )
            if task.status == "ready" or include_drafts:
                tasks.append(task)
    return tasks


#: Kept out of the prompt unless the task actually starts one. It used to be part of
#: prompts/ide.md unconditionally, which told 63 of 66 tasks that a live Airflow was
#: waiting for them — agents then spent turns on an `af` CLI that answered
#: "connection refused" every time, 158 trials of it on one harness alone.
_LIVE_AIRFLOW = """
<airflow_state>
A live Airflow is running for this task and `AIRFLOW_API_URL` is set. For Airflow state, load
the airflow skill when relevant and use the `af` CLI through bash. Do not run `af instance
add`, `discover`, or `use` — the instance is already configured for you.
</airflow_state>
"""

#: What the other 63 get instead. Saying nothing invites the same guesswork.
_NO_LIVE_AIRFLOW = """
<airflow_state>
No Airflow is running for this task, and there is no container runtime — `astro dev start`
and the `af` CLI have nothing to talk to. Work against the project files directly.
</airflow_state>
"""


def system_prompt(task: Task) -> str:
    """The system prompt baked into every trial against the task's world.

    A world writes its prompt straight into world.yaml — the prompt is one thing
    in one place, and anything about the project itself lives in the workspace's
    AGENTS.md instead, where an agent finds it the way it would in a real repo.

    The Airflow paragraph is chosen per task rather than per world: whether one is
    running is a property of the task, and the prompt claiming otherwise is a prompt
    that sends the agent looking for a service that isn't there. A world that says
    so in its own AGENTS.md switches the paragraph off with `airflow_note: false`.
    """
    text = task.world.system_prompt_text
    if not task.world.airflow_note:
        return text
    # Always the no-live paragraph: nothing in the harness starts an Airflow or
    # sets AIRFLOW_API_URL, yet five start_airflow tasks promised one — the
    # exact guesswork this paragraph exists to prevent, inverted. start_airflow
    # itself stays: prepare_live_env reads it as the env-build version hint.
    # _LIVE_AIRFLOW returns when the live-scheduler class lands (see
    # the scheduler scope in docs/ when it does).
    return text + _NO_LIVE_AIRFLOW


def _tar_filter(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
    parts = Path(info.name).parts
    if any(p in _EXCLUDE_NAMES for p in parts):
        return None
    # Normalize ownership: extracted-as-root files must belong to root, or git
    # in the container refuses the repo as dubiously owned.
    info.uid = info.gid = 0
    info.uname = info.gname = "root"
    return info


def workspace_tar(task: Task) -> bytes:
    """A gzipped tar of the task's world workspace (plus read-only evidence/ if it ships one).

    Ships what the workspace holds and nothing else. An earlier version synthesized
    an `.astro/config.yaml` into every workspace so the Astro CLI would stop calling
    them "not an Astro project" — which told all 48 own-workspace tasks they were
    Astro Runtime projects when 44 of them are pip-managed Airflow repos. Astro's
    own rule is not to pin `apache-airflow`, Runtime supplies it, so the agents that
    know that convention dropped the pin — and 28 of those tasks grade on the pin
    being there. The corpus already draws the line correctly: the four projects with
    a Dockerfile upgrade by moving the Runtime tag and ship the marker themselves,
    the rest upgrade by moving the pin.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        # Base tree first, then each tree the world's size lays over it, then the
        # task's own overlay. Extraction takes the last writer, so the order here
        # is the precedence order.
        for base in task.workspace_dirs:
            tar.add(base, arcname=".", filter=_tar_filter)
        if task.overlay is not None:
            # After the world tree, so extraction lets the task's files win.
            tar.add(task.overlay, arcname=".", filter=_tar_filter)
        if task.evidence_dir is not None:
            tar.add(task.evidence_dir, arcname="evidence", filter=_tar_filter)
    return buf.getvalue()


def verifier_tar(task: Task) -> bytes | None:
    """A gzipped tar of the task's grading pytest, for the scorer only."""
    if task.verifier_dir is None:
        return None
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(task.verifier_dir, arcname=".", filter=_tar_filter)
    return buf.getvalue()


def oracle_tar(task: Task) -> bytes | None:
    """A gzipped tar of the task's own solution/ + task.yaml (+ expected.json,
    when the task ships one), for the scorer only — this is the `oracle_dir`
    a scoring_primitive check reads. Just those, not the whole task
    directory: oracle_dir is task.dir itself (the primitives read
    oracle_dir/"solution"/..., oracle_dir/"task.yaml", and
    oracle_dir/"expected.json" directly), but task.dir also holds
    checks.yaml, prompt.md, and — for a task with its own workspace/ — the
    workspace a second time. None of that is oracle content."""
    if task.oracle_dir is None:
        return None

    def _oracle_filter(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
        # Same junk-exclusion and ownership normalization as _tar_filter, but
        # skip the arcname's own leading "solution" segment — _EXCLUDE_NAMES
        # lists "solution" precisely so a task's own_workspace tar never
        # leaks it, which would also strip this tar's deliberate root.
        parts = Path(info.name).parts[1:]
        if any(p in _EXCLUDE_NAMES for p in parts):
            return None
        info.uid = info.gid = 0
        info.uname = info.gname = "root"
        return info

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(task.oracle_dir / "solution", arcname="solution", filter=_oracle_filter)
        tar.add(task.oracle_dir / "task.yaml", arcname="task.yaml")
        expected = task.oracle_dir / "expected.json"
        if expected.is_file():
            tar.add(expected, arcname="expected.json")
    return buf.getvalue()


def generated_tar(task: Task) -> bytes | None:
    """A gzipped tar of the world's data generator and its config — setup
    only, never the agent. The generator is an answer key: its source names
    every era boundary and its config names every planted row. So it never sits
    inside `workspace/`; it rides beside the workspace tar, unpacks outside the
    workdir at setup, writes the warehouse, and is deleted before the agent
    starts.

    Layout inside the tar, which is the contract the container side reads:

        <pkgname>/       the generator package, named by the generator path
        timeline.yaml    always this name, whatever the world calls the file
        planted.yaml       likewise
    """
    fx = task.world.generated
    if fx is None:
        return None
    for key in ("generator", "timeline", "planted"):
        if not fx[key].exists():
            raise ValueError(f"world {task.world.name!r}: generated path {fx[key]} does not exist")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(fx["generator"], arcname=fx["generator"].name, filter=_tar_filter)
        tar.add(fx["timeline"], arcname="timeline.yaml", filter=_tar_filter)
        tar.add(fx["planted"], arcname="planted.yaml", filter=_tar_filter)
    return buf.getvalue()


def _tree_sha(root: Path) -> str:
    """Content hash of a directory tree. Tar bytes carry mtimes, so hash
    sorted (path, contents) instead."""
    h = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if not path.is_file() or set(path.parts) & _EXCLUDE_NAMES:
            continue
        h.update(str(path.relative_to(root)).encode())
        h.update(b"\0")
        h.update(path.read_bytes())
    return h.hexdigest()


@cache
def _cached_tree_sha(path: str) -> str:
    return _tree_sha(Path(path))


def _chain_sha(dirs: tuple[Path, ...]) -> str:
    """Content hash of a stack of trees. One tree hashes to its own value and
    nothing else, so every key made before variants existed stays where it is."""
    if len(dirs) == 1:
        return _cached_tree_sha(str(dirs[0]))
    h = hashlib.sha256()
    for d in dirs:
        h.update(_cached_tree_sha(str(d)).encode())
    return h.hexdigest()


def _load_planted(generated: dict) -> list:
    return yaml.safe_load(generated["planted"].read_text(encoding="utf-8")) or []


def _generated_base_sha(generated: dict) -> str:
    """Hash of the generated inputs every task in a world shares: the generator
    package source, timeline.yaml, and any planted row that declares
    changes_row_count — a row-count change is visible to every task's profile,
    so it retires every task's trials, not only the named tasks'. Every other
    planted row stays out: it names the tasks it serves, hashes per task
    (generated_sha), and planting it retires those tasks' trials and nothing
    else."""
    h = hashlib.sha256()
    h.update(_cached_tree_sha(str(generated["generator"])).encode())
    h.update(b"\0")
    h.update(generated["timeline"].read_bytes())
    visible = [e for e in _load_planted(generated) if e.get("changes_row_count")]
    h.update(json.dumps(visible, sort_keys=True, default=str).encode())
    return h.hexdigest()


def _task_planted_rows(generated: dict, task_id: str) -> list:
    """The planted.yaml entries naming this task, in file order."""
    return [e for e in _load_planted(generated) if task_id in (e.get("tasks") or ())]


def generated_sha(task: Task) -> str | None:
    """Hash of the generated data this task runs on: the shared inputs plus
    the rows planted in its name. None for worlds that generate no data,
    so nothing about existing worlds' hashes moves."""
    fx = task.world.generated
    if fx is None:
        return None
    h = hashlib.sha256()
    h.update(_generated_base_sha(fx).encode())
    h.update(json.dumps(_task_planted_rows(fx, task.id), sort_keys=True, default=str).encode())
    return h.hexdigest()


def generated_base_sha(name: str) -> str | None:
    """The shared generated-input hash for a world, by name. The bake step
    embeds it in the image build command (so a config change rebuilds the
    image) and writes it beside the baked artifacts; trial setup asserts the
    two agree, so a stale image can never serve a newer config."""
    world = load_world(name)
    if world.generated is None:
        return None
    return _generated_base_sha(world.generated)


def world_sha(name: str) -> str:
    """Content hash of a world's workspace tree (what the old corpus-repo SHA
    pinned). For a world with generated data, the shared generated inputs fold
    in — a timeline or generator change rewrites every table without touching a
    shipped file, and every reader of this hash (run metadata, the drift checks
    in score/backfill/report/browse) must see that as the world changing."""
    world = load_world(name)
    base = _chain_sha(world.workspace_dirs)
    if world.generated is None:
        return base
    h = hashlib.sha256()
    h.update(base.encode())
    h.update(_generated_base_sha(world.generated).encode())
    return h.hexdigest()


def workspace_sha(task: Task) -> str:
    """Content hash of the tree this task actually ships. Equal to
    world_sha(task.world.name) for world-backed tasks — existing trial-cache
    keys must not move — and the task's own tree when it carries one. A task
    overlay folds into the hash, so editing the overlay retires only that
    task's cached trials. In a generated world the task's generated_sha
    folds in the same way: the shared inputs plus the task's own planted rows,
    so a timeline change moves every task's hash and planting a row moves only
    the named tasks'."""
    base = _chain_sha(task.workspace_dirs)
    if task.overlay is not None:
        h = hashlib.sha256()
        h.update(base.encode())
        h.update(_cached_tree_sha(str(task.overlay)).encode())
        base = h.hexdigest()
    fx = generated_sha(task)
    if fx is None:
        return base
    h = hashlib.sha256()
    h.update(base.encode())
    h.update(fx.encode())
    return h.hexdigest()


def evidence_sha(task: Task) -> str | None:
    return _tree_sha(task.evidence_dir) if task.evidence_dir is not None else None
