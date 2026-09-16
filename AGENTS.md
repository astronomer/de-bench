# Working on de-bench

This is for whoever is working **on** the benchmark. It is not shipped to anything
being measured.

Do not confuse it with the other `AGENTS.md` files here. Those sit inside
`worlds/*/workspace/` and are read by the agent under test — they are world
content, part of the fiction, and nothing about the benchmark itself belongs in
them.

## Use these words

`README.md` defines them and is the source. Pick the term and use it every time;
do not reach for a synonym to keep the writing lively, and do not invent a new one
for something that already has a name.

| Word | What it means |
|---|---|
| world | the project an agent works in — a directory under `worlds/` |
| team project | `projects/<team>/dags/`; one team owns one project |
| task | one ticket, its checks and its grading |
| trial | one agent, one task, one attempt |
| config | an (agent, model, thinking) triple |
| check | one graded assertion |
| expected result | what a check compares against, computed from the fixtures |
| solution | the known-good fix a task keeps beside it |
| flaw | something wrong with a world on purpose, listed in that world's `PLANTED.md` |
| find-task, build-task | a task that must locate something first, against one told where to work |
| run | a stored set of trials under `~/.de-bench/runs` |

Words that were removed for being pictures rather than descriptions: **rung**,
**ladder**, **spine**, **surround**, **control**, **treatment**. Say what changes,
not what it resembles.

## What is expensive to get wrong

**Changing a world's tree retires every stored trial in it.** `world_sha` hashes
`worlds/<name>/workspace/`, and the trial cache keys on it. Batch those changes;
never make them one at a time.

Editing a task's `prompt.md` or its checks retires that task's trials only, which
is much cheaper.

**Worlds hold source only.** No `__pycache__`, no `logs/`, no dbt run junk. A test
enforces it. Set `PYTHONDONTWRITEBYTECODE=1` before loading a world locally.

**A world's `PLANTED.md` never ships.** It sits beside `workspace/`, not inside it.
It is the answer key: a fault listed there is furniture, an unlisted one is a bug.

**A duplicate `dag_id` breaks the whole world.** Airflow 3 raises and records an
import error, and every `dag_parses` check asserts the DagBag has none — so one
collision fails every task in that world, not one DAG.

## Checking your work

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m pytest tests/ -q
python tools/check_world.py <world>          # unpacks the shipped tar, builds a DagBag
de-bench prove --world <world>               # solution passes, untouched tree fails
```

`prove` only speaks for tasks that ship a `solution/`. Every task in both worlds
does.

## Where things are

```
worlds/<name>/world.yaml     metadata; `extends` makes it a size of another world
worlds/<name>/workspace/     the tree the agent gets (an overlay, if it extends)
worlds/<name>/PLANTED.md     the answer key, never shipped
tasks/<world>/<id>/          task.yaml, prompt.md, checks.yaml
configs/*.yaml               named (agent, model, thinking) sets
docs/judge.md                how the prose judge grades written answers,
                             with real fact keys and verdicts
docs/metrics.md              what we report (Pass@1, Pass^3) and how the
                             field's benchmarks name the same quantities
docs/authoring.md            how to write a task and its checks
docs/copperline-spec/        the design of the copperline world
~/.de-bench/runs             stored runs, outside every checkout
```
