# Supply-chain conventions

These are supply-chain's house rules. They govern everything under
`projects/supply/`. Where they and the workspace `CONVENTIONS.md` differ, this
file wins inside this directory and nowhere else.

Style only. What a DAG must do about correctness is in `docs/`, in `contracts/`
and in the library's docstrings.

## Names

Every `dag_id` starts `sc_`, and the cadence goes at the end:
`sc_inventory_snapshot_daily`, `sc_shrink_weekly`, `sc_carrier_scan_intake`.
Subject in the middle, cadence last, so an alphabetical list groups by subject
and a person scanning it can see what runs how often.

The file is named for the DAG it holds. One file, one DAG. `blueprints.py` is
the one exception and holds only the loader.

A task id is the step it runs. Where a task drives a legacy artifact, the task
id is the artifact's own step name — `set_constants`, `load_inventory`,
`history_complete` — so the graph reads like the job it replaced.

## The legacy estate

Anything that touches the old estate goes through `include.lib.legacy_runner`,
**one step per task**. A step that fails retries alone, and the graph shows
which step it was. A task that runs a whole job hides seven steps behind one red
square, and half of what supply owns is still that job.

Pass the variable block forward, exactly as it comes back. Do not add a variable
the job did not set and do not resolve one twice.

`legacy/` is read-only text. Nothing edits it and nothing executes it directly.

## Operators

Classic operators with `retries` written out, plus the runner. The house
decorator `@lake_task` is used where a batch can be rejected and the run should
carry on — a rate that has no card, a scan for a lane we do not know. Everywhere
else, a plain operator.

Map over locations freely: a distribution centre list is data and a map is the
honest shape for it. Set `map_index_template` so the UI names the location
rather than the index.

## depends_on_past

**Every DAG that maps over locations sets `depends_on_past=True`**, and says why
in its `doc_md`. A movement processed out of order corrupts the on-hand
position, and one bad night then blocks the nights after it — which is the
behaviour we want, because the alternative is a position nobody can trust and
nothing that says so.

Do not set it anywhere else. On an independent daily load it turns one bad day
into a week-deep queue for no gain.

## Paths

Supply keeps its own `projects/supply/lib/paths.py` and uses it for partition
paths and for the landing tree. `include.lib.warehouse` is still the only way
into the warehouse and the only thing that writes a partition; `paths.py` is
where this team's file names are written down, in one place, so a name is
changed once rather than in five sensors.

## The DAG header

Every DAG carries, written out rather than defaulted:

- a module docstring, fed to `doc_md`, saying what it produces and who reads it;
- `schedule`, `start_date` as a fixed UTC literal, and `catchup`;
- `max_active_runs=1` wherever the DAG writes a shared table;
- `tags` naming the team and the layer;
- `default_args` with `owner`, `retries` and `retry_delay`, and nothing else;
- `on_failure_callback=notify("supply")`.

Two retries is the default. Anything above three carries a comment saying what
it is waiting out. A step over SSH to the old box gets three, and the comment
says the box.

## Where things go

| Kind of thing | Where |
|---|---|
| the graph | `projects/supply/dags/<dag_id>.py` |
| a step's logic | a module-level function in the same file, or `projects/supply/lib/` |
| a file name or a partition path | `projects/supply/lib/paths.py` |
| a rendered DAG | `projects/supply/dags/<name>.dag.yaml` |
| a supply-only blueprint kind | `projects/supply/dags/kinds.py` |

`include/lib/` is the house library and it is protected: drive it, read it,
never rewrite it.
