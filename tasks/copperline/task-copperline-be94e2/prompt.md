# SC-1477 — the legacy nightly has not loaded a night since April

The quarter-end pack came back with the old supply-chain reports flat since the
spring. They are flat because nothing has loaded them. `ops.load_control` has
not recorded a complete night for the Pentaho jobs since the first week of
April, and the nightly is still red this morning.

Nobody changed the Airflow side. The deployment has driven those jobs the same
way since wave 1 cut over in January and nothing under `projects/supply/` has
been touched since.

Two constraints, both firm:

- The PDI box is frozen. Wave 2 has no date and no owner, and nobody is logging
  into that box to change its settings — it is being decommissioned, not
  maintained. Anything the job needs has to come out of this repository.
- `legacy/` is read-only, as `CONVENTIONS.md` says. Read the job and its
  transformations. Do not edit them, and do not add anything to them.

Do two things.

**1. Make tonight's run load again.** The fix goes in the DAG that drives the
job, written out in that file — the name the job reads and the value it needs,
both spelled out, so the next person to open it can see what the job is handed.
Keep one Airflow task per step of the job, and keep the check on the load
context: a night that cannot resolve one must still stop rather than load on a
guess.

**2. Write the incident note.** `ops/incidents/2026-06-15-legacy-nightly-failing.md`,
in the shape of the notes already in that folder. Answer all six of these:

- when it started, and which night was the last one that loaded;
- which step stopped producing what, and where that step used to get it from;
- the value the load context needs, and what in the data says it is that value;
- the control table does not look the same across the whole year. Say what else
  changes in it, and why that is not this;
- what you ruled out on the Airflow side, and why it could not have caused this;
- what has to be re-run to repair the nights we lost, and what closes them off
  afterwards.

Keep it short. Supply-chain read it at stand-up.
