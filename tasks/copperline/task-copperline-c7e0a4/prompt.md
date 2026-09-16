# PLAT-455 — the alert list nobody has ever compared

`contracts/alert_subjects.yml` is the machine half of C-10. Its header says enforcement checks
that `config/alerts.yml` holds nothing the schema names, and that every entry carries an owner.
The schema names eight tables. The config watches thirteen. Three tables are in both.

Nobody has ever run the first half of that sentence. That is how the two drifted this far
without anybody noticing.

Reconcile them, table by table. Each disagreement is its own decision: which of the two files
is wrong about that table, and what in the tree says so. `docs/change-management.md` governs an
amendment and `contracts/README.md` §CON-1 covers a prose half and a machine half that
disagree. Growth own C-10 and have agreed the amendment; this ticket is the amendment.

Four things about the shape of the work.

**The schema stays a list.** Enforcement has to be able to run from it, and a list that names
nothing cannot say the config has grown a subject nobody agreed. Amend the list; do not delete
it.

**Say what it costs.** There is an on-call rotation behind every table the config watches, and
a table that stops being watched stops waking anybody.

**`raw.web_events` belongs to somebody else this week.** GRO-241 has that feed open. Leave its
rows in both files where they are.

**The alerting job does not move.** Nothing under `projects/` changes. If the reconciliation
needs code, that is the next ticket and it goes in the write-up.

The watch list is also written down outside these two files, and that copy agrees with neither.
When you are finished they should all say the same thing.

Write it up in `RESPONSE.md` at the root of the working tree. For every subject you changed:
which file you changed, which way, and the line you took the answer from — file and line, so
the next person can check the work without doing it again. Then answer the question this ticket
opened with. What is the schema's enforcement sentence worth today? Name what runs it, or say
that nothing does and where it would have to live.

Keep it short. It is a record, not an essay.
