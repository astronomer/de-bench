# PLAT-437 — why the rotation check told nobody

## What was wrong

`plat_secret_rotation_check` set three parameters in `default_args`: `email`,
`email_on_failure` and `email_on_retry`. Nothing in this deployment reads them.
Airflow applies a `default_args` key only where the operator's signature has a
parameter of that name, and on the pinned runtime these three are not operator
parameters, so they were taken, dropped and never acted on. No mail was ever
sent and no record of the failure was ever written. Three failed Mondays
reached nobody because the DAG asked for a kind of alert this platform does not
have.

The failures themselves are real and recorded. `include/data/_manifest/
plat_secret_rotation_check.json` holds a failed entry for every run. Nothing
turned one into a page.

## What was changed

One line in `projects/platform/dags/plat_secret_rotation_check.py`:

    "on_failure_callback": notify("platform", "a credential is past its rotation date"),

`notify()` comes from `include/lib/notify.py` and is what the other 91 DAGs in
the six team projects use. The three email parameters are gone, so the file now
says one thing about how this DAG notifies.

The next time a run fails, the platform rotation is paged with the summary
above, and a line lands in `include/data/_notifications/plat_secret_rotation_check.jsonl`
carrying the team, the task, the try number, the run and the first line of the
exception. That log is how the on-call answers "what woke us last week".

The run is still red, as SEC-118 and PLAT-429 leave it. `config/iac/connections.yaml`
is untouched.

## The runbook

`ops/runbooks/alerting.md` is where the email parameters came from. Its review
date is 2025-06-30, which is 350 days ago, so CM-4 makes it out of date by
default; CM-3 makes the code canonical where the two disagree, and the code is
`notify()`. `CONVENTIONS.md` rule 11 says the same, and `contracts/alerting.md`
already asks the reader to check this runbook's review date before following
it.

The runbook needs rewriting around `on_failure_callback=notify("<team>")`: the
parameter table, the address table and the testing step are all about a mail
path that does not exist here. It is platform's document, so it is ours to fix.
Until it is, anybody wiring a new DAG from it ships a DAG that alerts nobody,
which is what happened here.
