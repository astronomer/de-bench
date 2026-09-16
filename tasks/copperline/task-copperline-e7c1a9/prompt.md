# CUS-318 — support SLA has never produced a day, and no run has ever failed

Support asked for the SLA numbers for the quarterly review and there are none.
Not stale ones — none at all. `marts.support_sla_daily` has never held a row.

`cus_support_sla_daily` has one run in its history. It started the morning after
the deployment came up in January and it is still going. Nothing failed,
nothing retried, no alert fired and nobody was ever paged, which is why this has
sat for five months and nobody heard about it.

Two things, and the second one matters more than the first.

**1. Make the run able to get past its first step.** Work out why that step
never ends, and fix it. The fix goes in this DAG's own file, under
`projects/customer/dags/`. `include/lib/` is the platform team's shared code and
is not ours to change, and the team's loader renders every customer blueprint,
so a change there lands on DAGs this ticket never looked at.

**2. Make this impossible to miss again.** Five months of a DAG sitting in
`running` is what we are really fixing. When this job cannot do its work, the
run has to end by itself, and the failure has to reach the team that owns the
DAG. `CONVENTIONS.md` governs whatever you write.

Nobody expects the mart to appear this morning — the models behind it are a
separate job and the customer team is on them. Leave `dbt/` alone. What is
wanted here is a run that can reach them and a job that says so when it cannot.
