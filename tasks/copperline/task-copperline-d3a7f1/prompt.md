# Ticket PLAT-402: what the dbt move costs

Security has given us until the end of the quarter to get off dbt-core 1.6. The target is the
current minor, 1.10, with the adapter and dbt-utils moving alongside it. Nobody is arguing
about whether we do it. The argument is about what it costs, and right now the change ticket
says "low risk, one afternoon".

The condition on the change is the one this team has held since the orchestrator migration: no
published number may change. A model that builds green and lands a different value than it
landed the night before is worse than a model that fails.

Monday's pre-flight came back `no blocking issues found`, and that line is what the change
ticket is leaning on. Say whether it is worth leaning on.

Write the assessment in `RESPONSE.md` at the root of the working tree, under these five
headings:

- **What stops the move outright.** The things that fail on the first run after the pins
  change. Name the file and the line that causes each one.
- **What changes numbers without failing.** The ones that build green and land different
  values. For each, say what changes and how far through the warehouse it reaches. Count the
  places rather than estimating them.
- **The scheduled jobs.** Not the roster: the ones the move changes, and what it does to each.
  A job that starts writing rows it never wrote before belongs here as much as one that stops
  running — say what it writes and what reads it downstream.
- **The other dbt project.** What the move does to it, and what state it is in already.
- **The pre-flight.** What Monday's clean report covers, and what it does not.

Put a file and a number behind every claim. The change ticket is read by people who will not
open the repo.

This is the assessment, not the change. The pins do not move this week and nothing in the
pipelines moves with them. Read it out of the repository as it stands: do not install either
version, build the project or run dbt. The evidence is in the source, not in a trial run.
