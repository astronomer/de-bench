# Metrics, and how the field reports them

What de-bench reports, what the two nearest benchmarks report, and how the
names line up. Written 2026-08-24 from the published sources; re-check the
sources before quoting them, they revise.

## de-bench

Three trials per cell. The headline is **Pass@1**: the mean single-attempt
pass rate, averaged across the three trials — equivalently, the fraction of
all trial cells passed. **Pass^3** is the stability floor: a task counts
only if all three trials pass. **Pass@k** (at least one trial passed) is
reported in `RESULTS.md` for completeness but never headlined: it rewards
variance, and nothing else in the field quotes it. Cost is mean dollars per
trial through the LLM gateway; the pareto plots Pass@1 against cost per
trial, harness by colour, model by marker.

## Snowflake data-eng-bench

103 dbt tasks (DuckDB + Snowflake), three trials per task — the same
protocol shape as ours, and the vocabulary we adopted:

- **Pass@1** — "mean single-attempt pass rate (averaged across three
  attempts)". Identical to our definition.
- **Pass^3** — resolved only when every assertion passes in all three runs.
  Identical to ours.
- Cost per trial, a cost multiplier against their CoCo baseline, and total
  tokens per trial.
- Behavioural metrics: mean agent steps, tool operations by phase, SQL
  queries issued, file and shell operations.
- Headline chart: Pass@1 against cost per trial, up-and-left better.

A pass requires every hidden assertion green after dbt materializes the
pipeline — deterministic grading, no judge. Reference points from their
table: CoCo + Opus 5 at 73.8% Pass@1 / 64.1% Pass^3; Claude Code + Opus 5
at 69.6% / 60.2%.

Their rates are comparable to ours metric-for-metric; the costs are not,
since their tasks run on a different image and grade through pytest.

## Databricks' coding-agent benchmark

The benchmark that made pi's name: real merged PRs from their multi-million
line monorepo (Python, Go, TypeScript, Scala, Rust, protobuf, Bazel).

- **Task completion rate** — when the agent declares the task complete, the
  code is checkpointed, held-out tests are patched in, and the tests decide
  the pass. No LLM judge.
- **Cost per task** in dollars, **token consumption**, and **context re-fed
  per turn** — the harness-efficiency angle their write-up centres on (pi
  sent ~3x less context per turn than Claude Code and Codex at the same
  quality).
- No published pass@k, no stated trial count, no variance methodology.

## The rule this sets for de-bench

Quote **Pass@1** as the headline, name **Pass^3** beside it when stability
matters, and always attach the harness to the model — every one of these
benchmarks, ours included, measures the pair, and the harness moves the
number as much as most model swaps.

Sources: Databricks, "Benchmarking Coding Agents on Databricks'
Multi-Million Line Codebase" (databricks.com/blog); Snowflake, "A Data
Engineering Benchmark for AI Agents" (snowflake.com/en/blog/engineering)
and Snowflake-Labs/data-eng-bench on GitHub.
