# DQ-77 — the nightly test run has been red for two quarters

`plat_dbt_test_nightly` fails every night. Twelve tests fail in it and they have failed in it
since before I joined the rotation. We now close that alert without opening it, which means the
next real break arrives as a page nobody reads.

I want the nightly to mean something again. Red should mean a test found something.

There are two kinds of failure in the twelve and they want opposite treatment.

- Some of them assert a rule we no longer hold. Priya's April memo to the six teams replaced
  that rule and says what becomes of a test written before it. Retire or re-place each of those
  where the memo puts it, and where the memo moves a test rather than retiring it, say which
  model it moved onto.
- The rest are real. Two sources disagree about the same money and the test is right to say so.
  Those stay failing tonight. We are not fixing them this week and we are not hiding them.

There is a fast way to do this that makes everything worse, so:

- Every test gets its own decision, and the decision comes from what the test says, not from the
  colour it is. No blanket move — no muting the selection, no `exclude` on a step, no dropping
  the suite to warnings.
- The tolerance in a reconciliation tie came from a consumer contract. Do not move one.
- The nightly keeps its six team selections. Do not take a team out of the run.

Then write `RESPONSE.md` at the root of the working tree, the way `docs/change-management.md`
asks. It goes to the six team leads, so keep it short. Four things:

- how many of the twelve are stale and how many are real, as two numbers;
- all twelve named by test name, each under the heading it belongs to;
- for each stale one, what you did with it and the clause that says so;
- for each real one, which two sources disagree, and what it would take to settle it.

`dbt/README.md` has the run commands. Do not use `dbt build` on this tree — it stops at the
first failing test and takes a fifth of the warehouse down with it.
