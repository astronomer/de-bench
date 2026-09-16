# How the prose judge works

The judge grades the written halves of the benchmark — the `file_contains`
checks on RESPONSE.md — by reading substance from an authored fact key
instead of matching regexes. This page walks the pipeline with real
artifacts. `src/de_bench/judge.py` is the implementation and `tools/gen_judge_facts.py` writes fact keys for new
tasks.

## The problem it solves

A prose check used to be a regex like `payouts?[^\n]{0,80}monday` — the two
words within eighty characters on one line. An agent that wrote the correct
fact as

> "Every marketplace payout and every settlement line in the warehouse is
> dated on a\n**Monday**"

failed it, because markdown wrapped the line between the words. Three audit
waves (findings 010-012, 014) each found a fresh crop of this class:
correct answers convicted on spelling, and — the mirror image — wrong
answers passing through loose alternations. A regex cannot read negation,
attribution, or a hedge. A reader can.

## Step 1 — the answer key

Each task with prose checks ships `judge_facts.yaml`, generated from the
old patterns plus the author's rubric comments, calibrated until the task's
solution passes it, then hand-reviewed and committed. It is answer-key
material and gets the same review a wrong-answer battery gets. A real
entry, from task e030d8 (the guest-orders footnote):

```yaml
- seq: 0
  groups:
  - any_of:
    - States that guest / accountless orders make up a share in the 14-18% range (e.g. 16.3%)
    - Gives a guest-order count and total order count whose ratio is between 14% and 18%
  - any_of:
    - Mentions the member_orders column in any capacity - e.g. as the denominator of repeat_order_share_bps
  - any_of:
    - Says a guest order is counted in the guest_orders column
  - any_of:
    - Says a guest order is counted in the order_count column
```

The semantics mirror the regexes they replaced exactly: **every group must
be satisfied; any one fact inside a group suffices** (each pattern was
required; `|` alternates inside a pattern were an OR).

Translation has known failure modes, each of which convicted a correct
answer in trials before review caught it: demanding an attribution the
check never graded, facts about the pre-fix world convicting statements
about the answer's own fix, turning one pattern's alternation into several
required groups, requiring a date range's endpoints to be re-enumerated,
and overfitting to the solution's citation style. Read a generated key
against that list before committing it.

## Step 2 — the call

At scoring time the judge sends one message to sonnet: a system prompt with
the grading rules, then the fact groups and the delivered RESPONSE.md. The
rules are the audits' lessons distilled:

- a figure counts only at its exact value — a close figure is a different
  figure (21 Sundays is not the graded 23);
- polarity and attribution matter — "23 Fridays" does not state "23 missing
  Sundays";
- hedges do not count — "most likely a matcher on the domain" is a guess;
- any wording, table, layout, or line wrap counts.

The reply comes through a forced tool call, **finding before boolean** —
when the boolean came first, the model decided before it reasoned:

```json
{"patterns": [{"n": 1, "finding": "<quote from the answer, or what is missing>",
               "satisfied": true}, ...],
 "verdict": "pass"}
```

The harness recomputes the verdict from the group booleans and ignores the
model's own summary field — that killed a failure mode where the model
wrote "actually this satisfies the requirement" in its reasoning and still
emitted fail. Groups are read positionally, not by the model's numbering,
which proved unreliable.

## Step 3 — the stored verdict

Every verdict lands in score.json the way a regex's matched pattern used
to, findings included, so a scored run carries the judge's reasoning. A
real pass (pi-sonnet, e030d8 t1 — a cell the regex had convicted):

```
judge claude-sonnet-5+claude-opus-5 v3, 1 call(s):
group 1: ok — 16.3% of orders (4,184 of 25,617) carried no account at all.
group 2: ok — `repeat_order_share_bps` is divided by `member_orders` — the members' own orders
group 3: ok — A guest order reaches two columns only: `order_count` and `guest_orders`.
group 4: ok — A guest order reaches two columns only: `order_count` and `guest_orders`.
```

A real conviction (pi-opus, a6d2f8 t1 — wrong figures the regex had passed):

```
group: MISSING — The answer states 21 Sundays and 3 Mondays (24 total) where
the graded count is 23; a close figure is a different figure.
```

## Step 4 — the conviction policy

- A **pass** stands on one sonnet call.
- A **conviction** must be confirmed by **opus** — a different model on
  purpose. Single calls carry ~1-3% noise on borderline cells and the noise
  is correlated: on one cell sonnet twice in a row wrote a finding that
  quoted the answer stating the fact and still set satisfied to false.
  Same-model re-asking cannot catch that; opus does not share the
  correlation.
- A split goes to a third call, majority rules. In the live trials this
  outvoted the one sticky misfire and agreed with the hand adjudications
  everywhere else.

## What is deliberately not in the loop

- **The wrong-answer lists never reach the judge.** Every runtime channel
  for them misfired — reported as extra rows they read as missing groups;
  as a dedicated field the model over-applied them, once convicting a
  solution for stating the correct figure instead of a listed wrong one.
  The measured wrong answers are all convicted by group failures plus the
  hedge rule, so `convicts` stays in the YAML as authoring documentation.
- **Regex is never a fallback.** A judge outage is a visible check failure.
- **Everything else stays deterministic** — figures, verifiers, DAG and
  file checks are untouched. The judge grades one thing: does the answer
  state the fact.

## The measurement behind it

The shipped contract scores 188/188 against a hand-verified evaluation set
(169 correct answers, 19 wrong ones drawn from three audits and the
adversarial batteries), with zero errors and repeat-stable verdicts. Cost
runs ~2.6k input / ~250 output tokens per check on sonnet, with opus spent
only on conviction confirmations.
