"""The copperline world's data generator.

Runs harness-side at trial setup — never ships inside the world — and writes
the whole warehouse (`raw.*`, `ops.*`) plus the dated landing tree from two
config files that live beside the world's workspace:

    timeline.yaml    the company's history: seed, date range, eras, volumes,
                     calendars, late tails
    planted.yaml     the deliberately placed graded rows, each naming the
                     policy clause it tests and the tasks that grade it

Three rules make the output trustworthy, and tests enforce each:

1. No wall clock. Every "now" comes from `today` in timeline.yaml.
2. Per-day independent streams: every row derives from
   hash(seed, table, ds, i), so regenerating one day never moves another,
   and adding a table never moves an existing one.
3. Facts derive from the simulated upstream (`sim_*` schemas, dropped before
   the generator exits), never from marts. `staging` and `marts` are what the
   pipeline under test produces.

The upstream simulation is specified in docs/copperline-spec/02,
the extracts in 03, this generator's own contract in 07.
"""
