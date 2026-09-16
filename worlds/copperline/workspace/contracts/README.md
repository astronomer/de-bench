# Consumer contracts

One pair of files per consumer: `<consumer>.md`, the prose contract, and the `.yml` beside it, the machine schema. `docs/report-registry.md` says which pair belongs to which consumer.

## CON-1 The prose and the schema

The yml is enforced and the prose governs meaning. `plat_contracts_enforce` runs the yml against the mart every morning; nothing runs the prose. Where the two disagree, that is a bug in one of them — open a ticket and fix the pair, rather than reading either one as the winner.

## What a contract is for

A contract is what the consumer's owner and the team that builds the model have agreed. It binds both ways. It says what the output is — grain, columns, meaning — and what it is not, and `docs/change-management.md` §CM-1 says a ticket cannot override it.

Contracts govern presentation. They do not govern recognition, comparability, valuation or retention; the policy documents under `docs/` do, and a contract that appears to contradict one of them is stating an exclusion, not a rule.

## Writing one

Three load-bearing clauses is the shape. Grain and columns, the exclusions or carve-outs the consumer asked for, and whatever this consumer will get wrong if it is not written down. Longer contracts do not get read.

Name the yml in the first line. The registry is regenerated from these files and the pairing is how it finds them.
