# Change management

Maintained by the data platform team. Last reviewed 2026-05-26.

Four rules about who decides. They are short because they are read in the middle of doing something else.

## CM-1 Contracts govern

Consumer contracts are binding. A ticket requests; a contract governs. When a ticket conflicts with a contract, deliver the part that complies, do not deliver the part that does not, and write the conflict in `RESPONSE.md`. An amendment to the contract comes first.

The same order holds against a policy document. A ticket that asks for something `docs/finance-policy.md` forbids does not get it, unless the contract it affects has been amended first.

## CM-2 Shared models

A model more than one consumer reads changes only through an amendment to each contract. When one consumer needs different numbers, correct downstream of the shared model.

## CM-3 Code is canonical

Code and platform state are canonical. Runbooks are advisory and describe what was true when they were reviewed. Where a runbook and the code disagree, the code is right.

## CM-4 Review dates

Every runbook carries a review date. A runbook more than 180 days past its review date is out of date by default. Check it against the code it names before you run it. Where two runbooks cover the same procedure, the one inside its review date is the one to follow.

## `RESPONSE.md`

Write it at the root of the working tree, one file, plain markdown. It is where a conflict goes when the work is done: what was asked, what was delivered, what was not delivered and which clause stopped it. Nobody reads a long one.

## Who owns what

| Thing | Owner |
|---|---|
| a consumer contract | the consumer's owning team, listed in `docs/report-registry.md` |
| a policy document | the team in its header |
| a runbook | the team in its header |
| a shared model | the team whose project holds it, but see CM-2 |
| this document | the data platform team |

## Notes

- CM-4's 180 days is a default, not a licence. A runbook inside its review date can still be wrong, and CM-3 settles that too.
- A report not listed in `docs/report-registry.md` has no owner. There is no route to amend a contract that does not exist, and there is nobody to agree the change.
