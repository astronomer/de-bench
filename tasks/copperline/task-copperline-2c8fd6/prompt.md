# PLAT-509 — the contract run has been quiet about `order_economics` since March

`plat_contracts_enforce` has come back green on `order_economics` every morning since
March, and nothing was amended in March. `marts.order_economics` publishes a `channel`
the contract does not allow, and it has published it since the trade book moved onto the
order spine. The alert stopped instead.

Commerce has settled the argument about the mart: the mart is right. That is a third of
the book and it is not coming out of a merch dashboard. The contract is the half that is
behind.

Three things.

**Finish the amendment, and put it where it is read.** After this the morning run says
nothing about `order_economics` because the contract states what the mart publishes —
not because nothing is looking. Take the channel list from what the business sells rather
than from whichever document you find first; several of them are as old as the contract
and still say three. `include/lib/contracts.py` is in the platform team's next release
and is not open this week, so read what it reads before you write: a clause it does not
look at is a clause that enforces nothing, which is the state we are in already.

**Put the morning run back.** A channel nobody agreed still has to come back as a
violation the next morning. That is all there is between the mart and a typo in an
upstream feed, and it is not this contract's guard alone — five contracts pin a list of
allowed values and none of those lists has been enforced since March either.

**One rule, one record.** The same stale list is asserted a second time in the dbt suite,
against `channel_mix_daily`. Once the contract holds the list and the morning run runs
it, a dbt test that holds it too is a second copy of one rule — and it is the copy that
went stale. Retire it: the file comes out of the suite. Do not settle it by moving the
list along one channel and leaving it there.

What does not move. The grain, the columns and the three assertions under them are what
C-4 built on, and `contracts/merch-dashboards.md` MD-1 and MD-2 say what breaks if any of
them changes. Nor does the mart: `marts.order_economics` publishes what the order book
holds. Comment fixes are not required — the stale ones go with the platform release.
