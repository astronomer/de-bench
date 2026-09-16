# SC-1512 — Kestrel have had no file since January, and we have been chasing the wrong team

Kestrel Outdoor rang on Friday. They have not had a sell-through file since the
new year and they have been planning production off their own till receipts.
They are a vendor on a partner-share agreement, so this is a commitment we have
been quietly missing every week for five months.

`sc_partner_share_kestrel` has a run for every Friday since the scheduler moved
in January. Every one of them failed at `wait_for_sell_through`, six hours in,
after two retries. We raised it twice as "the sell-through export is late" and
both times it went to the team that builds the mart, who came back saying the
mart is built. Nobody went further than that.

Three things, and the third one is the one I actually want.

**1. Make the Friday run able to reach its delivery.** Work out what that step
is waiting for and put the wait on something that turns up. It has to be
answerable out of the warehouse — ask the mart whether the week this run is
delivering is there — and not off another DAG's run state: the build is daily,
we are weekly, and `plat_asset_republish` says in its own docstring why we do
not couple to how that build names things. Do not add an export to feed the
wait; nothing else reads a file like that, and a second producer is one more
thing to go missing. The wait still has to end by itself, and when it gives up
the failure still has to reach us. `CONVENTIONS.md` governs whatever you write, and
`include/lib/` is the platform team's and is not ours to change.

**2. Make the two documents true again.** `docs/report-registry.md` and
`docs/lineage.md` are written together and both of them describe how this
consumer is coupled to the mart. The registry also lists it as one of the
consumers nothing can find. If the coupling moves, they move with it — REG-2 and
LIN-1 are the rules and they are the reason the next person who changes that
mart will know we exist.

**3. It still will not deliver, and I want that written down rather than
patched.** Past the wait, the step that cuts the file cannot run either, and
that one is not about waiting. Find it, work out how far it goes, and how far
it can be fixed from inside this repository, then write `RESPONSE.md` at
the top of the repository: what it is, what in the tree shows it, and who has
to settle it before Kestrel get a file. Do not guess your way through it —
`contracts/partner-share.md` PS-2 says a conflict is raised and not decided
alone, and that is a house rule about more than redaction. The share is for one
vendor and it stays scoped to that one vendor; the columns in PS-1 do not move.
