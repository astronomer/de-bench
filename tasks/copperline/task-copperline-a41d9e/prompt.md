# PLAT-437 — the rotation check fails every week and tells nobody

`plat_secret_rotation_check` failed again this morning. It failed on each of the last three
Mondays. The first any of us heard about it was Finance asking why the Meridian key is still
the one we opened the account with.

The DAG is wired to notify on failure — J. Okonkwo set that up from the runbook when the
check went in. Whatever the wiring is doing, it is not reaching a person: no mail, no page,
nothing in the on-call notes, three failed runs nobody saw.

Work out why a failed run of this DAG reaches nobody, and make it reach somebody. The
platform rotation owns this DAG and is who should hear about it.

Two things are wrong here that are not this ticket, and the run stays red until both are
done:

- the overdue keys are SEC-118, with the vendors already;
- the check writes its record to `ops.secret_rotation`, which was never created in the
  warehouse — that is PLAT-429.

So leave `config/iac/connections.yaml` alone and do not go making the run green. A red run
that reaches the on-call is the whole point of this ticket.

When you are done the file should say one thing about how this DAG notifies, not two. Take
out whatever was not working.

Then write `RESPONSE.md` at the root of the working tree, the way `docs/change-management.md`
asks. It is read by the on-call, so keep it short. Three things:

- what was actually wrong — why a failed run reached nobody;
- what you changed, and what the on-call gets the next time this DAG fails;
- anything in the repo that told us to wire it the way it was wired, and what should happen
  to that.
