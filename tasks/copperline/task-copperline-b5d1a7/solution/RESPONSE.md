# PAY-276 — the attempt pick, and what the ambiguity flag is really saying

**The flag.** `payment_match_is_ambiguous` is true on 206,992 of the 914,694 orders that match
any attempt at all. The model's note says it means more than one order shares the reference in
the window; the code says `candidate_count > 1`, and `candidate_count` counts the attempts in
the window, not the orders on the reference. So it is not a warning about the recycled
reference. It says the order raised more than one attempt — a retry, or the same attempt number
twice — which is a normal card journey and not an exception.

Nothing in the estate is ambiguous the way the note describes: **0 orders**. The recycle cannot
reach into the window. `OE-` references come back round in date order and the closest two live
orders sharing one sit **50 days** apart, against a window of 15 days — the order's date plus or
minus seven. No attempt in the book is a candidate for two orders, so the reference never
attaches a payment to the wrong order here. The flag has been feeding the exception queue a
fifth of the matched book for a year and none of it was ever the thing the queue is for.

**What moves.** The pick ranks the candidates on day gap, then success before failure, then
attempt number, and stops. That is not a unique key. **59,947 orders** carry two attempts that
rank level on all three — same day, same outcome, the same attempt number raised twice minutes
apart — and `row_number()` takes whichever the engine hands it first, so the order can come back
with either intent from one build to the next.

**What it costs.** `int_payment_matched` joins Meridian on `payment_intent_id`, so the money
follows the pick. On **58,960** of those 59,947 the two attempts net different Meridian money,
and `settled_net_cents` moves with the pick. Added over the book the most it can differ between
two builds is **807,149,253 cents**. That is the number finance signs on the first of the month
and cannot reproduce on the second.

**The worst day.** **2025-11-28** — Black Friday — with **6,236** of the 59,947 on it. The
volume spike put the duplicate attempts through in a block, so a single day carries a tenth of
the whole population; 2025-12-01 is next at 873 and no other day reaches 250.

**Which columns move.** Only two: `payment_intent_id` and `payment_attempted_at`. The tied
attempts share the day, the outcome and the attempt number by construction — that is what makes
them tie — so `payment_outcome`, `payment_attempt_no`, `is_paid`, `has_payment_attempt` and
`payment_match_is_ambiguous` are the same whichever attempt wins. Downstream, the flap reaches
`settled_net_cents` and the Meridian columns of `int_payment_matched` and `marts.fct_payments`
through the intent id, and nothing else.

**The fix.** `int_orders_enriched` now ranks on `created_at` and then `intent_id` after the
existing three. The first three still decide the match and no order that already had a clear
winner changes; the last two only settle which of two equal rows we write down, so the answer
is the same on every build. Re-cutting the flag to mean what its note says is a separate
ticket — the honest reading of it today is "this order attempted more than once".
