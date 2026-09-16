# TRE-206: what do we owe the states on gift cards?

Treasury has never had a gift-card number it could stand behind. The workbook we file
unclaimed property from is built by hand off the OMS card export, it counts cards
rather than money, and when the controller asked in February what the states are owed
it took a fortnight and came back with two answers.

The card book is in the warehouse and so is every entry ever made against a card.
Work the exposure out of those, and the next filing has a figure with a derivation
behind it instead of a spreadsheet.

There is a second reason this lands now. REV-12 in `docs/finance-policy.md` sends a
card in an escheat jurisdiction to the state instead of to revenue, and the open items
at the foot of that document say `dormancy_months` on the escheat rows of
`raw.gift_card_jurisdictions` has not been checked against the statutes since FY2025.
Reading sixty-six statutes again costs a quarter of somebody's year, and before we
spend it the controller wants to know what that column is worth — what moves, and by
how much, if it turns out to be wrong.

Dormancy runs from the card's issue date. That is what the statutes we hold count
from, and the file carries no other anchor.

Today is 2026-06-15.

## What to write

`RESPONSE.md` at the root of the working tree. Integer cents throughout, and every
figure positive: `raw.gift_card_ledger` is a liability ledger and the signs on it are
the liability's, not ours.

- **The exposure the ledger has already written**, on a line of its own, written
  `Escheat exposure: <cents> cents`. Cards old enough to have reached the age REV-12
  breaks them at already carry the entry; this is the part of it that goes to a state
  rather than to revenue.
- **The rest of the breakage in the ledger, and the two added.** The auditors ask
  what was written, not what we kept, so both halves and the whole have to be
  readable.
- **The exposure split by dormancy tier** — one figure for each distinct
  `dormancy_months`, and how many jurisdictions sit in each. The tiers add to the
  exposure.
- **The first date each tier comes due**, and how much of the exposure falls due to be
  filed on or before today.
- **The balance still on escheat-jurisdiction cards that have not aged out yet**, on a
  line of its own, written `Not yet aged out: <cents> cents`. That is the filing
  calendar for the years after this one, and nobody here has seen it.
- **The single jurisdiction carrying the most**, by name and by figure.
- **What `raw.gift_cards.status` is worth for this.** Say what it claims, whether the
  card book itself agrees with it, and how much of the exposure sits on the cards it
  calls expired.
- **The verdict on the open item**: of the columns on `raw.gift_card_jurisdictions`,
  which one moves money and which one moves only a date, and why. That is the answer
  the controller is buying the quarter with.

Keep it to a page. It goes to treasury and to the controller's office, and both of
them read the first line and the last.

This is a question, not a change. The jurisdiction table is treasury's own and we do
not edit it; nothing in the pipelines moves for this.
