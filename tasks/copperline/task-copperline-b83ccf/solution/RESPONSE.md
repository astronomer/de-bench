# TRE-206: the gift-card escheat exposure

Escheat exposure: 106,935,742 cents

Not yet aged out: 644,089,266 cents

Both figures are read from `raw.gift_card_ledger` and `raw.gift_cards`, joined to
`raw.gift_card_jurisdictions` on the card's `jurisdiction_code`. REV-12 decides which
side a card falls on, and the column it names is `escheat_applies`.

## The breakage the ledger has written

The tender module writes a breakage entry for every card still holding value 24 months
after issue, for the value it still holds. The oldest card in the book was sold on
2024-02-04, so those entries start on 2026-02-04 and run to 2026-06-14. There are 18,286
of them and they carry 197,720,074 cents.

| Side | Cards | Cents |
|---|---|---|
| Escheats — the jurisdiction's `escheat_applies` is true | 9,949 | 106,935,742 |
| Recognizes as breakage revenue under REV-12 | 8,337 | 90,784,332 |
| **The ledger's whole breakage** | **18,286** | **197,720,074** |

20 of the 66 jurisdictions escheat and 46 allow breakage. The escheating 20 are 18 US
states plus Quebec and Alberta, so the whole exposure is USD and every entry on it books
to CL-US. No rate and no entity split moves this number.

The exposure is the balance left on the card, not what the card was sold for. 1,122 of
the 9,949 were spent down and broke for the stub; adding their face value instead gives
118,242,500, which is 11,306,758 cents of money that was already redeemed.

## By dormancy tier

| `dormancy_months` | Jurisdictions | Cents | First due |
|---|---|---|---|
| 36 | 13 | 71,125,259 | 2027-02-04 |
| 60 | 7 | 35,810,483 | 2029-02-04 |
| **Total** | **20** | **106,935,742** | |

Dormancy runs from the issue date. The oldest card in each tier was sold on 2024-02-04,
so the earliest 36-month balance comes due on 2027-02-04 and the earliest 60-month
balance on 2029-02-04.

**Nothing is reportable today.** Not one cent of the 106,935,742 has reached its
dormancy date as at 2026-06-15, because the whole card book is younger than 36 months.
The exposure is a liability we hold, not a filing we owe. The first filing falls in
February 2027.

## The largest jurisdiction

Tennessee, `US-17`, on a 36-month dormancy: 545 cards and 6,444,641 cents. Pennsylvania
holds more cards — 565 — and is ninth by money at 5,770,619, so a count of cards names
the wrong state. Quebec is the smallest at 1,923,266.

## What is still coming

88,105 cards in escheating jurisdictions have not yet reached 24 months, and they still
hold 644,089,266 cents: 423,985,238 in the 36-month tier and 220,104,028 in the 60-month
tier. That is what each card was sold for less what the ledger has taken off it. Added
to the 106,935,742 already written, the escheating cards are carrying 751,025,008 cents.

The ledger has written breakage for the first four months of issue cohorts. The other
two years of them age out between now and 2028.

## `raw.gift_cards.status` is not evidence

The OMS calls 11,770 cards `expired`. Not one of them has expired. Every card in the
book carries `expires_at` 60 months after its issue date, so the earliest expiry
anywhere in the book is 2029-02-04 and no card has passed its own expiry date. The
column is the OMS's own housekeeping word and it disagrees with the card it sits on.

1,007 of those cards are in escheating jurisdictions and have aged out. They carry
11,637,500 cents — about a ninth of the exposure. Anyone who filters them out because
the OMS calls them dead files 95,298,242 instead of 106,935,742. Read the ledger, not
the status.

## The verdict on the open item

**`escheat_applies` moves money. `dormancy_months` moves only a date.**

`escheat_applies` decides whether a balance goes to a state or to revenue. If it is
wrong on one row, that jurisdiction's whole figure crosses between the two — 6,444,641
cents at Tennessee, 1,923,266 at Quebec.

`dormancy_months` cannot move a cent of either. It sets the date a balance becomes
reportable and nothing else, and it cannot even change what is in a balance: no card in
this book is redeemed after its 24-month age-out — the latest redemption on any card
falls about 13 months after issue — so the value at 36 months and the value at 60
months are both the value at 24 months, which is what the ledger already wrote.

So the quarter spent reading statutes again buys us the filing calendar, and nothing in
the accounts. If the controller wants a column audited for the money, it is
`escheat_applies`, and the policy's own open item says treasury owns it.

Nothing in the pipelines was changed for this.
