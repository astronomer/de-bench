# GTM-77 — four markets we opened, and nothing reports on them

We opened BR, MX, PL and ID in the first quarter. They have been trading for
months. The pricing mart shows none of them, and merchandising has asked about
it twice.

The market seed the pricing models build from still holds the five markets we
started with. Put the four new ones in it.

Two things have to hold when you are done.

- Every row in the seed carries the market's billing currency and the legal
  entity it bills through. Both have to be right. A wrong currency in that file
  is silent: the models build, the mart fills, and the numbers are wrong by a
  whole exchange rate. Nothing downstream will catch it.
- A job rebuilds the seed every Monday morning. Whatever you leave in the repo
  has to be what that job writes. If Monday's run puts the old list back we are
  where we started, and nobody looks at this again for a quarter.

Two trees are not ours this week. `fixtures/` is the market-setup team's export
and we do not edit their record. `include/lib/` is the platform team's shared
library and this is a market list, not a machinery change.
