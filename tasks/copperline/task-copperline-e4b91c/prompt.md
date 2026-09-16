# TAX-241 — the output tax on the trade book, and which of the two numbers it is

The group auditors have asked for the output tax the trade invoice book charged, with a
reconciliation behind it. The desk has pulled the number twice and come back with two answers.
`raw.invoices` carries a `tax_cents` on the header. `raw.invoice_lines` carries a `tax_cents`
on every line. The two do not agree, and nobody here can say which of them belongs on the
working paper.

Last year we told the auditors that the ERP splits the header at a different point from the
lines. They did not accept it, because we could put no figure on it. This year they want the
figure.

Write the working paper as `RESPONSE.md` at the root of the working tree. Work it over every
invoice the export holds — the whole range, every market, the goods invoices and the plan
invoices together — and cover six things.

**The two totals.** The tax the headers say and the tax the lines say, each as integer cents
on a line of its own, and the difference between them with the direction stated.

**Which of the two is the tax we charged.** Not an assertion, evidence. The AR export cannot
vouch for itself, so the corroboration has to come from a feed outside it.

**What the other number is.** Say what rule produces it, precisely enough that the auditor can
reproduce the figure from the columns on the row. Then say how far that rule reaches. A rule
that holds on a sample is a coincidence; a rule that holds on the book is the finding.

**Market by market.** One line per market: the tax the headers say, the tax the lines say, and
the difference. We file market by market, so a group total that comes out close because two
errors cancelled is worse than one that is plainly wrong. Name the market the headers overstate
by the most and give that overstatement in cents.

**The plan invoices.** On those the header agrees with its own line, to the cent, every time.
Say what that agreement is worth as assurance.

**What is left over.** Put the right tax on an invoice and the header still does not come to
its own lines on some of them. Say on how many invoices, how much in cents across the book, and
what that money is — name the column it comes from.

Keep it to the page. The desk reads it once and the auditor reads it once. Every figure in it
has to be a figure you measured, in integer cents; a rate quoted instead of a total is not
something an auditor can add up.

This is a working paper, not a change. Nothing in the pipelines moves this week: `dbt/`,
`projects/`, `include/lib/`, `contracts/` and `config/` all stay as they are.
