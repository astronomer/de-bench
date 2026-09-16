-- The whole frozen feed, with the currency worked out and the amount parsed.
--
-- The currency is the feed's own where the feed states one. Where it does not,
-- it comes off the merchant account: `raw.merchant_regions` says which market
-- the account belongs to and `raw.market_config` says which currency that
-- market bills in. `docs/runbooks/market-setup.md` MKT-1 says the market record
-- is the system of record for that, and that a billing currency is a commercial
-- decision rather than a geographical fact — so the Canadian market bills USD
-- and a region-to-currency map written from country names gets it wrong.
--
-- `amount` is a two-decimal string, the one money column in the estate that
-- lands as text. It parses through DECIMAL, never a float, so every value comes
-- back the exact cent it went out as.
--
-- The region row is joined effective-dated. Every account ships one open span
-- today, and a span that closed would still put each settlement in the market
-- the account belonged to on the day it settled.
INSERT INTO copperline.ops.halcyon_settlement_currency
SELECT
    h.txn_id,
    coalesce(h.currency, mc.billing_currency)                       AS currency_code,
    cast(round(cast(h.amount AS DECIMAL(18,4)) * 100, 0) AS BIGINT) AS amount_cents,
    h.settled_on
FROM copperline.raw.pay_halcyon_settlements h
LEFT JOIN copperline.raw.merchant_regions r
       ON r.merchant_acct = h.merchant_acct
      AND h.settled_on >= r.valid_from
      AND (r.valid_to IS NULL OR h.settled_on <= r.valid_to)
LEFT JOIN copperline.raw.market_config mc
       ON mc.market_code = r.region_code
