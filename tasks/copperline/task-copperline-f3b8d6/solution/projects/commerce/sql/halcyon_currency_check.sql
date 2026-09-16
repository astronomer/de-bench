-- Four ways the fill can be wrong, counted in one pass.
--
--   missing     a settlement in the feed with no row here
--   duplicated  more rows than settlements
--   unfilled    a row that came out with no currency
--   contested   a merchant account holding more than one currency, which is
--               what a fill that disagrees with the rows the feed already
--               filled looks like from the outside
SELECT
    (SELECT count(*)
       FROM copperline.raw.pay_halcyon_settlements h
       LEFT JOIN copperline.ops.halcyon_settlement_currency c
              ON c.txn_id = h.txn_id
      WHERE c.txn_id IS NULL)                                       AS missing,
    (SELECT count(*) - count(DISTINCT txn_id)
       FROM copperline.ops.halcyon_settlement_currency)             AS duplicated,
    (SELECT count(*)
       FROM copperline.ops.halcyon_settlement_currency
      WHERE currency_code IS NULL OR trim(currency_code) = '')      AS unfilled,
    (SELECT count(*) FROM (
         SELECT h.merchant_acct
           FROM copperline.raw.pay_halcyon_settlements h
           JOIN copperline.ops.halcyon_settlement_currency c
                ON c.txn_id = h.txn_id
          GROUP BY h.merchant_acct
         HAVING count(DISTINCT c.currency_code) > 1))               AS contested
