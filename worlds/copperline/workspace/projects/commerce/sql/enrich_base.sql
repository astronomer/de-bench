-- The day's orders, less the synthetic ones. A soft-deleted order stays:
-- docs/reconciliation-policy.md R-6 says a row the source deleted still has to
-- be accounted for, and dropping it here loses it everywhere downstream.
CREATE OR REPLACE TABLE copperline.staging.enrich_base AS
SELECT order_id, customer_ref, loyalty_id, brand, channel, store_id,
       market_code, local_order_date, event_time_utc, event_time_local,
       order_status, currency_code, fx_rate_ppm, subtotal_cents,
       order_discount_cents, tax_cents, shipping_cents, grand_total_cents,
       gift_card_applied_cents, order_ref, source_system,
       deleted_at IS NOT NULL AS deleted_at_source
FROM copperline.raw.orders
WHERE local_order_date = ?
  AND NOT is_test
