-- The delete half of one settlement date's replace. Scoped to the one
-- partition the day owns, per `CONVENTIONS.md`.
DELETE FROM copperline.ops.payments_restored WHERE settlement_date = ?::DATE
