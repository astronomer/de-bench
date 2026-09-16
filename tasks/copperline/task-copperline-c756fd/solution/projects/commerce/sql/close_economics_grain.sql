-- One row per order in the day the close has just built.
--
-- This read `marts.order_economics`, which `publish_economics` does not write
-- until three steps later. On a date the close had never published it counted
-- nought rows over nought orders and passed while grading nothing; on a rerun it
-- graded the previous run's publish. The grain that has to hold is the grain of
-- the build, and the build is `staging.close_order_economics`.
SELECT
    (SELECT count(*) FROM copperline.staging.close_order_economics
      WHERE order_date = ?),
    (SELECT count(DISTINCT order_id) FROM copperline.staging.close_order_economics
      WHERE order_date = ?)
