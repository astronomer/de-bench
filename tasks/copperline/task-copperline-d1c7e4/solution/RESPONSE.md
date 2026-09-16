# MER-311

- The number the page never had is `raw.orders.order_discount_cents`: the
  promotion the OMS takes off the whole order. It sits on the order header and
  the OMS never pushes it down, so `raw.order_lines.line_total_cents` is the
  line before it. Adding the lines up prices the order before its own discount
  came off, which is the three per cent.
- The rule for spreading it over the lines is in
  `dbt/copperline_analytics/models/shared/intermediate/int_order_lines_discounted.sql`,
  which is where the platform spreads it once for everybody. `int_net_sales_lines`
  and `fct_order_line` read that model, and so do commerce, finance, growth and
  customer.
- The rule: give each line the order's discount pro rata by its tax-exclusive
  line total, in whole cents, by largest remainder — every line takes the floor
  of its share, then the leftover cents go one each to the lines with the
  biggest fraction. An order whose lines sum to zero takes no allocation.
- On every order, the shares have to add back to `order_discount_cents`
  exactly. That is what largest remainder is for: rounding each line on its own
  loses or invents cents, and the loss shows up later as a wandering one-cent
  gap in the reconciliation ties.
