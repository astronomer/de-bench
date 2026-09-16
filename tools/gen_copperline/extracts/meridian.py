"""S2a — Meridian Pay: `raw.pay_meridian_settlements` and `raw.payment_intents`.

Spec 03 section 7 (S2a) and the armed register in section 21. The processor's
own event feed, the OMS's payment-attempt log that bridges it back to an
order, and the hourly JSON-lines files both are landed from.

## The reference is ambiguous and the bridge is not

`order_ref` is spelled `OE-#######`, seven digits, and Copperline's order ids
run to about 181,000,000 over the fixture range. Seven digits hold ten
million, so the reference **cannot** be unique, and the formula every system
in the world uses — `'OE-' || lpad(order_id % 10000000, 7, '0')` — recycles.
The order key block is 200,000 wide per day, so two orders share a reference
when their day numbers are 10,000,000 / 200,000 apart: **the collision
cadence is 50 days**. Inside Meridian's own life (2025-07-01 onward, 349
days) that is about seven orders per reference, and roughly three of them
carry card settlements, so a reference names about three orders.

This is a real property of a seven-digit reference, not a fault, and it is
owned here because this is where the reference is landed. What makes the
world coherent rather than broken is `raw.payment_intents.order_id`: the
bridge carries the OMS's own integer order key, which is unique, beside the
ambiguous text reference. Any question about which order paid is answerable;
it is answerable only through the bridge.

`order_ref` recycles a second way, and this one is inside a single order:
every attempt at one payment reuses one reference, so a settlement joined
straight to an order on the reference picks an arbitrary attempt. That is
NLO-4's whole subject, and `attempt_no` is how the right attempt is named.

EVERY OTHER ID IS RE-MINTED. `event_id`, `intent_id`, `payment_id` and
`processor_txn_id` are all spelled in the simulation by arithmetic that
overflows their fixed widths, so all four collide: `intent_id` collapses
about three to one, and `event_id` — the true idempotency key — repeats a
handful of times at the shipped profile. A key that repeats itself is a fault
nobody planted, and one that repeats itself only at the shipped profile is
worse, because the small profile would say the world was fine. All four are
re-minted densely here in their own shapes and in a deterministic order.
`order_ref` is the one id left ambiguous, and it is ambiguous on purpose.

A restatement names the event it restates, so it needs the landed id of that
event and not the simulation's. The link is rebuilt from the payment's own
event sequence rather than from the id, which is what keeps a colliding id
from fanning the join out.

ONE COLLISION IS NOT FIXED HERE, BECAUSE IT IS NOT THIS MODULE'S.
`sales.payments.gateway_reference` is eight hex digits — 32 bits — and at the
shipped profile 2.3 million card payments draw from it, so about 300 pairs
share a reference. That reference is the only thing Copperline and the
processors have in common, and the simulation joins its payments to its
intents on it, so a shared one emits every event of both payments twice.
This module keeps one row per (reference, step) and lets the first order take
the reference, which is the whole of what the seam can tell apart. The real
fix is upstream: widen `gateway_reference` in `upstream.sales._payments`.
Until then a few hundred payments a world settle under one another's
reference.

## Restatement timing is owned at this boundary

Meridian restates a settlement it has already sent by sending a second event
that names the first in `restates_event_id`. Which events restate, and what
they restate, is the simulation's. **How far back a restatement reaches is
delivery behaviour, so it is set here**, and the simulation's uniform 28-to-31
day spread is replaced with the three populations spec 03 section 7 states:

  * **20 rows at exactly 30 days back and 20 at exactly 31**, the two sides
    of the R-5 boundary. Exact at every profile — a boundary population that
    scaled away would take the graded case with it.
  * **120 rows that restate a closed fiscal month**, the REV-8 adjustment-row
    branch. Also exact at every profile. These are the same 120 rows: the 40
    at the R-5 boundary are 40 of the 120, because a restatement reaching 30
    days back has crossed a period end and its month closed on the fifth
    business day.
  * **every other restatement lands inside the restated event's own fiscal
    period**, three to seven days back, clamped so it never crosses the
    period end. So the count of restatements into a closed month is exactly
    120 and not 120-and-a-tail, which is what makes it gradeable.

Because a restatement's event time moves, its lag is drawn fresh from the
same late tail. Every other row keeps the whole-day lag the simulation gave
it.

## The delivery clock, and the day the world stops

Meridian pushes a file every hour, so `loaded_at` is an hour of the day and
not a copy of the event's own minute. The simulation captures every card
payment inside one fifty-minute window at 03:10, so a `loaded_at` that
carried the event's minute would put all 24 hours of every day in `hr=03`
and the hourly partitioning would be a directory shape with nothing in it.
The landed `loaded_at` keeps the simulation's lag in whole days — which is
what LD-1 and NLO-2 measure — and lands on an hour drawn across the day,
never earlier than the hour after the event.

Nothing is landed that has not been delivered by `today`. The simulation
carries a refund event as far as 46 days past its capture, which for the last
weeks of the fixture range is a delivery in the future; those rows are
dropped here, along with the late tail of the final days.

## The landing tree

`landing/meridian/dt=<ds>/hr=<hh>/events.jsonl`, partitioned on `loaded_at` —
the file is what the vendor delivered in that hour, so a row that happened on
Monday and arrived on Thursday sits in Thursday's file. That is what makes
the late tail visible in the tree and not only in the table.

RET-1 keeps vendor landing files 90 days, so the tree holds the 90 days
ending at the fixture range end and nothing older. Spec 03 section 2 states
that directly, and it is what DR-1 turns on: a rebuild reaching further back
has to read the archive, and the archive job was itself broken for six days.
`loaded_at` is written into each row so a rebuild from the files reproduces
the table exactly rather than to the hour.
"""

from __future__ import annotations

import datetime as dt
import shutil

from ..config import Context
from ..streams import draw, lag_days
from ..upstream._util import DAY_BLOCK
from ..upstream.external import ORDER_REF  # one spelling of the reference

# How far a restatement reaches back. The two boundary counts are exact at
# every profile: a graded boundary that scales away stops being graded.
BOUNDARY_30_ROWS = 20
BOUNDARY_31_ROWS = 20
CLOSED_MONTH_ROWS = 120

# A restatement can only reach a closed month if the event it restates sat
# near the end of a fiscal period, so the closed-month population is drawn
# from the rows whose restated event falls in the last few days of one.
NEAR_CLOSE_DAYS = 8
CLOSED_MONTH_LAG_LO, CLOSED_MONTH_LAG_HI = 18, 26
BULK_LAG_LO, BULK_LAG_HI = 3, 7

# The furthest a restatement can reach forward: 31 days back plus the five
# days of late tail. An exact population picked inside that window of `today`
# could be cut by the delivery clip, so eligibility stops that far short.
RESERVE_DAYS = 31 + 5


def _near_close(ctx: Context) -> str:
    """The rows the exact restatement populations may be drawn from: near a
    fiscal period end, and far enough from `today` to survive delivery."""
    return (f"date_diff('day', e.base_time::DATE, p.period_end) "
            f"<= {NEAR_CLOSE_DAYS - 1} "
            f"AND e.base_time::DATE "
            f"< DATE '{ctx.today}' - INTERVAL {RESERVE_DAYS} DAY")

# RET-1. The vendor keeps 90 days of files and the archive holds the rest.
RETENTION_DAYS = 90
LANDING_SOURCE = "meridian"

# The order key block per day, from the integration contract. Seven digits of
# reference divided by this is the collision cadence in days.
_REF_MODULUS = 10_000_000


def collision_cadence_days() -> int:
    """Days between two orders that share one `OE-#######` reference."""
    return _REF_MODULUS // DAY_BLOCK


# One event per payment per step: authorized, captured, the refund or
# chargeback, and the restatement. The simulation does not carry the step
# number, so it is read back off the row — which is what makes
# (gateway_reference, step) a key the landed ids can be counted against.
_STEP = """
    CASE e.event_type
         WHEN 'authorized' THEN 0
         WHEN 'refunded'   THEN 2
         WHEN 'chargeback' THEN 2
         ELSE CASE WHEN e.restates_event_id IS NULL THEN 1 ELSE 3 END
    END
"""


def _keys(ctx: Context) -> None:
    """Dense, unique `ME-`, `PI-`, `PM-` and `mtx_` keys.

    An intent is one (order, payment attempt) pair, and one order can hold
    two payments, so the order id and the attempt number are not enough to
    name it — the gateway reference is what separates the two chains.

    `_util._meridian_events` is the simulation's event table with every id it
    carries replaced by a dense one, and with the event a restatement restates
    resolved by the window rather than by the ambiguous id.
    """
    ctx.sql("CREATE SCHEMA IF NOT EXISTS _util")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util._meridian_events AS
        WITH stepped AS (
            SELECT e.* EXCLUDE (event_id, payment_id, intent_id, processor_txn_id),
                   ({_STEP})                          AS step,
                   e.restates_event_id IS NOT NULL    AS is_restatement
            FROM sim_ext.meridian_events e
        ), one_per_step AS (
            -- `sales.payments.gateway_reference` is 32 bits of hex, which
            -- repeats a few hundred times at the shipped profile, and the
            -- simulation joins its payments to its intents on it. Where two
            -- payments share a reference every event of both is emitted
            -- twice. One row per (reference, step) is the whole of what the
            -- seam can tell apart, so the copies go here.
            SELECT * FROM stepped
            QUALIFY row_number() OVER (
                PARTITION BY gateway_reference, step
                ORDER BY event_time_utc, settled_amount) = 1
        ), numbered AS (
            SELECT s.* EXCLUDE (restates_event_id),
                   'ME-' || lpad(row_number() OVER (
                       ORDER BY s.gateway_reference, s.step)::VARCHAR, 12, '0')
                       AS event_id
            FROM one_per_step s
        )
        SELECT * EXCLUDE (is_restatement),
               CASE WHEN is_restatement THEN max(event_id) FILTER (WHERE step = 1)
                    OVER (PARTITION BY gateway_reference) END       AS restates_event_id,
               CASE WHEN is_restatement THEN max(event_time_utc) FILTER (WHERE step = 1)
                    OVER (PARTITION BY gateway_reference) END       AS base_time
        FROM numbered
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE _util._meridian_intent_keys AS
        SELECT i.*,
               'PI-' || lpad(row_number() OVER (
                   ORDER BY i.order_id, i.gateway_reference, i.attempt_no
               )::VARCHAR, 8, '0') AS landed_intent_id,
               -- The settlement joins its intent on the gateway reference, so
               -- only one succeeded intent per reference can answer. Where a
               -- shared reference offers two, the first order takes it.
               i.outcome = 'succeeded' AND row_number() OVER (
                   PARTITION BY i.gateway_reference, i.outcome
                   ORDER BY i.order_id, i.attempt_no) = 1 AS settles
        FROM sim_ext.meridian_intents i
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE _util._meridian_payment_keys AS
        SELECT gateway_reference,
               'PM-' || lpad(row_number() OVER (
                   ORDER BY gateway_reference)::VARCHAR, 7, '0') AS landed_payment_id,
               'mtx_' || lpad(row_number() OVER (
                   ORDER BY gateway_reference)::VARCHAR, 8, '0') AS landed_txn_id
        FROM (SELECT DISTINCT gateway_reference FROM sim_ext.meridian_events)
    """)


def _intents(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.payment_intents (
            intent_id VARCHAR PRIMARY KEY,
            order_id BIGINT NOT NULL,
            order_ref VARCHAR NOT NULL,
            attempt_no INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            outcome VARCHAR NOT NULL
        )
    """)
    ctx.sql("""
        INSERT INTO raw.payment_intents
        SELECT landed_intent_id, order_id, order_ref, attempt_no, created_at, outcome
        FROM _util._meridian_intent_keys
    """)


def _settlements(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.pay_meridian_settlements (
            event_id VARCHAR PRIMARY KEY,
            payment_id VARCHAR NOT NULL,
            intent_id VARCHAR NOT NULL,
            order_ref VARCHAR NOT NULL,
            processor_txn_id VARCHAR NOT NULL,
            event_type VARCHAR NOT NULL,
            amount_cents BIGINT NOT NULL,
            currency_code VARCHAR NOT NULL,
            event_time_utc TIMESTAMP NOT NULL,
            loaded_at TIMESTAMP NOT NULL,
            settlement_date DATE,
            restates_event_id VARCHAR,
            attempt_no INTEGER NOT NULL,
            deleted_at TIMESTAMP
        )
    """)
    _check_restatement_room(ctx)
    tail = ctx.cfg["late_tail"]["default"]
    d_order = draw(ctx.seed, "'mer_restate_rank'", "e.event_id")
    d_lag_bulk = draw(ctx.seed, "'mer_restate_lag'", "e.event_id")
    d_lag_closed = draw(ctx.seed, "'mer_restate_closed'", "e.event_id")
    d_load = draw(ctx.seed, "'mer_restate_load'", "e.event_id")
    d_hour = draw(ctx.seed, "'mer_delivery_hour'", "e.event_id")
    ctx.sql(f"""
        INSERT INTO raw.pay_meridian_settlements
        WITH restated AS (
            -- Every restatement, the event it restates, and where that event
            -- sits inside its fiscal period.
            SELECT e.event_id,
                   e.base_time,
                   p.period_end,
                   ({_near_close(ctx)})                            AS near_close,
                   ({d_order})                                     AS ord,
                   ({d_lag_bulk})                                  AS lag_bulk,
                   ({d_lag_closed})                                AS lag_closed,
                   greatest(0, date_diff('day', e.base_time::DATE, p.period_end))
                                                                   AS room_in_period
            FROM _util._meridian_events e
            JOIN (SELECT cal_date,
                         max(cal_date) OVER (PARTITION BY fiscal_year, fiscal_period)
                             AS period_end
                  FROM raw.fiscal_calendar) p ON p.cal_date = e.base_time::DATE
            WHERE e.restates_event_id IS NOT NULL
        ), picked AS (
            SELECT *, CASE WHEN near_close
                           THEN row_number() OVER (PARTITION BY near_close ORDER BY ord)
                      END AS rk_close
            FROM restated
        ), timed AS (
            SELECT event_id,
                   date_trunc('minute', base_time + INTERVAL 1 DAY * (
                       CASE WHEN rk_close <= {BOUNDARY_30_ROWS} THEN 30
                            WHEN rk_close <= {BOUNDARY_30_ROWS + BOUNDARY_31_ROWS} THEN 31
                            WHEN rk_close <= {CLOSED_MONTH_ROWS}
                                 THEN {CLOSED_MONTH_LAG_LO}
                                      + lag_closed % {CLOSED_MONTH_LAG_HI - CLOSED_MONTH_LAG_LO + 1}
                            -- Everything else stays inside the restated
                            -- event's own period, so its month is still open.
                            ELSE greatest(1, least(
                                     {BULK_LAG_LO} + lag_bulk % {BULK_LAG_HI - BULK_LAG_LO + 1},
                                     room_in_period))
                       END)) AS event_time_utc
            FROM picked
        ), landed AS (
            SELECT
                e.event_id,
                pk.landed_payment_id                               AS payment_id,
                ik.landed_intent_id                                AS intent_id,
                e.order_ref,
                pk.landed_txn_id                                   AS processor_txn_id,
                e.event_type,
                round(e.settled_amount * 100)::BIGINT              AS amount_cents,
                e.currency_code,
                coalesce(t.event_time_utc, e.event_time_utc)       AS event_time_utc,
                -- The lag in whole days: the simulation's own for an
                -- ordinary event, a fresh draw for a restatement, whose
                -- event time has just moved.
                CASE WHEN t.event_id IS NULL
                     THEN date_diff('day', e.event_time_utc::DATE, e.loaded_at::DATE)
                     ELSE {lag_days(d_load, tail)} END             AS lag_days,
                ({d_hour}) % 24                                    AS delivery_hour,
                e.settlement_date,
                e.restates_event_id,
                e.attempt_no,
                -- The source API deletes a payment's records at a moment, so
                -- the stamp is the payment's. A restatement that has just
                -- moved past it takes the stamp with it: nothing is deleted
                -- before it happened.
                CASE WHEN e.deleted_at IS NULL THEN NULL
                     ELSE greatest(e.deleted_at,
                                   coalesce(t.event_time_utc, e.event_time_utc)
                                       + INTERVAL 1 DAY) END        AS deleted_at
            FROM _util._meridian_events e
            JOIN _util._meridian_payment_keys pk USING (gateway_reference)
            JOIN _util._meridian_intent_keys ik
              ON ik.gateway_reference = e.gateway_reference AND ik.settles
            LEFT JOIN timed t ON t.event_id = e.event_id
        )
        SELECT event_id, payment_id, intent_id, order_ref, processor_txn_id,
               event_type, amount_cents, currency_code, event_time_utc,
               loaded_at, settlement_date, restates_event_id, attempt_no,
               deleted_at
        FROM (
            SELECT * EXCLUDE (lag_days, delivery_hour),
                   greatest(
                       date_trunc('hour', event_time_utc) + INTERVAL 1 HOUR,
                       (event_time_utc::DATE + INTERVAL 1 DAY * lag_days)::TIMESTAMP
                           + INTERVAL 1 HOUR * delivery_hour
                   ) AS loaded_at
            FROM landed
        )
        -- Nothing the warehouse has not seen yet. The simulation carries a
        -- refund out as far as 46 days past its capture, which for the last
        -- weeks of the range is a delivery that has not happened.
        WHERE loaded_at < TIMESTAMP '{ctx.today} 00:00:00'
    """)


def _check_restatement_room(ctx: Context) -> None:
    """Refuse to build a world that cannot hold the exact populations."""
    need = CLOSED_MONTH_ROWS
    have = ctx.sql(f"""
        SELECT count(*) FROM _util._meridian_events e
        JOIN (SELECT cal_date,
                     max(cal_date) OVER (PARTITION BY fiscal_year, fiscal_period)
                         AS period_end
              FROM raw.fiscal_calendar) p ON p.cal_date = e.base_time::DATE
        WHERE e.restates_event_id IS NOT NULL AND ({_near_close(ctx)})
    """).fetchone()[0]
    if have < need:
        raise ValueError(
            f"the closed-month restatement population needs {need} rows near a "
            f"fiscal period end and the simulation offers {have}; widen "
            f"NEAR_CLOSE_DAYS or raise the restatement rate in upstream.external")


def _landing(ctx: Context) -> None:
    """The 90 live days of hourly JSON lines RET-1 keeps."""
    first = ctx.end - dt.timedelta(days=RETENTION_DAYS - 1)
    root = ctx.landing_dir(LANDING_SOURCE)
    staging = root.parent / f"_{LANDING_SOURCE}_staging"
    shutil.rmtree(staging, ignore_errors=True)
    # DuckDB's JSON writer cannot partition, so each line is built as JSON
    # text and written through the CSV writer with quoting turned off. The
    # result is byte-for-byte the JSON lines the vendor sends.
    stamp = "'%Y-%m-%dT%H:%M:%SZ'"
    ctx.sql(f"""
        COPY (
            SELECT
                loaded_at::DATE                                     AS dt,
                lpad(date_part('hour', loaded_at)::VARCHAR, 2, '0')  AS hr,
                to_json({{
                    'event_id': event_id,
                    'payment_id': payment_id,
                    'intent_id': intent_id,
                    'order_ref': order_ref,
                    'processor_txn_id': processor_txn_id,
                    'event_type': event_type,
                    'amount_cents': amount_cents,
                    'currency_code': currency_code,
                    'event_time_utc': strftime(event_time_utc, {stamp}),
                    'loaded_at': strftime(loaded_at, {stamp}),
                    'settlement_date': strftime(settlement_date, '%Y-%m-%d'),
                    'restates_event_id': restates_event_id,
                    'attempt_no': attempt_no,
                    'deleted_at': strftime(deleted_at, {stamp})
                }})                                                 AS line
            FROM raw.pay_meridian_settlements
            WHERE loaded_at::DATE BETWEEN DATE '{first}' AND DATE '{ctx.end}'
            ORDER BY loaded_at, event_id
        ) TO '{staging}'
        (FORMAT CSV, PARTITION_BY (dt, hr), HEADER false,
         QUOTE '', ESCAPE '', OVERWRITE_OR_IGNORE)
    """)
    for day_dir in sorted(staging.glob("dt=*")):
        for hour_dir in sorted(day_dir.glob("hr=*")):
            out = root / day_dir.name / hour_dir.name
            out.mkdir(parents=True, exist_ok=True)
            part = next(iter(sorted(hour_dir.glob("*.csv"))))
            part.replace(out / "events.jsonl")
    shutil.rmtree(staging, ignore_errors=True)


def _check_reference_spelling(ctx: Context) -> None:
    """The bridge's own reference must be the world's one spelling of it.

    Every system spells `OE-#######` the same way, and NLO-4 only works if
    the naive join matches. If the sales module ever changes the rule this
    fails here rather than quietly halving a match rate.
    """
    ref = ORDER_REF.format(order_id="order_id")
    wrong = ctx.sql(f"SELECT count(*) FROM raw.payment_intents "
                    f"WHERE order_ref <> {ref}").fetchone()[0]
    if wrong:
        raise ValueError(f"{wrong} intent(s) spell order_ref against the formula")


def build(ctx: Context) -> None:
    _keys(ctx)
    _intents(ctx)
    _check_reference_spelling(ctx)
    _settlements(ctx)
    _landing(ctx)
    ctx.sql("DROP TABLE _util._meridian_events")
    ctx.sql("DROP TABLE _util._meridian_intent_keys")
    ctx.sql("DROP TABLE _util._meridian_payment_keys")
