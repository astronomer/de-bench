"""`sim_identity` and `sim_nwv` — the two account books, and the NW-214 truth.

Northwave Supply ran a separate estate and Copperline never merged it (spec
02 section 16). So this module builds a second, smaller company: an order
management system, a three-level catalog, an account book numbered
`NWA-#####`, a stock ledger and a small general ledger. It shares **no
identifier** with Copperline. That is the whole point of NW-214: no join by
natural key can accidentally work, and the only bridge is the decided merge
list.

BUILD ORDER. This module reads no other `sim_*` schema. It runs after the
reference modules and **before `customer`**, because the customer module
reads `sim_identity.trade_accounts` by `trade_seq` rather than inventing
names of its own.

WHAT IT WRITES

  `sim_identity.trade_accounts`  4,000 rows, `trade_seq` 1..4000. The
      identity of Copperline's trade book: legal name, trading name,
      contact, e-mail, phone, address, tax id. The customer module owns
      `customer_id` and joins this table by `trade_seq`.
  `sim_identity.merge_truth`     360 rows: `trade_seq`, `nwa_id`, `kind`.
      **Answer-key material.** It is dropped with the other `sim_` schemas
      and must never reach `raw.*`. The extract that ships is
      `ops.merge_candidates`, built later from the 300 true rows; the
      absence of the 60 decoys from it is load-bearing.
  `sim_nwv.*`                    Northwave's own upstream, `accounts` first.

Both books' identity strings are written here, together, because the
300/90/60 counts are only exact if one author writes both sides of every
pair.

THE ADVERSARIAL POPULATION (spec 01 section 3.3, spec 03 section 13).
4,000 Copperline trade accounts, 3,960 active. 1,400 Northwave accounts,
1,380 active. Exactly 300 of them are the same business twice. Every
record's role is fixed by its ordinal, so nothing is sampled and no count
can drift:

  `true_visible`             210  Of these, 205 share a `tax_id` and an
                                  exact e-mail as well as a near-identical
                                  name and the same city, so both the exact
                                  rules and any fuzzy matcher find them; the
                                  other 5 share the name and the city alone.
  `true_fuzzy_invisible`      90  A different trading name, a moved city, a
                                  married surname on the contact, a shared
                                  `info@` dealer-group address, no `tax_id`.
                                  Nothing links the two rows.
  `decoy_fuzzy_attractive`    60  NOT the same business. Identical account
                                  name, identical city; different tax id,
                                  different contact, different street. 43 of
                                  them share the e-mail domain too, which is
                                  what the CRM's own rule matches on.

So a fuzzy matcher gets 210 right and up to 60 wrong against a decided
answer of exactly 300.

WHY NULLS CANNOT GIVE THE ANSWER AWAY. The 95 true pairs that carry no
shared tax id hold NULL on the Northwave side. About 35% of every other
Northwave account holds NULL too, drawn, so `tax_id IS NULL` is no signal.

WHY NO NAME COLLIDES BY ACCIDENT. A business identity is a bijection from an
ordinal to a (place, trade, suffix) triple, and each population takes a
disjoint block of ordinals. Two businesses therefore never share a full
name, a domain or a tax id unless this module pairs them on purpose. Two
businesses can share a place and a trade word and differ only in the suffix,
as any real book does; those always sit in different cities with different
contacts, domains and tax ids.

NORTHWAVE'S CLOCK. Its history runs from its own founding to `nwv_frozen`
(2025-09-30) and stops — the frozen namespace never refreshed again. No date
in any `sim_nwv` table falls after it. `sim_identity` is Copperline's side
and keeps Copperline's clock.
"""

from __future__ import annotations

import datetime as dt

from ..config import Context
from ..streams import draw, uniform

# Northwave's founding. It is not in timeline.yaml because no extract reaches
# back past the fixture range; it dates the store and account book only.
NWV_FOUNDED = dt.date(2011, 3, 7)

# The populations, spec 03 section 13. Every one of these is exact.
TRADE_ACCOUNTS = 4000
NWV_ACCOUNTS = 1400
TRUE_PAIRS = 300
DETERMINISTIC_PAIRS = 205        # share tax id and exact e-mail
FUZZY_ONLY_PAIRS = 5             # share name and city only
INVISIBLE_PAIRS = 90             # share nothing
DECOY_PAIRS = 60
DECOY_SHARED_DOMAIN = 43         # the CRM's own false matches
CL_INACTIVE = 40                 # 20 churned, 20 deleted
NWV_INACTIVE = 20

VISIBLE_PAIRS = DETERMINISTIC_PAIRS + FUZZY_ONLY_PAIRS   # 210

# The three `kind` values `sim_identity.merge_truth` carries.
KIND_VISIBLE = "true_visible"
KIND_INVISIBLE = "true_fuzzy_invisible"
KIND_DECOY = "decoy_fuzzy_attractive"

# Tables that hold Northwave's own history and therefore stop at nwv_frozen.
NWV_OWNED_TABLES = (
    "accounts", "stores", "departments", "classes", "subclasses", "products",
    "orders", "order_lines", "stock_ledger", "gl_accounts", "gl_journal",
)

# The exact column list the fleet agreed for sim_identity.trade_accounts.
TRADE_ACCOUNT_COLUMNS = (
    "trade_seq", "legal_name", "trading_name", "contact_name", "contact_email",
    "phone", "address_line1", "city", "region_code", "postal_code",
    "country_code", "tax_id",
)

# --- the invented word stock ------------------------------------------------

PLACES = (
    "Ashgrove", "Brackenridge", "Calderwood", "Dunmore", "Elmridge", "Fairholt",
    "Glenmark", "Harrowgate", "Ironvale", "Junipergate", "Kestrelbank",
    "Larkfield", "Marlowe", "Northmark", "Oakhollow", "Pinewick", "Quarryhill",
    "Redmarsh", "Stonefell", "Thornbury", "Underhill", "Vandermere",
    "Westbrook", "Yarrowdale", "Amberline", "Blackthorn", "Copperfield",
    "Drumlin", "Eastgate", "Foxglove", "Greystone", "Hazelmoor", "Ivybridge",
    "Jessamine", "Kirkstall", "Lindenway", "Mossbank", "Netherby",
    "Orchardgate", "Pembroke", "Quillon", "Ravenscar", "Sandringham",
    "Tallowfield", "Uppercross", "Vellacott", "Willowmere", "Xanthorpe",
    "Yewbank", "Zephyrhill", "Alderton", "Bellamy", "Carrowmore", "Dovecourt",
    "Ellerby", "Fenwick", "Garrowby", "Havercroft", "Inchcape", "Jorvik",
)
TRADES = (
    "Timber", "Hardware", "Fasteners", "Supply", "Millworks", "Aggregates",
    "Builders", "Fixtures", "Lumber", "Tooling", "Ironworks", "Masonry",
    "Roofing", "Plumbing", "Electrical", "Joinery", "Coatings", "Abrasives",
    "Bearings", "Castings", "Flooring", "Glazing", "Insulation", "Cladding",
    "Decking", "Paving", "Drainage", "Ductwork", "Framing", "Sealants",
    "Adhesives", "Cordage", "Sheeting", "Trusses", "Scaffolding",
    "Landscaping", "Foundry", "Quarry", "Forestry", "Outfitters",
)
SUFFIXES = ("LLC", "Inc", "Co", "Company", "Group", "Holdings", "Partners", "& Sons")

CITIES = (
    ("Bellhaven", "OR"), ("Cedarbrook", "OR"), ("Marchfield", "OR"),
    ("Portway", "OR"), ("Sable Creek", "OR"), ("Tillard", "WA"),
    ("Kingsmere", "WA"), ("Ravensport", "WA"), ("Wendover Falls", "WA"),
    ("Blythe Point", "WA"), ("Granger Mill", "ID"), ("Sturgis Bend", "ID"),
    ("Aspenvale", "ID"), ("Cold Harbor", "MT"), ("Redstone Gap", "MT"),
    ("Fairmount", "CO"), ("Silverlark", "CO"), ("Deep Fork", "CO"),
    ("Wheatridge", "CO"), ("Halloway", "UT"), ("Bristow", "UT"),
    ("Saltmarsh", "NV"), ("Dry Creek", "NV"), ("Verano", "AZ"),
    ("Ocotillo Springs", "AZ"), ("Mesa Verde Junction", "AZ"),
    ("Calder", "NM"), ("Piedra Blanca", "NM"), ("Longmire", "TX"),
    ("Cottonhill", "TX"), ("Brazos Gate", "TX"), ("Wanderlee", "TX"),
    ("Fort Merit", "TX"), ("Ardmore Hills", "OK"), ("Kettle Bluff", "OK"),
    ("Prairieton", "KS"), ("Whitfield", "KS"), ("Cornerstone", "NE"),
    ("Loup Crossing", "NE"), ("Fairwater", "IA"), ("Elmsbury", "IA"),
    ("Northbank", "MN"), ("Pine Hollow", "MN"), ("Birch Landing", "WI"),
    ("Kettleford", "WI"), ("Millbrook Springs", "IL"), ("Danforth", "IL"),
    ("Cranmere", "IN"), ("Salt Fork", "IN"), ("Havenwood", "OH"),
    ("Grindstone", "OH"), ("Barrow Green", "MI"), ("Torrey Lake", "MI"),
    ("Stonemill", "PA"), ("Allersby", "PA"), ("Quarrytown", "PA"),
    ("Windmere", "NY"), ("Fallowfield", "NY"), ("Hartlow", "NY"),
    ("Colmere", "VT"), ("Ashbury Falls", "NH"), ("Pemberton Bay", "ME"),
    ("Saltbridge", "MA"), ("Wrenfield", "MA"), ("Kingsbarrow", "CT"),
    ("Rivermede", "NJ"), ("Tanners Reach", "MD"), ("Applecross", "VA"),
    ("Ferrymoor", "VA"), ("Cattaway", "NC"), ("Sandhill Grove", "NC"),
    ("Windrow", "SC"), ("Pecan Bend", "GA"), ("Marlow Junction", "GA"),
    ("Bayou Chase", "LA"), ("Cypress Landing", "FL"), ("Vero Ridge", "FL"),
    ("Foxhill", "TN"), ("Clay Fork", "KY"), ("Braddock Row", "WV"),
)
STREETS = (
    "Foundry Row", "Kiln Street", "Millrace Way", "Depot Avenue",
    "Quarry Lane", "Tannery Road", "Cooper Street", "Draymans Walk",
    "Sawmill Court", "Anvil Way", "Hoist Lane", "Freightyard Road",
    "Bellows Street", "Forge Crescent", "Gantry Place", "Spur Line Road",
    "Warehouse Row", "Chandlery Street", "Ropewalk Lane", "Brickfield Way",
    "Stonecutter Road", "Wheelwright Street", "Loadbank Avenue",
    "Trestle Way", "Salvage Court", "Ballast Road", "Pallet Lane",
    "Crane Street", "Slipway Road", "Yardmaster Way", "Ironbound Avenue",
    "Kilnfield Road", "Clinker Street", "Grainstore Lane", "Bindery Row",
    "Lathe Street", "Whetstone Way", "Scarp Road", "Culvert Lane",
    "Tallow Street",
)
FIRSTS = (
    "Priya", "Marcus", "Dana", "Oluwaseun", "Ingrid", "Tomas", "Rachel",
    "Hideo", "Amara", "Ben", "Carla", "Dmitri", "Esme", "Farid", "Greta",
    "Hollis", "Imani", "Jonas", "Kaia", "Lev", "Mei", "Nadia", "Otto",
    "Pilar", "Quentin", "Rosa", "Soren", "Tabitha", "Umar", "Vera",
    "Wendell", "Xiomara", "Yusuf", "Zora", "Alice", "Bram", "Cleo",
    "Devan", "Elke", "Fintan", "Gita", "Hank", "Iris", "Jarrah", "Kemal",
    "Liesl", "Mateo", "Noor",
)
LASTS = (
    "Raman", "Okonkwo", "Halloran", "Berzins", "Nakamura", "Duarte",
    "Feldman", "Oyelaran", "Kovacs", "Mbeki", "Sandoval", "Trelawney",
    "Aurand", "Bosco", "Cardew", "Dhillon", "Ericsson", "Falkner",
    "Gundersen", "Haverford", "Ibarra", "Jessup", "Kirkbride", "Lindqvist",
    "Marchetti", "Nightingale", "Ostrowski", "Pennington", "Quiroga",
    "Rasmussen", "Stavros", "Thackeray", "Ulmer", "Varela", "Whitcomb",
    "Yardley", "Zielinski", "Ashcroft", "Bellweather", "Coplin",
    "Devereux", "Ellingham", "Fairbrother", "Golightly", "Hensarling",
    "Isenberg", "Jarvis", "Kettleborough", "Lacroix", "Merriwether",
    "Norrington", "Ollivander", "Pemberton", "Quill", "Ravensworth",
    "Sutcliffe", "Tolliver", "Underwood", "Vasquez", "Wainwright",
)
DEALER_GROUPS = (
    "northmarkgroup", "sablecreekdealers", "trelawneyholdings",
    "cascadetradegroup", "ironboundpartners", "meridiantradeco",
    "harrowdealernet", "coppergategroup", "sixriversgroup",
    "keystonetradegroup", "blackthorndealers", "orchardgatenet",
    "farrowtradegroup", "quarryhilldealers", "westbrooktradegroup",
)
OWNERS = (
    "R. Sandoval", "P. Okonkwo", "M. Halloran", "T. Berzins", "K. Nakamura",
    "J. Duarte", "L. Feldman", "S. Kovacs",
)
REVIEWERS = ("d.chastain", "m.arriaga", "s.opoku", "t.villiers")

assert len(PLACES) == 60 and len(TRADES) == 40 and len(SUFFIXES) == 8
assert len(CITIES) == 80 and len(STREETS) == 40
assert len(FIRSTS) == 48 and len(LASTS) == 60 and len(DEALER_GROUPS) == 15
assert DETERMINISTIC_PAIRS + FUZZY_ONLY_PAIRS + INVISIBLE_PAIRS == TRUE_PAIRS

# Disjoint blocks of the identity ordinal.
_G_PAIR = 0             # 0..299      the 300 shared businesses
_G_DECOY = 1000         # 1000..1059  the Copperline half of the 60 decoys
_G_CL_PLAIN = 2000      # 2000..5639  the other 3,640 Copperline accounts
_G_NWV_PLAIN = 6000     # 6000..7039  the other 1,040 Northwave accounts
_G_DECOY_DOMAIN = 7500  # 7500+seq    a domain of their own, for the roles that
                        #             match on name and city but not on e-mail
_G_SHADOW = 8000        # 8000..8299  the invisible pairs' own trading identity
_G_DECOY_PERSON = 9000  # 9000..9359  the decoys' own people and tax ids
_G_TRADING = 10000      # 10000..13999 alternate trading names for Copperline
_G_MAX = 14000


def _reference(ctx: Context) -> None:
    """The word stock. Six hundred rows of Python, well under the ten
    thousand reference rows the generator contract allows."""
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_identity")
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_nwv")
    ctx.sql("CREATE OR REPLACE TABLE sim_nwv._words "
            "(kind VARCHAR, idx INT, word VARCHAR)")
    rows = []
    for kind, words in (("place", PLACES), ("trade", TRADES), ("suffix", SUFFIXES),
                        ("street", STREETS), ("first", FIRSTS), ("last", LASTS),
                        ("group", DEALER_GROUPS), ("owner", OWNERS),
                        ("reviewer", REVIEWERS)):
        rows += [(kind, i, w) for i, w in enumerate(words)]
    ctx.con.executemany("INSERT INTO sim_nwv._words VALUES (?,?,?)", rows)

    ctx.sql("CREATE OR REPLACE TABLE sim_nwv._cities "
            "(idx INT, city VARCHAR, state VARCHAR)")
    ctx.con.executemany(
        "INSERT INTO sim_nwv._cities VALUES (?,?,?)",
        [(i, c, s) for i, (c, s) in enumerate(CITIES)],
    )


def _identities(ctx: Context) -> None:
    """`sim_nwv._biz` — one row per business identity ordinal.

    (g % 60, g // 60 % 40, g // 2400 % 8) is a bijection onto the 19,200
    name combinations, so distinct ordinals always give distinct names and
    distinct domains. `tax_id` is an arithmetic function of the ordinal for
    the same reason: two businesses must never collide on the deterministic
    key by chance, or the answer key stops being the answer.
    """
    s = ctx.seed
    d_city = draw(s, "'nwv_city'", "g.g")
    d_city_alt = draw(s, "'nwv_city_alt'", "g.g")
    d_first = draw(s, "'nwv_first'", "g.g")
    d_last = draw(s, "'nwv_last'", "g.g")
    d_married = draw(s, "'nwv_married'", "g.g")
    d_street = draw(s, "'nwv_street'", "g.g")
    d_street_no = draw(s, "'nwv_street_no'", "g.g")
    d_postal = draw(s, "'nwv_postal'", "g.g")
    d_area = draw(s, "'nwv_area'", "g.g")
    d_line = draw(s, "'nwv_line'", "g.g")
    d_owner = draw(s, "'nwv_owner'", "g.g")
    city_idx = "({}) % 80".format(d_city)
    city_alt_idx = "((({}) % 80 + 1 + ({}) % 79) % 80)".format(d_city, d_city_alt)

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv._biz AS
        SELECT
            g.g,
            p.word                                           AS place,
            t.word                                           AS trade,
            x.word                                           AS suffix,
            p.word || ' ' || t.word || ' ' || x.word         AS account_name,
            lower(p.word) || lower(t.word)                   AS slug,
            -- The suffix belongs in the domain. Without it two businesses
            -- that share a place word and a trade word would share a domain,
            -- and the CRM's shared-domain rule would find pairs nobody built.
            lower(p.word) || lower(t.word) || '-'
                || lower(replace(replace(x.word, '& ', 'and'), ' ', ''))
                || '.example.com'                            AS domain,
            c.city, c.state,
            ca.city AS city_alt, ca.state AS state_alt,
            '94-' || lpad((3000000 + g.g * 7)::VARCHAR, 7, '0') AS tax_id,
            f.word  AS contact_first,
            l.word  AS contact_last,
            la.word AS contact_last_alt,
            ({street_no})::VARCHAR || ' ' || st.word          AS street,
            lpad((({postal}) % 89999 + 10000)::VARCHAR, 5, '0') AS postal_code,
            '+1-' || ({area})::VARCHAR || '-555-'
                  || lpad((({line}) % 10000)::VARCHAR, 4, '0') AS phone,
            ow.word AS owner
        FROM generate_series(0, {gmax}) g(g)
        JOIN sim_nwv._words p  ON p.kind = 'place'  AND p.idx = g.g % 60
        JOIN sim_nwv._words t  ON t.kind = 'trade'  AND t.idx = (g.g // 60) % 40
        JOIN sim_nwv._words x  ON x.kind = 'suffix' AND x.idx = (g.g // 2400) % 8
        JOIN sim_nwv._cities c  ON c.idx  = {city}
        JOIN sim_nwv._cities ca ON ca.idx = {city_alt}
        JOIN sim_nwv._words f  ON f.kind = 'first' AND f.idx = ({first}) % 48
        JOIN sim_nwv._words l  ON l.kind = 'last'  AND l.idx = ({last}) % 60
        JOIN sim_nwv._words la ON la.kind = 'last'
             AND la.idx = ((({last}) % 60 + 1 + ({married}) % 59) % 60)
        JOIN sim_nwv._words st ON st.kind = 'street' AND st.idx = ({street}) % 40
        JOIN sim_nwv._words ow ON ow.kind = 'owner'  AND ow.idx = ({owner}) % 8
    """.format(
        gmax=_G_MAX, city=city_idx, city_alt=city_alt_idx,
        first=d_first, last=d_last, married=d_married,
        street=d_street, street_no=uniform(d_street_no, 100, 8999),
        postal=d_postal, area=uniform(d_area, 201, 989), line=d_line,
        owner=d_owner,
    ))


def _trade_accounts(ctx: Context) -> None:
    """`sim_identity.trade_accounts` — Copperline's trade book, by ordinal.

    Roles by `trade_seq`: 1..300 are the 300 shared businesses, 301..360 are
    the Copperline half of the 60 decoys, 361..3960 are ordinary accounts,
    and 3961..4000 are the 40 that are not active. The customer module joins
    this table by `trade_seq` and adds its own `customer_id`.

    `trading_name` differs from `legal_name` on about one account in eight,
    which is what a trade book looks like. It is drawn from its own ordinal
    block, so it can never coincide with a Northwave name.
    """
    s = ctx.seed
    d_trading = draw(s, "'cl_trading'", "q.trade_seq")
    d_created = draw(s, "'cl_created'", "q.trade_seq")
    d_tier = draw(s, "'cl_tier'", "q.trade_seq")
    active_end = TRADE_ACCOUNTS - CL_INACTIVE          # 3960
    churn_end = TRADE_ACCOUNTS - CL_INACTIVE // 2      # 3980

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_identity.trade_accounts AS
        SELECT
            q.trade_seq::INTEGER                             AS trade_seq,
            b.account_name                                   AS legal_name,
            CASE WHEN ({trading}) % 100 < 12
                 THEN tn.place || ' ' || tn.trade || ' ' || tn.suffix
                 ELSE b.account_name END                     AS trading_name,
            b.contact_first || ' ' || b.contact_last         AS contact_name,
            lower(b.contact_first) || '.' || lower(b.contact_last)
                || '@' || b.domain                           AS contact_email,
            b.phone,
            b.street                                         AS address_line1,
            b.city,
            b.state                                          AS region_code,
            b.postal_code,
            'US'                                             AS country_code,
            b.tax_id,
            b.domain                                         AS email_domain,
            (DATE '{start}' - INTERVAL 1 DAY * (({created}) % 2900))::DATE AS created_on,
            CASE WHEN q.trade_seq <= {active_end} THEN 'active'
                 WHEN q.trade_seq <= {churn_end}  THEN 'churned'
                 ELSE 'deleted' END                          AS status,
            CASE WHEN ({tier}) % 100 < 12 THEN 'platinum'
                 WHEN ({tier}) % 100 < 38 THEN 'gold'
                 ELSE 'standard' END                         AS tier
        FROM (
            SELECT t.i AS trade_seq,
                   CASE WHEN t.i <= {pairs} THEN {g_pair} + t.i - 1
                        WHEN t.i <= {decoy_end} THEN {g_decoy} + t.i - 1 - {pairs}
                        ELSE {g_plain} + t.i - 1 - {decoy_end} END AS g
            FROM generate_series(1, {n}) t(i)
        ) q
        JOIN sim_nwv._biz b  ON b.g  = q.g
        JOIN sim_nwv._biz tn ON tn.g = {g_trading} + q.trade_seq
        ORDER BY q.trade_seq
    """.format(
        trading=d_trading, created=d_created, tier=d_tier, start=ctx.start,
        active_end=active_end, churn_end=churn_end, n=TRADE_ACCOUNTS,
        pairs=TRUE_PAIRS, decoy_end=TRUE_PAIRS + DECOY_PAIRS,
        g_pair=_G_PAIR, g_decoy=_G_DECOY, g_plain=_G_CL_PLAIN,
        g_trading=_G_TRADING,
    ))
    n, active = ctx.sql(
        "SELECT count(*), count(*) FILTER (WHERE status = 'active') "
        "FROM sim_identity.trade_accounts"
    ).fetchone()
    assert (n, active) == (TRADE_ACCOUNTS, TRADE_ACCOUNTS - CL_INACTIVE), (n, active)


def _accounts(ctx: Context) -> None:
    """`sim_nwv.accounts` — the `NWA-#####` book, 1,400 rows.

    Roles by `nwv_seq`, and every boundary is a count from spec 03 section 13:

        0..204      deterministic pair    shares tax id and e-mail
        205..209    name-and-city pair    shares neither
        210..299    invisible pair        shares nothing at all
        300..342    decoy, shared domain  same name, same city, other company
        343..359    decoy, name only      same name, same city, other domain
        360..1379   ordinary
        1380..1399  ordinary, not active

    `b` is the shared business identity, so `b.g` for a pair or a decoy is
    the same ordinal the Copperline row used. `x` is the shadow identity: it
    equals `b` except for the invisible pairs, which trade under a name of
    their own, and the decoys, whose people and tax ids are their own.

    The name variants keep the suffix. Dropping it would let two businesses
    that differ only in their suffix print the same string.
    """
    s = ctx.seed
    det_end = DETERMINISTIC_PAIRS                       # 205
    fuzzy_end = det_end + FUZZY_ONLY_PAIRS              # 210
    pair_end = TRUE_PAIRS                               # 300
    dom_end = pair_end + DECOY_SHARED_DOMAIN            # 343
    decoy_end = pair_end + DECOY_PAIRS                  # 360
    active_end = NWV_ACCOUNTS - NWV_INACTIVE            # 1380
    close = dt.date.fromisoformat(str(ctx.era("E3_northwave_acquisition")["at"]))
    open_span = (close - NWV_FOUNDED).days

    d_variant = draw(s, "'nwv_name_variant'", "r.nwv_seq")
    d_tax_null = draw(s, "'nwv_tax_null'", "r.nwv_seq")
    d_opened = draw(s, "'nwv_opened'", "r.nwv_seq")
    d_crm = draw(s, "'nwv_crm'", "r.nwv_seq")
    d_crm_id = draw(s, "'nwv_crm_id'", "r.nwv_seq")
    d_group = draw(s, "'nwv_group'", "r.nwv_seq")

    role = """CASE
        WHEN q.nwv_seq < {det}   THEN 'pair_deterministic'
        WHEN q.nwv_seq < {fuzz}  THEN 'pair_name_city'
        WHEN q.nwv_seq < {pair}  THEN 'pair_invisible'
        WHEN q.nwv_seq < {dom}   THEN 'decoy_shared_domain'
        WHEN q.nwv_seq < {decoy} THEN 'decoy_name_only'
        ELSE 'plain' END""".format(det=det_end, fuzz=fuzzy_end, pair=pair_end,
                                   dom=dom_end, decoy=decoy_end)

    # Three spellings of one name. A fuzzy matcher clears all three; an
    # exact string match clears only the first.
    variant = """CASE ({d}) % 3
        WHEN 0 THEN b.account_name
        WHEN 1 THEN b.place || ' ' || b.trade || ' ' || CASE b.suffix
                        WHEN 'LLC' THEN 'L.L.C.'
                        WHEN 'Inc' THEN 'Incorporated'
                        WHEN 'Co' THEN 'Co.'
                        WHEN 'Company' THEN 'Company Ltd'
                        WHEN 'Group' THEN 'Grp'
                        WHEN 'Holdings' THEN 'Hldgs'
                        WHEN 'Partners' THEN 'Ptnrs'
                        ELSE 'and Sons' END
        ELSE 'The ' || b.account_name END""".format(d=d_variant)

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv.accounts AS
        WITH q AS (
            SELECT t.i AS nwv_seq,
                   CASE WHEN t.i < {pair}  THEN {g_pair} + t.i
                        WHEN t.i < {decoy} THEN {g_decoy} + t.i - {pair}
                        ELSE {g_plain} + t.i - {decoy} END AS g,
                   CASE WHEN t.i >= {fuzz} AND t.i < {pair}  THEN {g_shadow} + t.i
                        WHEN t.i >= {pair} AND t.i < {decoy} THEN {g_person} + t.i
                        WHEN t.i < {pair}                    THEN {g_pair} + t.i
                        ELSE {g_plain} + t.i - {decoy} END   AS gx
            FROM generate_series(0, {last}) t(i)
        ), r AS (SELECT q.*, {role} AS role FROM q)
        SELECT
            r.nwv_seq,
            'NWA-' || lpad((10000 + r.nwv_seq * 3)::VARCHAR, 5, '0') AS nwv_account_id,
            CASE r.role
                WHEN 'pair_invisible'      THEN x.account_name
                WHEN 'decoy_shared_domain' THEN b.account_name
                WHEN 'decoy_name_only'     THEN b.account_name
                WHEN 'plain'               THEN b.account_name
                ELSE {variant} END                                   AS account_name,
            CASE r.role
                WHEN 'pair_invisible' THEN b.contact_first || ' ' || b.contact_last_alt
                ELSE x.contact_first || ' ' || x.contact_last END     AS primary_contact,
            CASE r.role
                WHEN 'pair_deterministic' THEN lower(b.contact_first) || '.'
                     || lower(b.contact_last) || '@' || b.domain
                WHEN 'pair_name_city'      THEN 'sales@' || dd.domain
                WHEN 'pair_invisible'      THEN 'info@' || dg.word || '.example.com'
                WHEN 'decoy_shared_domain' THEN 'accounts@' || b.domain
                WHEN 'decoy_name_only'     THEN lower(x.contact_first) || '.'
                     || lower(x.contact_last) || '@' || dd.domain
                ELSE lower(b.contact_first) || '.' || lower(b.contact_last)
                     || '@' || b.domain END                           AS contact_email,
            CASE WHEN r.role = 'pair_deterministic' THEN b.phone ELSE x.phone END AS phone,
            x.street                                                  AS billing_street,
            CASE WHEN r.role = 'pair_invisible' THEN b.city_alt  ELSE b.city  END AS billing_city,
            CASE WHEN r.role = 'pair_invisible' THEN b.state_alt ELSE b.state END AS billing_state,
            x.postal_code                                             AS billing_postal,
            'US'                                                      AS country,
            CASE
                WHEN r.role = 'pair_deterministic' THEN b.tax_id
                WHEN r.role IN ('pair_name_city', 'pair_invisible') THEN NULL
                WHEN ({tax_null}) % 100 < 35 THEN NULL
                ELSE x.tax_id END                                     AS tax_id,
            (DATE '{founded}' + INTERVAL 1 DAY * (({opened}) % {span}))::DATE AS opened_on,
            CASE WHEN r.nwv_seq < {active_end} THEN 'active' ELSE 'inactive' END AS status,
            x.owner                                                   AS owner,
            CASE WHEN ({crm}) % 100 < 80 THEN 'NWCRM-'
                 || lpad((({crm_id}) % 900000 + 100000)::VARCHAR, 6, '0') END AS legacy_crm_id,
            r.role                                                    AS pair_role
        FROM r
        JOIN sim_nwv._biz b ON b.g = r.g
        JOIN sim_nwv._biz x ON x.g = r.gx
        LEFT JOIN sim_nwv._biz dd
             ON r.role IN ('decoy_name_only', 'pair_name_city')
            AND dd.g = {g_decoy_domain} + r.nwv_seq
        LEFT JOIN sim_nwv._words dg ON dg.kind = 'group' AND dg.idx = ({group}) % 15
        ORDER BY r.nwv_seq
    """.format(
        pair=pair_end, decoy=decoy_end, fuzz=fuzzy_end,
        last=NWV_ACCOUNTS - 1, active_end=active_end,
        g_pair=_G_PAIR, g_decoy=_G_DECOY, g_plain=_G_NWV_PLAIN,
        g_shadow=_G_SHADOW, g_person=_G_DECOY_PERSON,
        g_decoy_domain=_G_DECOY_DOMAIN,
        role=role, variant=variant, tax_null=d_tax_null,
        founded=NWV_FOUNDED, opened=d_opened, span=open_span,
        crm=d_crm, crm_id=d_crm_id, group=d_group,
    ))
    n, ids, active = ctx.sql(
        "SELECT count(*), count(DISTINCT nwv_account_id), "
        "count(*) FILTER (WHERE status = 'active') FROM sim_nwv.accounts"
    ).fetchone()
    assert (n, ids, active) == (NWV_ACCOUNTS, NWV_ACCOUNTS,
                                NWV_ACCOUNTS - NWV_INACTIVE), (n, ids, active)


def _merge_truth(ctx: Context) -> None:
    """`sim_identity.merge_truth` — 300 decided pairs and 60 decoys.

    Answer-key material. The extract writes `ops.merge_candidates` from the
    rows where `kind` starts `true_`; the 60 decoys must never reach it,
    because their absence is what makes the flagged table trustworthy and a
    fuzzy answer wrong.
    """
    s = ctx.seed
    lands = dt.date.fromisoformat(
        str(ctx.era("E3_northwave_acquisition")["book_lands"]))
    d_conf = draw(s, "'nwv_conf'", "a.nwv_seq")
    d_decided = draw(s, "'nwv_decided'", "a.nwv_seq")
    d_reviewer = draw(s, "'nwv_reviewer'", "a.nwv_seq")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_identity.merge_truth AS
        SELECT
            (a.nwv_seq + 1)::INTEGER                     AS trade_seq,
            a.nwv_account_id                             AS nwa_id,
            CASE
                WHEN a.pair_role IN ('pair_deterministic', 'pair_name_city')
                     THEN '{visible}'
                WHEN a.pair_role = 'pair_invisible' THEN '{invisible}'
                ELSE '{decoy}' END                       AS kind,
            a.pair_role                                  AS pair_role,
            a.pair_role LIKE 'pair_%'                    AS is_true_pair,
            a.pair_role = 'pair_deterministic'           AS tax_id_match,
            a.pair_role = 'pair_deterministic'           AS email_exact_match,
            a.pair_role IN ('pair_deterministic',
                            'decoy_shared_domain')       AS email_domain_match,
            CASE WHEN a.pair_role = 'pair_deterministic'
                 THEN 'deterministic_tax_id' ELSE 'manual_review' END AS method,
            (CASE WHEN a.pair_role = 'pair_deterministic' THEN 1.0000
                  ELSE round(0.72 + (({conf}) % 2700) / 10000.0, 4)
             END)::DECIMAL(5,4)                          AS confidence,
            rv.word                                      AS decided_by,
            (DATE '{lands}' + INTERVAL 1 DAY *
                CASE WHEN a.pair_role = 'pair_deterministic'
                     THEN 1 + ({decided}) % 8
                     ELSE 3 + ({decided}) % 100 END)::DATE AS decided_on,
            CASE a.pair_role
                WHEN 'pair_deterministic' THEN 'matched on federal tax id'
                WHEN 'pair_name_city' THEN 'same trading name and city, no tax id on file'
                ELSE 'confirmed by the account team from the dealer agreement'
            END                                          AS note
        FROM sim_nwv.accounts a
        JOIN sim_nwv._words rv ON rv.kind = 'reviewer' AND rv.idx = ({rev}) % 4
        WHERE a.pair_role <> 'plain'
        ORDER BY trade_seq
    """.format(visible=KIND_VISIBLE, invisible=KIND_INVISIBLE, decoy=KIND_DECOY,
               conf=d_conf, lands=lands, decided=d_decided, rev=d_reviewer))

    counts = dict(ctx.sql(
        "SELECT kind, count(*) FROM sim_identity.merge_truth GROUP BY 1"
    ).fetchall())
    assert counts == {KIND_VISIBLE: VISIBLE_PAIRS,
                      KIND_INVISIBLE: INVISIBLE_PAIRS,
                      KIND_DECOY: DECOY_PAIRS}, counts
    n = ctx.sql("SELECT count(*) FROM sim_identity.merge_truth m "
                "JOIN sim_identity.trade_accounts t USING (trade_seq)").fetchone()[0]
    assert n == TRUE_PAIRS + DECOY_PAIRS, n


def _stores(ctx: Context) -> None:
    """44 stores, opened between 2011 and 2023 — all before the close, which
    is what CMP-4 turns on downstream. Copperline's own estate carries the
    same 44 as store_id 325..368; that mapping belongs to the store module."""
    s = ctx.seed
    span = (dt.date(2023, 12, 31) - NWV_FOUNDED).days
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv.stores AS
        SELECT
            t.i                                              AS store_seq,
            'NWS-' || lpad((101 + t.i * 2)::VARCHAR, 3, '0')  AS store_code,
            p.word || ' ' || c.city                          AS store_name,
            c.city, c.state,
            (DATE '{founded}' + INTERVAL 1 DAY * (({opened}) % {span}))::DATE AS opened_on,
            CASE ({region}) % 4 WHEN 0 THEN 'NW-North' WHEN 1 THEN 'NW-South'
                                WHEN 2 THEN 'NW-Plains' ELSE 'NW-East' END AS region_name,
            ({sqft} * 1000)                                  AS floor_sqft
        FROM generate_series(0, 43) t(i)
        JOIN sim_nwv._words p  ON p.kind = 'place' AND p.idx = ({name}) % 60
        JOIN sim_nwv._cities c ON c.idx = ({city}) % 80
        ORDER BY t.i
    """.format(
        founded=NWV_FOUNDED, span=span,
        opened=draw(s, "'nwv_store_open'", "t.i"),
        region=draw(s, "'nwv_store_region'", "t.i"),
        sqft=uniform(draw(s, "'nwv_store_sqft'", "t.i"), 8, 42),
        name=draw(s, "'nwv_store_name'", "t.i"),
        city=draw(s, "'nwv_store_city'", "t.i"),
    ))


def _catalog(ctx: Context) -> None:
    """Three levels, not Copperline's four, with its own codes and its own
    SKU root. There is no crosswalk, and that is deliberate."""
    s = ctx.seed
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv.departments AS
        SELECT t.i AS dept_seq,
               'ND-' || lpad((t.i + 1)::VARCHAR, 2, '0') AS dept_code,
               w.word || ' Division'                     AS dept_name
        FROM generate_series(0, 7) t(i)
        JOIN sim_nwv._words w ON w.kind = 'trade' AND w.idx = t.i * 5
        ORDER BY t.i
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv.classes AS
        SELECT t.i AS class_seq,
               'NC-' || lpad((100 + t.i)::VARCHAR, 3, '0') AS class_code,
               d.dept_code,
               w.word || ' ' || v.word                     AS class_name
        FROM generate_series(0, 33) t(i)
        JOIN sim_nwv.departments d ON d.dept_seq = t.i % 8
        JOIN sim_nwv._words w ON w.kind = 'place' AND w.idx = ({a}) % 60
        JOIN sim_nwv._words v ON v.kind = 'trade' AND v.idx = ({b}) % 40
        ORDER BY t.i
    """.format(a=draw(s, "'nwv_class_a'", "t.i"), b=draw(s, "'nwv_class_b'", "t.i")))
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv.subclasses AS
        SELECT t.i AS subclass_seq,
               'NS-' || lpad((1000 + t.i)::VARCHAR, 4, '0') AS subclass_code,
               c.class_code, c.dept_code,
               w.word || ' ' || v.word                      AS subclass_name
        FROM generate_series(0, 119) t(i)
        JOIN sim_nwv.classes c ON c.class_seq = t.i % 34
        JOIN sim_nwv._words w ON w.kind = 'street' AND w.idx = ({a}) % 40
        JOIN sim_nwv._words v ON v.kind = 'trade'  AND v.idx = ({b}) % 40
        ORDER BY t.i
    """.format(a=draw(s, "'nwv_sub_a'", "t.i"), b=draw(s, "'nwv_sub_b'", "t.i")))
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv.products AS
        SELECT t.i AS product_seq,
               'NWSKU-' || lpad((20000 + t.i * 4)::VARCHAR, 5, '0') AS nwv_sku,
               sc.subclass_code, sc.class_code, sc.dept_code,
               p.word || ' ' || v.word || ' ' || ({size})::VARCHAR  AS product_name,
               'EA'                                                 AS uom,
               round(({cost}) / 100.0, 2)::DECIMAL(18,4)            AS unit_cost,
               'USD'                                                AS currency_code
        FROM generate_series(0, 2399) t(i)
        JOIN sim_nwv.subclasses sc ON sc.subclass_seq = t.i % 120
        JOIN sim_nwv._words p ON p.kind = 'place' AND p.idx = ({a}) % 60
        JOIN sim_nwv._words v ON v.kind = 'trade' AND v.idx = ({b}) % 40
        ORDER BY t.i
    """.format(size=uniform(draw(s, "'nwv_prod_size'", "t.i"), 2, 96),
               cost=uniform(draw(s, "'nwv_prod_cost'", "t.i"), 180, 42000),
               a=draw(s, "'nwv_prod_a'", "t.i"), b=draw(s, "'nwv_prod_b'", "t.i")))


def _orders(ctx: Context) -> None:
    """The acquired book's own order management system.

    Northwave settled everything in USD and stamped local dates, so none of
    Copperline's era boundaries apply here. The feed runs to `nwv_frozen`
    and stops. It starts at the fixture range start rather than at the
    founding: no extract reaches further back, so earlier days would be rows
    nothing can ever read.
    """
    s = ctx.seed
    frozen = dt.date.fromisoformat(
        str(ctx.era("E3_northwave_acquisition")["nwv_frozen"]))
    per_day = ctx.scale(260)

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv._order_skeleton AS
        SELECT date_diff('day', DATE '1970-01-01', d.ds)::BIGINT * 100000 + g.i AS order_id,
               d.ds AS order_date
        FROM (SELECT range::DATE AS ds
              FROM range(DATE '{start}', DATE '{frozen}' + INTERVAL 1 DAY, INTERVAL 1 DAY)) d
        CROSS JOIN generate_series(1, {per_day}) g(i)
        WHERE g.i <= greatest(1, ({per_day} * CASE dayofweek(d.ds)
                                     WHEN 0 THEN 55 WHEN 6 THEN 78 ELSE 100 END) // 100)
    """.format(start=ctx.start, frozen=frozen, per_day=per_day))

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv.order_lines AS
        SELECT order_line_id, order_id, line_no, nwv_sku, qty, unit_price,
               unit_cost, currency_code,
               round(qty * unit_price, 2)::DECIMAL(18,4) AS line_total
        FROM (
            SELECT k.order_id * 10 + l.line_no                     AS order_line_id,
                   k.order_id, l.line_no, p.nwv_sku,
                   ({qty})::DECIMAL(12,3)                          AS qty,
                   round(p.unit_cost * (1 + ({markup}) / 100.0), 2)::DECIMAL(18,4) AS unit_price,
                   p.unit_cost,
                   'USD'                                           AS currency_code
            FROM sim_nwv._order_skeleton k
            JOIN generate_series(1, 5) l(line_no)
                 ON l.line_no <= 1 + ({lines}) % 5
            JOIN sim_nwv.products p ON p.product_seq = ({sku}) % 2400
        )
    """.format(
        qty=uniform(draw(s, "'nwv_qty'", "k.order_id", "l.line_no"), 1, 40),
        markup=uniform(draw(s, "'nwv_markup'", "k.order_id", "l.line_no"), 22, 78),
        lines=draw(s, "'nwv_line_count'", "k.order_id"),
        sku=draw(s, "'nwv_sku'", "k.order_id", "l.line_no"),
    ))

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv.orders AS
        SELECT order_id, order_no, nwv_account_id, store_code, order_date,
               order_local_ts, currency_code, subtotal_amount, discount_amount,
               tax_amount, cost_amount, order_status,
               (subtotal_amount - discount_amount + tax_amount)::DECIMAL(18,4) AS grand_total
        FROM (
            SELECT
                k.order_id,
                'NWO-' || lpad((k.order_id % 10000000)::VARCHAR, 7, '0') AS order_no,
                a.nwv_account_id,
                st.store_code,
                k.order_date,
                (k.order_date::TIMESTAMP + INTERVAL 1 MINUTE * ({ts}))    AS order_local_ts,
                'USD'                                                     AS currency_code,
                agg.subtotal_amount,
                round(agg.subtotal_amount * (({disc}) % 9) / 100.0, 2)::DECIMAL(18,4) AS discount_amount,
                round(agg.subtotal_amount * 0.0725, 2)::DECIMAL(18,4)     AS tax_amount,
                agg.cost_amount,
                CASE ({status}) % 100 WHEN 0 THEN 'cancelled' WHEN 1 THEN 'open'
                                      ELSE 'closed' END                   AS order_status
            FROM sim_nwv._order_skeleton k
            JOIN (SELECT order_id,
                         sum(line_total)::DECIMAL(18,4) AS subtotal_amount,
                         sum(round(qty * unit_cost, 2))::DECIMAL(18,4) AS cost_amount
                  FROM sim_nwv.order_lines GROUP BY 1) agg USING (order_id)
            LEFT JOIN sim_nwv.accounts a
                 ON ({has_acct}) % 100 < 82 AND a.nwv_seq = ({pick}) % {n_acct}
            JOIN sim_nwv.stores st ON st.store_seq = ({store}) % 44
        )
    """.format(
        ts=uniform(draw(s, "'nwv_ts'", "k.order_id"), 420, 1140),
        disc=draw(s, "'nwv_disc'", "k.order_id"),
        status=draw(s, "'nwv_status'", "k.order_id"),
        has_acct=draw(s, "'nwv_acct'", "k.order_id"),
        pick=draw(s, "'nwv_acct_pick'", "k.order_id"),
        store=draw(s, "'nwv_store'", "k.order_id"),
        n_acct=NWV_ACCOUNTS,
    ))
    ctx.sql("DROP TABLE sim_nwv._order_skeleton")


def _stock_ledger(ctx: Context) -> None:
    """Movement grain, store by SKU. Furniture that makes the acquired book
    look like a going concern; no task reads it yet."""
    s = ctx.seed
    frozen = dt.date.fromisoformat(
        str(ctx.era("E3_northwave_acquisition")["nwv_frozen"]))
    per_day = ctx.scale(300)
    kind = draw(s, "'nwv_mv_type'", "d.ds", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv.stock_ledger AS
        SELECT
            date_diff('day', DATE '1970-01-01', d.ds)::BIGINT * 10000 + g.i AS movement_id,
            d.ds AS posted_on,
            st.store_code,
            p.nwv_sku,
            CASE ({kind}) % 10 WHEN 0 THEN 'receipt' WHEN 1 THEN 'transfer_in'
                               WHEN 2 THEN 'transfer_out' WHEN 3 THEN 'adjustment'
                               ELSE 'sale' END                              AS movement_type,
            CASE WHEN ({kind}) % 10 IN (0, 1) THEN ({up})::DECIMAL(12,3)
                 ELSE (-({down}))::DECIMAL(12,3) END                        AS qty_delta,
            p.unit_cost
        FROM (SELECT range::DATE AS ds
              FROM range(DATE '{start}', DATE '{frozen}' + INTERVAL 1 DAY, INTERVAL 1 DAY)) d
        CROSS JOIN generate_series(1, {per_day}) g(i)
        JOIN sim_nwv.stores   st ON st.store_seq   = ({store}) % 44
        JOIN sim_nwv.products p  ON p.product_seq  = ({sku}) % 2400
    """.format(
        kind=kind, start=ctx.start, frozen=frozen, per_day=per_day,
        up=uniform(draw(s, "'nwv_mv_qty'", "d.ds", "g.i"), 4, 240),
        down=uniform(draw(s, "'nwv_mv_qty'", "d.ds", "g.i"), 1, 60),
        store=draw(s, "'nwv_mv_store'", "d.ds", "g.i"),
        sku=draw(s, "'nwv_mv_sku'", "d.ds", "g.i"),
    ))


def _ledger(ctx: Context) -> None:
    """A small general ledger — enough for the acquired book's revenue to
    tie to something, not enough to be a second oracle (spec 02 section 16).
    Every journal entry balances."""
    ctx.sql("CREATE OR REPLACE TABLE sim_nwv.gl_accounts "
            "(account_code VARCHAR, account_name VARCHAR, account_type VARCHAR)")
    ctx.con.executemany(
        "INSERT INTO sim_nwv.gl_accounts VALUES (?,?,?)",
        [("1000", "Cash", "asset"), ("1200", "Accounts receivable", "asset"),
         ("1300", "Inventory", "asset"), ("2000", "Accounts payable", "liability"),
         ("2200", "Sales tax payable", "liability"),
         ("3000", "Retained earnings", "equity"), ("4000", "Trade sales", "revenue"),
         ("4100", "Trade discounts", "revenue"),
         ("5000", "Cost of goods sold", "expense"),
         ("6000", "Store operating cost", "expense")],
    )
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_nwv.gl_journal AS
        WITH m AS (
            SELECT date_trunc('month', order_date)::DATE AS period_start,
                   (date_trunc('month', order_date) + INTERVAL 1 MONTH
                        - INTERVAL 1 DAY)::DATE          AS posted_on,
                   sum(subtotal_amount)::DECIMAL(18,4)   AS gross,
                   sum(discount_amount)::DECIMAL(18,4)   AS disc,
                   sum(tax_amount)::DECIMAL(18,4)        AS tax,
                   sum(cost_amount)::DECIMAL(18,4)       AS cogs
            FROM sim_nwv.orders WHERE order_status <> 'cancelled' GROUP BY 1, 2
        ), lines AS (
            SELECT period_start, posted_on, 1 AS line_no, '1200' AS account_code,
                   (gross - disc + tax) AS debit, 0::DECIMAL(18,4) AS credit FROM m
            UNION ALL SELECT period_start, posted_on, 2, '4000', 0, gross FROM m
            UNION ALL SELECT period_start, posted_on, 3, '4100', disc, 0 FROM m
            UNION ALL SELECT period_start, posted_on, 4, '2200', 0, tax FROM m
            UNION ALL SELECT period_start, posted_on, 5, '5000', cogs, 0 FROM m
            UNION ALL SELECT period_start, posted_on, 6, '1300', 0, cogs FROM m
        )
        SELECT 'NWJ-' || strftime(period_start, '%Y%m') AS entry_id,
               strftime(period_start, '%Y-%m')          AS period,
               posted_on, line_no, account_code,
               debit::DECIMAL(18,4)  AS debit_amount,
               credit::DECIMAL(18,4) AS credit_amount
        FROM lines ORDER BY entry_id, line_no
    """)
    off = ctx.sql("""
        SELECT count(*) FROM (
            SELECT entry_id FROM sim_nwv.gl_journal GROUP BY 1
            HAVING round(sum(debit_amount) - sum(credit_amount), 2) <> 0)
    """).fetchone()[0]
    assert off == 0, "{} Northwave journal entries do not balance".format(off)


def build(ctx: Context) -> None:
    _reference(ctx)
    _identities(ctx)
    _trade_accounts(ctx)
    _accounts(ctx)
    _merge_truth(ctx)
    _stores(ctx)
    _catalog(ctx)
    _orders(ctx)
    _stock_ledger(ctx)
    _ledger(ctx)
