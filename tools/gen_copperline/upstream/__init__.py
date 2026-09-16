"""The simulated company: the application databases copperline runs on
(spec chapter 02), written into `sim_*` schemas the orchestrator drops before
the generator exits. The upstream keeps its own clean conventions — decimal
money, one clock, one id scheme — because it is fiction the extracts read,
not data the agent sees. One module per schema group; ORDER is the build
order (the extracts run after all of it)."""

from . import (
    core,
    customer,
    ecommerce,
    external,
    finance,
    finance_reference,
    hr,
    inventory,
    logistics,
    northwave,
    product,
    sales,
    store,
    supply_chain,
    support,
)

ORDER = (
    core,               # first: everything points at reference data
    hr,                 # departments before their cost centers
    finance_reference,  # fiscal periods, chart of accounts, cost centers
    product,            # after core (country ids); owns variants 550001..575000
    store,              # after core; owns stores 101..368 and their versions
    northwave,          # builds sim_identity before customer reads it
    customer,           # trade identity from sim_identity, by trade_seq
    inventory,          # after product and store; reorder_policies is the
    #                   # stocked-SKU registry supply_chain and logistics read
    supply_chain,
    logistics,          # order refs derive from the shared day-block formula
    sales,              # after product/store/customer
    ecommerce,          # after sales
    external,           # settles what sales charged
    support,            # tickets reference orders and customers
    finance,            # the GL posts what everything above did
)
