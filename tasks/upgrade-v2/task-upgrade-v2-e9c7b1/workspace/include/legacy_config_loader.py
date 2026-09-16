"""Pipeline configuration loaded by etl_master via imp.load_source."""

DOMAINS = {
    "users": ["user_profiles", "user_preferences", "user_sessions"],
    "transactions": ["payments", "refunds", "subscriptions", "invoices"],
    "products": ["products", "pricing", "inventory"],
}
