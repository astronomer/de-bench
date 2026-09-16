"""Pipeline configuration consumed by etl_master via standard import."""

DOMAINS = {
    "users": ["user_profiles", "user_preferences", "user_sessions"],
    "transactions": ["payments", "refunds", "subscriptions", "invoices"],
    "products": ["products", "pricing", "inventory"],
}
