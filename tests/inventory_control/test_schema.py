from app.plugins.inventory_control.install import install
from app.objects import get_db_connection


REQUIRED_TABLES = [
    "inventory_items",
    "inventory_locations",
    "inventory_batches",
    "inventory_stock_levels",
    "inventory_transactions",
    "inventory_suppliers",
    "inventory_invoices",
    "inventory_invoice_lines",
    "inventory_supplier_performance",
    "inventory_audit",
]


def _table_exists(cur, name: str) -> bool:
    cur.execute("SHOW TABLES LIKE %s", (name,))
    return cur.fetchone() is not None


def test_install_creates_core_inventory_tables():
    """
    Running the inventory_control install script should create the core tables
    defined in the Inventory Control data model.
    """
    install()
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        try:
            missing = [t for t in REQUIRED_TABLES if not _table_exists(cur, t)]
        finally:
            cur.close()
    finally:
        conn.close()

    assert not missing, f"Missing inventory tables after install(): {missing}"

