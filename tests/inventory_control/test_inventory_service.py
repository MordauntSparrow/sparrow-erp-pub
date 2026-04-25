from app.plugins.inventory_control.objects import InventoryService
from app.objects import get_db_connection


def test_inventory_service_has_core_methods():
    """
    The InventoryService should expose the core domain methods described in the
    Inventory Control design so other layers can depend on a stable surface.
    """
    svc = InventoryService()
    for name in [
        "create_item",
        "update_item",
        "archive_item",
        "create_location",
        "update_location",
        "create_batch",
        "update_batch",
        "get_batches_for_item",
        "record_transaction",
        "get_stock_levels",
        "transfer_stock",
        "rollback_transaction",
    ]:
        assert hasattr(svc, name), f"InventoryService missing method: {name}"


def test_record_transaction_persists_row_and_updates_stock_levels():
    """
    Basic integration: recording an 'in' transaction should create a row in
    inventory_transactions and update inventory_stock_levels for the item/location.
    """
    svc = InventoryService()
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        # Create a simple item and location directly for the test.
        cur.execute(
            "INSERT INTO inventory_items (sku, name) VALUES (%s, %s)",
            ("TEST-SKU", "Test Item"),
        )
        item_id = cur.lastrowid
        cur.execute(
            "INSERT INTO inventory_locations (name, code, type) VALUES (%s, %s, %s)",
            ("Main Warehouse", "MAIN", "warehouse"),
        )
        location_id = cur.lastrowid
        conn.commit()
    finally:
        cur.close()

    tx = svc.record_transaction(
        item_id=item_id,
        location_id=location_id,
        quantity=10,
        transaction_type="in",
        unit_cost=5.0,
    )
    assert tx["transaction_id"] is not None

    # Verify stock level updated
    stock = svc.get_stock_levels(item_id=item_id, location_id=location_id)
    assert stock["quantity_on_hand"] >= 10

