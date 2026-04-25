import os

from app.objects import PluginManager
from app import create_app


def _plugins_dir() -> str:
    """
    Resolve the absolute plugins directory for tests.
    Mirrors the logic in run.py/create_app but from the tests folder.
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return os.path.join(project_root, "app", "plugins")


def test_inventory_plugin_manifest_present():
    """
    The inventory_control plugin should appear in PluginManager.get_all_plugins()
    once its manifest.json exists in app/plugins/inventory_control.
    """
    pm = PluginManager(plugins_dir=_plugins_dir())
    plugins = pm.get_all_plugins() or []
    systems = {p.get("system_name") for p in plugins}
    assert "inventory_control" in systems


def test_inventory_blueprint_registered():
    """
    The inventory_control admin blueprint should be registered on the Flask app
    via PluginManager.register_admin_routes(create_app()).
    """
    app = create_app()
    # Blueprint name should match the internal Blueprint name defined in routes.py
    assert "inventory_control_internal" in app.blueprints

