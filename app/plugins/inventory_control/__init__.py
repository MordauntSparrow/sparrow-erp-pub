"""
Inventory Control plugin package.

This module provides
database install helpers via install.py plus Flask blueprints via routes.py.
"""

from .install import install  # Re-export for Plugin/UpdateManager convenience

__all__ = ["install"]

