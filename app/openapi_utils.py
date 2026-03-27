"""
Lightweight OpenAPI 3 registry for Sparrow ERP.
Plugins register operations; generate_openapi_spec() produces a JSON spec
for AI integrations and documentation.
"""
from typing import Any, Dict, List, Optional

# Global registry: { blueprint_name: [ { path, method, operation_id, summary, tags, ... } ] }
_OPERATIONS: Dict[str, List[Dict[str, Any]]] = {}


def register_path(
    blueprint_name: str,
    path: str,
    method: str,
    operation_id: str,
    summary: str,
    tags: Optional[List[str]] = None,
    request_schema: Optional[Dict[str, Any]] = None,
    response_schema: Optional[Dict[str, Any]] = None,
    parameters: Optional[List[Dict[str, Any]]] = None,
    security: Optional[List[Dict[str, str]]] = None,
):
    """Register an API operation for OpenAPI spec generation."""
    method = method.upper()
    if blueprint_name not in _OPERATIONS:
        _OPERATIONS[blueprint_name] = []
    _OPERATIONS[blueprint_name].append({
        "path": path,
        "method": method,
        "operationId": operation_id,
        "summary": summary,
        "tags": tags or [blueprint_name],
        "requestBody": {"content": {"application/json": {"schema": request_schema or {}}}} if request_schema else None,
        "responses": {"200": {"description": "OK", "content": {"application/json": {"schema": response_schema or {}}}}} if response_schema else {"200": {"description": "OK"}},
        "parameters": parameters or [],
        "security": security or [{"sessionCookie": []}],
    })


def generate_openapi_spec(
    blueprint_name: Optional[str] = None,
    server_url: str = "/",
    title: str = "Sparrow ERP API",
    version: str = "1.0.0",
) -> Dict[str, Any]:
    """
    Build an OpenAPI 3.0 JSON spec from registered operations.
    If blueprint_name is set, only include operations for that blueprint.
    """
    paths: Dict[str, Dict[str, Any]] = {}
    ops_list = []
    if blueprint_name:
        ops_list = _OPERATIONS.get(blueprint_name, [])
    else:
        for name in _OPERATIONS:
            ops_list.extend(_OPERATIONS[name])

    for op in ops_list:
        path = op["path"]
        method = op["method"].lower()
        if path not in paths:
            paths[path] = {}
        paths[path][method] = {
            "operationId": op.get("operationId"),
            "summary": op.get("summary"),
            "tags": op.get("tags"),
            "responses": op.get("responses", {"200": {"description": "OK"}}),
        }
        if op.get("parameters"):
            paths[path][method]["parameters"] = op["parameters"]
        if op.get("requestBody"):
            paths[path][method]["requestBody"] = op["requestBody"]
        if op.get("security"):
            paths[path][method]["security"] = op["security"]

    return {
        "openapi": "3.0.0",
        "info": {"title": title, "version": version},
        "servers": [{"url": server_url}],
        "paths": paths,
        "components": {
            "securitySchemes": {
                "sessionCookie": {"type": "apiKey", "in": "cookie", "name": "session"},
            },
        },
    }


def get_registered_blueprints() -> List[str]:
    return list(_OPERATIONS.keys())
