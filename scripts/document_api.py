#!/usr/bin/env python3
"""
Generate current public API documentation for comparison.

This script extracts all public classes, methods, and their signatures
from the KubeMQ Python SDK to create a snapshot for migration comparison.
"""

import inspect
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, get_type_hints
from datetime import datetime

# Add the project src to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))


def get_signature_string(obj: Any) -> str:
    """Get the signature of a callable as a string."""
    try:
        sig = inspect.signature(obj)
        return str(sig)
    except (ValueError, TypeError):
        return "(unknown)"


def get_docstring_summary(obj: Any) -> Optional[str]:
    """Get the first line of the docstring."""
    doc = inspect.getdoc(obj)
    if doc:
        return doc.split('\n')[0].strip()
    return None


def extract_class_api(cls: type) -> Dict[str, Any]:
    """Extract API information from a class."""
    api = {
        "type": "class",
        "docstring": get_docstring_summary(cls),
        "bases": [base.__name__ for base in cls.__bases__ if base.__name__ != 'object'],
        "methods": {},
        "class_methods": {},
        "static_methods": {},
        "properties": {},
        "attributes": {},
    }

    for name in dir(cls):
        if name.startswith('_') and not name.startswith('__'):
            continue  # Skip private methods but keep dunder methods

        # Skip inherited dunder methods except __init__
        if name.startswith('__') and name != '__init__':
            continue

        try:
            attr = getattr(cls, name)
        except AttributeError:
            continue

        # Check if it's defined in this class (not inherited from object)
        if name != '__init__':
            defining_class = None
            for klass in cls.__mro__:
                if name in klass.__dict__:
                    defining_class = klass
                    break
            if defining_class is object:
                continue

        if isinstance(attr, property):
            api["properties"][name] = {
                "docstring": get_docstring_summary(attr.fget) if attr.fget else None,
                "has_setter": attr.fset is not None,
            }
        elif isinstance(attr, classmethod):
            # Get the underlying function
            func = attr.__func__
            api["class_methods"][name] = {
                "signature": get_signature_string(func),
                "docstring": get_docstring_summary(func),
            }
        elif isinstance(attr, staticmethod):
            func = attr.__func__
            api["static_methods"][name] = {
                "signature": get_signature_string(func),
                "docstring": get_docstring_summary(func),
            }
        elif callable(attr) and not isinstance(attr, type):
            api["methods"][name] = {
                "signature": get_signature_string(attr),
                "docstring": get_docstring_summary(attr),
            }

    return api


def extract_function_api(func: Any) -> Dict[str, Any]:
    """Extract API information from a function."""
    return {
        "type": "function",
        "signature": get_signature_string(func),
        "docstring": get_docstring_summary(func),
    }


def extract_module_api(module: Any, module_name: str) -> Dict[str, Any]:
    """Extract all public APIs from a module."""
    api = {}

    for name in dir(module):
        if name.startswith('_'):
            continue

        try:
            obj = getattr(module, name)
        except AttributeError:
            continue

        # Skip if it's from another module (re-exports)
        if hasattr(obj, '__module__'):
            obj_module = obj.__module__
            if obj_module and not obj_module.startswith('kubemq'):
                continue

        if inspect.isclass(obj):
            api[name] = extract_class_api(obj)
        elif inspect.isfunction(obj):
            api[name] = extract_function_api(obj)

    return api


def main():
    """Generate API documentation snapshot."""
    output = {
        "version": "3.x",
        "generated_at": datetime.now().isoformat(),
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "modules": {}
    }

    # Import all modules
    try:
        from kubemq import pubsub, queues, cq
        from kubemq.common import exceptions
        from kubemq.transport import Connection, TlsConfig, KeepAliveConfig, ServerInfo

        modules_to_document = {
            "kubemq.pubsub": pubsub,
            "kubemq.queues": queues,
            "kubemq.cq": cq,
            "kubemq.common.exceptions": exceptions,
        }

        for module_name, module in modules_to_document.items():
            print(f"Extracting API from {module_name}...")
            output["modules"][module_name] = extract_module_api(module, module_name)

        # Also document transport-related classes
        print("Extracting transport API...")
        output["modules"]["kubemq.transport"] = {
            "Connection": extract_class_api(Connection),
            "TlsConfig": extract_class_api(TlsConfig),
            "KeepAliveConfig": extract_class_api(KeepAliveConfig),
            "ServerInfo": extract_class_api(ServerInfo),
        }

    except ImportError as e:
        print(f"Warning: Could not import module: {e}")
        print("Make sure you're running this from the project root with dependencies installed.")

    # Write output
    output_path = project_root / "v3_api.json"
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nAPI snapshot written to: {output_path}")

    # Print summary
    total_classes = 0
    total_methods = 0
    for module_name, module_api in output["modules"].items():
        for class_name, class_api in module_api.items():
            if isinstance(class_api, dict) and class_api.get("type") == "class":
                total_classes += 1
                total_methods += len(class_api.get("methods", {}))
                total_methods += len(class_api.get("class_methods", {}))
                total_methods += len(class_api.get("static_methods", {}))

    print(f"\nSummary:")
    print(f"  - Modules documented: {len(output['modules'])}")
    print(f"  - Total classes: {total_classes}")
    print(f"  - Total methods: {total_methods}")

    return output


if __name__ == "__main__":
    main()
