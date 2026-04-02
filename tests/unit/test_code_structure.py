"""Structure verification tests for KubeMQ Python SDK (REQ-CQ-5).

Validates package structure invariants:
- All public types declared in ``__all__`` are importable
- No internal modules leak into the public API surface
- PEP 561 ``py.typed`` marker exists
- ``_internal/`` modules are properly isolated
- File naming conventions are consistent
- Each messaging pattern has the expected module layout
"""

from __future__ import annotations

import importlib
import pathlib
import re

import kubemq

_PKG_ROOT = pathlib.Path(kubemq.__file__).parent


class TestAllExports:
    """Every symbol listed in __all__ must be importable from kubemq."""

    def test_all_public_types_importable(self) -> None:
        for name in kubemq.__all__:
            obj = getattr(kubemq, name, _SENTINEL := object())
            assert obj is not _SENTINEL, f"__all__ entry '{name}' resolved to missing attribute"
            assert obj is not None or name == "__version__", f"__all__ entry '{name}' is None"

    def test_all_entries_are_strings(self) -> None:
        for entry in kubemq.__all__:
            assert isinstance(entry, str), f"__all__ contains non-string: {entry!r}"

    def test_no_duplicates_in_all(self) -> None:
        seen: set[str] = set()
        dupes: list[str] = []
        for name in kubemq.__all__:
            if name in seen:
                dupes.append(name)
            seen.add(name)
        assert not dupes, f"Duplicate entries in __all__: {dupes}"


class TestInternalIsolation:
    """Internal modules must not leak into the public API."""

    def test_internal_not_in_all(self) -> None:
        allowed_dunders = {"__version__"}
        for name in kubemq.__all__:
            if name in allowed_dunders:
                continue
            assert not name.startswith("_"), (
                f"Private/internal name '{name}' should not appear in __all__"
            )

    def test_internal_package_exists(self) -> None:
        internal_dir = _PKG_ROOT / "_internal"
        assert internal_dir.is_dir(), "_internal/ package directory must exist"
        assert (internal_dir / "__init__.py").is_file(), "_internal/__init__.py must exist"

    def test_no_internal_reexport(self) -> None:
        """No symbol in __all__ should originate from _internal (by module path)."""
        for name in kubemq.__all__:
            obj = getattr(kubemq, name, None)
            if obj is None:
                continue
            module = getattr(obj, "__module__", "") or ""
            assert "_internal" not in module or name in (
                "NoOpLogger",
                "StdLibLoggerAdapter",
            ), (
                f"__all__ entry '{name}' originates from internal module '{module}'. "
                "Only logging adapters are allowed exceptions."
            )

    def test_grpc_types_not_in_all(self) -> None:
        """No gRPC/protobuf type should appear in __all__."""
        grpc_prefixes = ("pb2", "Pb2", "grpc.", "_pb2")
        for name in kubemq.__all__:
            obj = getattr(kubemq, name, None)
            if obj is None:
                continue
            module = getattr(obj, "__module__", "") or ""
            assert not any(p in module for p in grpc_prefixes), (
                f"__all__ entry '{name}' from gRPC module '{module}'"
            )
            assert not any(p in name for p in grpc_prefixes), (
                f"__all__ entry '{name}' looks like a protobuf type"
            )


class TestPyTypedMarker:
    """PEP 561 py.typed marker must be present and valid."""

    def test_py_typed_exists(self) -> None:
        marker = _PKG_ROOT / "py.typed"
        assert marker.exists(), (
            f"py.typed marker not found at {marker}. PEP 561 requires this for inline type stubs."
        )

    def test_py_typed_is_file(self) -> None:
        marker = _PKG_ROOT / "py.typed"
        assert marker.is_file(), "py.typed must be a regular file, not a directory"

    def test_py_typed_is_empty_or_marker(self) -> None:
        marker = _PKG_ROOT / "py.typed"
        content = marker.read_text(encoding="utf-8").strip()
        assert content == "", f"py.typed should be empty per PEP 561, but contains: {content!r}"


class TestNoCircularImports:
    """Importing kubemq must not raise ImportError."""

    def test_import_succeeds(self) -> None:
        mod = importlib.import_module("kubemq")
        assert mod is not None

    def test_subpackage_imports(self) -> None:
        for subpkg in ("core", "common", "pubsub", "queues", "cq", "transport"):
            mod = importlib.import_module(f"kubemq.{subpkg}")
            assert mod is not None, f"Failed to import kubemq.{subpkg}"


class TestFileNaming:
    """Source file names must be lowercase with underscores (PEP 8)."""

    _FILENAME_RE = re.compile(r"^[a-z][a-z0-9_]*\.py$")

    def test_source_filenames_lowercase(self) -> None:
        violations: list[str] = []
        for py_file in _PKG_ROOT.rglob("*.py"):
            rel = py_file.relative_to(_PKG_ROOT)
            name = py_file.name
            if name == "__init__.py":
                continue
            if "grpc" in str(rel):
                continue
            if not self._FILENAME_RE.match(name):
                violations.append(str(rel))
        assert not violations, f"Files with non-PEP-8 names: {violations}"

    def test_directory_names_lowercase(self) -> None:
        dir_re = re.compile(r"^_?[a-z][a-z0-9_]*$")
        violations: list[str] = []
        for child in _PKG_ROOT.rglob("*"):
            if not child.is_dir():
                continue
            if "__pycache__" in str(child):
                continue
            dir_name = child.name
            if not dir_re.match(dir_name):
                violations.append(str(child.relative_to(_PKG_ROOT)))
        assert not violations, f"Directories with non-PEP-8 names: {violations}"


class TestPatternLayout:
    """Each messaging pattern must have client.py and async_client.py."""

    _PATTERNS = ("pubsub", "queues", "cq")

    def test_pattern_directories_exist(self) -> None:
        for pattern in self._PATTERNS:
            pattern_dir = _PKG_ROOT / pattern
            assert pattern_dir.is_dir(), f"Pattern directory '{pattern}' missing"

    def test_sync_client_exists(self) -> None:
        for pattern in self._PATTERNS:
            client_file = _PKG_ROOT / pattern / "client.py"
            assert client_file.is_file(), f"{pattern}/client.py missing"

    def test_async_client_exists(self) -> None:
        for pattern in self._PATTERNS:
            async_file = _PKG_ROOT / pattern / "async_client.py"
            assert async_file.is_file(), f"{pattern}/async_client.py missing"

    def test_pattern_init_exists(self) -> None:
        for pattern in self._PATTERNS:
            init_file = _PKG_ROOT / pattern / "__init__.py"
            assert init_file.is_file(), f"{pattern}/__init__.py missing"
