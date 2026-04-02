"""Tests for PEP 565 deprecation decorators."""

from __future__ import annotations

import warnings

from kubemq._internal.deprecation import deprecated, deprecated_async


class TestDeprecatedDecorator:
    def test_emits_deprecation_warning(self):
        @deprecated(replacement="new_func()", since="4.0.0")
        def old_func():
            return 42

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = old_func()

        assert result == 42
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "Use new_func() instead" in str(w[0].message)
        assert "Deprecated since v4.0.0" in str(w[0].message)

    def test_preserves_function_name(self):
        @deprecated(replacement="bar()")
        def foo():
            """Foo docstring."""

        assert foo.__name__ == "foo"
        assert foo.__doc__ == "Foo docstring."

    def test_includes_removal_version(self):
        @deprecated(replacement="new()", since="4.0.0", removal="5.0.0")
        def old():
            pass

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            old()

        assert "Will be removed in v5.0.0" in str(w[0].message)

    def test_without_since_version(self):
        @deprecated(replacement="better()")
        def legacy():
            return "ok"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = legacy()

        assert result == "ok"
        assert len(w) == 1
        assert "Use better() instead" in str(w[0].message)
        assert "Deprecated since" not in str(w[0].message)


class TestDeprecatedAsyncDecorator:
    async def test_emits_deprecation_warning(self):
        @deprecated_async(replacement="new_async()", since="4.0.0")
        async def old_async():
            return 99

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await old_async()

        assert result == 99
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "Use new_async() instead" in str(w[0].message)

    async def test_preserves_async_function_name(self):
        @deprecated_async(replacement="new()")
        async def old_name():
            """Old docstring."""

        assert old_name.__name__ == "old_name"
        assert old_name.__doc__ == "Old docstring."

    async def test_includes_removal_version(self):
        @deprecated_async(replacement="new()", since="4.0.0", removal="5.0.0")
        async def old():
            pass

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            await old()

        assert "Will be removed in v5.0.0" in str(w[0].message)
