"""PEP 565-compliant deprecation decorators.

Provides ``@deprecated`` and ``@deprecated_async`` decorators that emit
``DeprecationWarning`` via ``warnings.warn()`` with ``stacklevel=2``
so the warning points at the *caller*, not the decorator itself.
"""

from __future__ import annotations

import functools
import warnings
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def deprecated(
    *,
    replacement: str,
    since: str = "",
    removal: str = "",
) -> Callable[[F], F]:
    """Mark a function or method as deprecated.

    Emits a DeprecationWarning at call time per PEP 565.

    Args:
        replacement: Name of the replacement API (e.g. "send_event()").
        since: Version when deprecation was introduced (e.g. "4.0.0").
        removal: Planned removal version (e.g. "5.0.0"). Empty if TBD.
    """

    def decorator(func: F) -> F:
        parts = [f"{func.__qualname__} is deprecated."]
        if since:
            parts.append(f"Deprecated since v{since}.")
        parts.append(f"Use {replacement} instead.")
        if removal:
            parts.append(f"Will be removed in v{removal}.")
        msg = " ".join(parts)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator


def deprecated_async(
    *,
    replacement: str,
    since: str = "",
    removal: str = "",
) -> Callable[[F], F]:
    """Async variant of ``@deprecated``.

    Args:
        replacement: Name of the replacement API.
        since: Version when deprecation was introduced.
        removal: Planned removal version. Empty if TBD.
    """

    def decorator(func: F) -> F:
        parts = [f"{func.__qualname__} is deprecated."]
        if since:
            parts.append(f"Deprecated since v{since}.")
        parts.append(f"Use {replacement} instead.")
        if removal:
            parts.append(f"Will be removed in v{removal}.")
        msg = " ".join(parts)

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
            return await func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator
