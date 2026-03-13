"""Shared channel name validation helpers."""


def validate_channel_name(v: str, *, allow_wildcards: bool = False) -> str:
    """Validate a channel name for common constraints.

    Args:
        v: The channel name to validate.
        allow_wildcards: If True, skip wildcard rejection (e.g. for Events subscriptions).

    Returns:
        The validated channel name.

    Raises:
        ValueError: If the channel violates any constraint.
    """
    if not allow_wildcards and ("*" in v or ">" in v):
        raise ValueError("Channel name cannot contain wildcard characters '*' or '>'.")
    if any(c.isspace() for c in v):
        raise ValueError("Channel name cannot contain whitespace.")
    if v.endswith("."):
        raise ValueError("Channel name cannot end with '.'.")
    return v
