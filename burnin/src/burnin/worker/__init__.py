"""Burn-in worker implementations for all 6 messaging patterns."""

from burnin.worker.base import BaseWorker

PATTERN_EVENTS = "events"
PATTERN_EVENTS_STORE = "events_store"
PATTERN_QUEUE_STREAM = "queue_stream"
PATTERN_QUEUE_SIMPLE = "queue_simple"
PATTERN_COMMANDS = "commands"
PATTERN_QUERIES = "queries"

ALL_PATTERNS = [
    PATTERN_EVENTS,
    PATTERN_EVENTS_STORE,
    PATTERN_QUEUE_STREAM,
    PATTERN_QUEUE_SIMPLE,
    PATTERN_COMMANDS,
    PATTERN_QUERIES,
]

__all__ = [
    "BaseWorker",
    "PATTERN_EVENTS",
    "PATTERN_EVENTS_STORE",
    "PATTERN_QUEUE_STREAM",
    "PATTERN_QUEUE_SIMPLE",
    "PATTERN_COMMANDS",
    "PATTERN_QUERIES",
    "ALL_PATTERNS",
]
