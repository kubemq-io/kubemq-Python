"""Run state machine with thread-safe atomic transitions."""

from __future__ import annotations

import threading
from enum import Enum


class RunState(str, Enum):
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


class PatternState(str, Enum):
    STARTING = "starting"
    RUNNING = "running"
    RECOVERING = "recovering"
    ERROR = "error"
    STOPPED = "stopped"


_START_ALLOWED = frozenset({RunState.IDLE, RunState.STOPPED, RunState.ERROR})
_STOP_ALLOWED = frozenset({RunState.STARTING, RunState.RUNNING})
_CLEANUP_ALLOWED = frozenset({RunState.IDLE, RunState.STOPPED, RunState.ERROR})
_READY_200 = frozenset({RunState.IDLE, RunState.RUNNING, RunState.STOPPED, RunState.ERROR})


class StateMachine:
    """Thread-safe run state machine with compare-and-swap transitions.

    Prevents TOCTOU races from concurrent HTTP requests.
    """

    def __init__(self) -> None:
        self._state = RunState.IDLE
        self._lock = threading.Lock()
        self._error_message: str = ""

    @property
    def state(self) -> RunState:
        with self._lock:
            return self._state

    @property
    def error_message(self) -> str:
        with self._lock:
            return self._error_message

    def is_ready(self) -> tuple[bool, RunState]:
        """Returns (is_ready_200, current_state) for /ready endpoint."""
        with self._lock:
            return self._state in _READY_200, self._state

    def try_start(self) -> tuple[bool, RunState]:
        """Atomically transition to STARTING. Returns (success, current_state)."""
        with self._lock:
            if self._state in _START_ALLOWED:
                self._state = RunState.STARTING
                self._error_message = ""
                return True, RunState.STARTING
            return False, self._state

    def try_stop(self) -> tuple[bool, RunState]:
        """Atomically transition to STOPPING. Returns (success, current_state)."""
        with self._lock:
            if self._state in _STOP_ALLOWED:
                self._state = RunState.STOPPING
                return True, RunState.STOPPING
            return False, self._state

    def can_cleanup(self) -> tuple[bool, RunState]:
        with self._lock:
            return self._state in _CLEANUP_ALLOWED, self._state

    def set_running(self) -> None:
        with self._lock:
            self._state = RunState.RUNNING

    def set_stopped(self) -> None:
        with self._lock:
            self._state = RunState.STOPPED

    def set_error(self, message: str) -> None:
        with self._lock:
            self._state = RunState.ERROR
            self._error_message = message
