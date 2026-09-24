"""Cron dispatch admission across a gateway restart.

A graceful restart first waits for in-flight work (``_await_active_work_before_restart``, up to
``restart_after_turn_timeout``), then runs ``stop()``. ``runner._draining`` is set for BOTH phases,
and the old ``can_dispatch`` gate refused every cron dispatch while it was set, so a restart that
waited 30 minutes on a long cron job stalled the ticker for 30 minutes: a 15-minute job skipped
beats (backlog collapse fires only one catch-up).

This gate keeps the ticker firing on time during the restart WAIT, while the process is going to
stay alive anyway, and refuses dispatch once ``stop()`` begins. A refused due job is not advanced:
its ``next_run_at`` stays in the past in the durable store, so the next gateway's first tick (which
runs immediately at boot) fires it once. A job fired during the wait is counted by the wait (it is
in the running-job ledger), so the process never exits mid-job, and its schedule was advanced before
execution, so the next process never sees it due again.

``hold()`` closes the check/submit race: ``tick`` holds it from the gate check until the due jobs
are in the running-job ledger, and the restart wait does not finish while a hold is outstanding.
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Any, Iterator

logger = logging.getLogger(__name__)


class RestartCronDispatchGate:
    """Callable ``can_dispatch`` gate that also supports ``hold()`` for race-free admission."""

    def __init__(self, runner: Any) -> None:
        self._runner = runner
        self._lock = threading.Lock()
        self._holds = 0
        self._restart_admission = False

    def _allowed_locked(self) -> bool:
        runner = self._runner
        if getattr(runner, "_external_drain_active", False):
            return False
        if not getattr(runner, "_draining", False):
            return True
        # Draining: only the restart wait admits, and only while stop() has not begun.
        return self._restart_admission and bool(getattr(runner, "_running", False))

    def __call__(self) -> bool:
        with self._lock:
            return self._allowed_locked()

    @contextmanager
    def hold(self) -> Iterator[bool]:
        """Admission held from the gate check until the tick has registered what it dispatched."""
        with self._lock:
            admitted = self._allowed_locked()
            if admitted:
                self._holds += 1
        try:
            yield admitted
        finally:
            if admitted:
                with self._lock:
                    self._holds -= 1

    def holds_in_flight(self) -> int:
        with self._lock:
            return self._holds

    @property
    def restart_admission_open(self) -> bool:
        with self._lock:
            return self._restart_admission

    def open_restart_admission(self) -> None:
        with self._lock:
            self._restart_admission = True
        logger.info("Restart wait: cron ticker keeps dispatching due jobs on time while in-flight work finishes")

    def close_restart_admission(self, reason: str) -> None:
        with self._lock:
            was_open = self._restart_admission
            self._restart_admission = False
        if was_open:
            logger.info(
                "Restart wait: cron dispatch handed to the next gateway process (%s); "
                "jobs falling due from now stay pending and fire on its first tick", reason)
