"""Opt-in per-process wall-clock deadline for delegated children (``HERMES_DELEGATION_CHILD_WALL_DEADLINE_SECONDS``).

The inactivity cap (``delegation.child_timeout_seconds``) restarts on every activity-clock tick, and a child stuck in
"receiving stream response" keeps ticking that clock while never completing the call (resweep-rotation 2026-10-12:
one lane of five hung for 40+ minutes and held the parent's synchronous ``delegate_task``). The wall-clock deadline
is the only bound progress cannot reset. It is read ONLY from the delegating process's environment, so when the env
var is absent nothing changes: config cannot set it and a slow child still runs to completion.
"""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace

from tools import delegate_tool
from tools.delegate_tool_config import CHILD_WALL_DEADLINE_ENV, _get_child_wall_deadline


class _StreamingChild:
    """Ticks its activity clock like an in-flight stream; finishes after ``total_seconds`` (or never)."""

    def __init__(self, *, total_seconds: float, name: str = "child") -> None:
        self.tool_progress_callback = None
        self._credential_pool = None
        self._delegate_saved_tool_names = []
        self._delegate_role = "leaf"
        self._delegate_depth = 1
        self._subagent_id = None
        self.session_id = name
        self.model = "test/model"
        self._total_seconds = total_seconds
        self._activity_ts = time.time()
        self.interrupted = threading.Event()
        self.name = name

    def steer(self, _text: str) -> bool:
        return True

    def run_conversation(self, **_kwargs):
        deadline = time.monotonic() + self._total_seconds
        while time.monotonic() < deadline:
            if self.interrupted.wait(0.05):
                break
            self._activity_ts = time.time()  # "receiving stream response": alive by every liveness signal
        return {"final_response": f"{self.name} DONE", "completed": True, "api_calls": 3, "messages": []}

    def get_activity_summary(self):
        return {"api_call_count": 3, "current_tool": None, "last_activity_ts": self._activity_ts, "max_iterations": 50}

    def hard_interrupt(self, *_args, **_kwargs):
        self.interrupted.set()

    def close(self):
        pass


def _parent(children):
    return SimpleNamespace(
        session_id="parent", _current_task_id=None, _active_children=list(children),
        _active_children_lock=threading.Lock(), _touch_activity=lambda _d: None, _interrupt_requested=False,
    )


def _run(child, monkeypatch):
    # Production default: delegation.child_timeout_seconds: 0 -> no inactivity cap.
    monkeypatch.setattr(delegate_tool, "_get_child_timeout", lambda: None)
    monkeypatch.setattr(delegate_tool, "_get_worktree_isolation", lambda: False)
    return delegate_tool._run_single_child(0, "sweep one lane", child=child, parent_agent=_parent([child]))


def test_hung_streaming_child_is_stopped_at_the_wall_deadline(monkeypatch):
    monkeypatch.setenv(CHILD_WALL_DEADLINE_ENV, "0.5")
    child = _StreamingChild(total_seconds=30.0)

    started = time.monotonic()
    entry = _run(child, monkeypatch)

    assert time.monotonic() - started < 5.0
    assert entry["status"] == "timeout", entry
    assert entry["wall_clock_deadline"] is True
    assert entry["timeout_seconds"] == 0.5
    assert "wall-clock deadline" in entry["error"], entry["error"]
    assert child.interrupted.is_set()


def test_child_finishing_inside_the_deadline_is_untouched(monkeypatch):
    monkeypatch.setenv(CHILD_WALL_DEADLINE_ENV, "5")
    child = _StreamingChild(total_seconds=0.3)

    entry = _run(child, monkeypatch)

    assert entry["status"] == "completed", entry
    assert "wall_clock_deadline" not in entry
    assert not child.interrupted.is_set()


def test_absent_env_means_no_deadline_and_a_slow_child_completes(monkeypatch):
    """No knob -> exactly today's behaviour: a child running well past any would-be deadline still completes."""
    monkeypatch.delenv(CHILD_WALL_DEADLINE_ENV, raising=False)
    child = _StreamingChild(total_seconds=1.0)

    entry = _run(child, monkeypatch)

    assert entry["status"] == "completed", entry
    assert entry["summary"] == "child DONE"
    assert "wall_clock_deadline" not in entry
    assert not child.interrupted.is_set()


def test_knob_reads_env_only_never_config(monkeypatch):
    monkeypatch.delenv(CHILD_WALL_DEADLINE_ENV, raising=False)
    monkeypatch.setattr(delegate_tool, "_load_config", lambda: {
        "child_wall_deadline_seconds": 60, "HERMES_DELEGATION_CHILD_WALL_DEADLINE_SECONDS": 60,
    })
    assert _get_child_wall_deadline() is None
    for raw in ("", "  ", "0", "-5", "abc"):
        monkeypatch.setenv(CHILD_WALL_DEADLINE_ENV, raw)
        assert _get_child_wall_deadline() is None, raw
    monkeypatch.setenv(CHILD_WALL_DEADLINE_ENV, "900")
    assert _get_child_wall_deadline() == 900.0


def test_batch_keeps_finished_lanes_and_returns_the_hung_one_as_timeout(monkeypatch):
    """The resweep shape: several lanes finish, one hangs. The join returns at the deadline with every result."""
    from tools.delegate_tool_dispatch import _run_children_parallel

    monkeypatch.setenv(CHILD_WALL_DEADLINE_ENV, "0.8")
    monkeypatch.setattr(delegate_tool, "_get_child_timeout", lambda: None)
    monkeypatch.setattr(delegate_tool, "_get_worktree_isolation", lambda: False)
    fast = [_StreamingChild(total_seconds=0.1 * (i + 1), name=f"lane{i}") for i in range(4)]
    hung = _StreamingChild(total_seconds=60.0, name="hung")
    kids = fast + [hung]
    parent = _parent(kids)
    tasks = [{"goal": f"lane {i}"} for i in range(len(kids))]
    batch = SimpleNamespace(
        parent_agent=parent, task_list=tasks, live_deleg_id=None, max_children=len(kids), unit_id=None,
        children=[(i, t, c) for i, (t, c) in enumerate(zip(tasks, kids))],
        run_child=lambda i, t, c: delegate_tool._run_single_child(i, t["goal"], child=c, parent_agent=parent),
    )

    results: list = []
    started = time.monotonic()
    _run_children_parallel(batch, results, honor_parent_interrupt=True)

    assert time.monotonic() - started < 5.0
    by_index = {r["task_index"]: r for r in results}
    assert [by_index[i]["status"] for i in range(4)] == ["completed"] * 4
    assert [by_index[i]["summary"] for i in range(4)] == [f"lane{i} DONE" for i in range(4)]
    assert by_index[4]["status"] == "timeout" and by_index[4]["wall_clock_deadline"] is True
    assert hung.interrupted.is_set()
