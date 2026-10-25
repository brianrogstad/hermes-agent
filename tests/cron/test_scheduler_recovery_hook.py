"""Provider-free idle recovery dispatch against isolated profile homes."""
import json
import sys
import time
from pathlib import Path

import pytest

from cron import scheduler
from cron import scheduler_tick
from contextlib import contextmanager
from hermes_constants import set_hermes_home_override, reset_hermes_home_override


@contextmanager
def hermes_home_scope(home):
    token = set_hermes_home_override(home)
    try:
        yield
    finally:
        reset_hermes_home_override(token)


@pytest.fixture
def idle(monkeypatch):
    monkeypatch.setattr(scheduler, '_should_yield_tick_to_fresh_gateway', lambda: None)
    monkeypatch.setattr(scheduler, '_maybe_reap_dead_owners', lambda: None)
    monkeypatch.setattr(scheduler, '_maybe_run_worktree_maintenance', lambda: None)
    monkeypatch.setattr(scheduler, '_sweep_stale_inflight_for_tick', lambda jobs: None)
    monkeypatch.setattr(scheduler, '_sweep_mcp_orphans', lambda: None)
    monkeypatch.setattr(scheduler, 'get_due_jobs', lambda: [])
    monkeypatch.setattr('cron.bot_chat_delivery.drain', lambda: None)
    monkeypatch.setattr('agent.estop.check_paused', lambda *a: False)


def configure(home):
    home.mkdir()
    code = "import os,pathlib,time; pathlib.Path(os.environ['HERMES_HOME'],'marker').write_text(os.environ['HERMES_HOME']); time.sleep(0.5)"
    (home / 'config.yaml').write_text(json.dumps({'cron': {'recovery_command': [sys.executable, '-c', code]}}))


def wait_marker(home):
    deadline = time.monotonic() + 5
    while not (home / 'marker').exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert (home / 'marker').read_text() == str(home)


def test_idle_tick_dispatches_per_home_with_live_child_and_throttle(idle, tmp_path):
    a, b = tmp_path / 'a', tmp_path / 'b'
    configure(a)
    configure(b)
    for home in (a, b):
        with hermes_home_scope(home):
            assert scheduler_tick.tick(verbose=False) == 0
        wait_marker(home)
    from cron import scheduler_recovery as recovery
    # A -> B -> A, with dead-owner reap disabled above: neither fence is global.
    with hermes_home_scope(a):
        proc = recovery._children[str(a.resolve())]
        recovery._last_scan[str(a.resolve())] = 0
        assert scheduler_tick.tick(verbose=False) == 0
        assert recovery._children[str(a.resolve())] is proc
        proc.wait(timeout=5)
        recovery._last_scan[str(a.resolve())] = time.monotonic()
        assert scheduler_tick.tick(verbose=False) == 0
        assert recovery._children[str(a.resolve())] is proc
    recovery._children[str(b.resolve())].wait(timeout=5)


def test_absent_opt_in_and_refused_gate_do_not_launch(idle, tmp_path):
    from cron import scheduler_recovery as recovery
    home = tmp_path / 'off'
    home.mkdir()
    with hermes_home_scope(home):
        assert scheduler_tick.tick(verbose=False) == 0
        assert str(home.resolve()) not in recovery._children
    enabled = tmp_path / 'enabled'
    configure(enabled)
    with hermes_home_scope(enabled):
        assert scheduler_tick.tick(verbose=False, can_dispatch=lambda: False) == 0
    assert not (enabled / 'marker').exists()
