"""LOCAL PATCH (restart-cron-ticker): cron keeps firing on time during a restart's after-turn wait.

The restart wait used to refuse every cron dispatch (``runner._draining``), so a 30-minute wait on a
long job stalled a 15-minute job for 30 minutes. These tests drive the real restart wait, the real
``tick`` and the real cron store: a job due mid-wait fires once in the old process, a job due after
the old process stops admitting is left pending and fires once in the next process, and the wait
still converges.
"""

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from gateway.restart_cron_gate import RestartCronDispatchGate
from gateway.run_shutdown import GatewayShutdownMixin


class _Runner:
    """The attributes the restart wait and the gate read; other work is a plain counter."""

    def __init__(self, other_work: int, timeout: float = 30.0):
        from cron.scheduler import get_running_job_ids

        self.other_work = other_work
        self._running = True
        self._draining = True  # request_restart() sets it before the wait starts
        self._external_drain_active = False
        self._restart_after_turn_timeout = timeout
        self._cron_dispatch_gate = RestartCronDispatchGate(self)
        self._active_work_count = lambda: self.other_work + len(get_running_job_ids())
        self._wedged_agent_count = lambda: 0
        self._describe_active_work = lambda: []
        self._scale_to_zero_status = lambda *a, **k: None
        self._awaitable_work_count = GatewayShutdownMixin._awaitable_work_count.__get__(self)
        self.wait = GatewayShutdownMixin._await_active_work_before_restart.__get__(self)


@pytest.fixture
def cron_env(monkeypatch):
    from cron import jobs, scheduler

    for name in ("_maybe_run_worktree_maintenance", "_sweep_mcp_orphans", "_maybe_reap_dead_owners"):
        monkeypatch.setattr(scheduler, name, lambda: None)
    monkeypatch.setattr(scheduler, "_sweep_mcp_orphans_when_all_done", lambda *_a: None)
    monkeypatch.setattr(scheduler, "_should_yield_tick_to_fresh_gateway", lambda: None)
    fired: list = []
    release = threading.Event()

    def run_one_job(job, **_kwargs):
        # Stands in for the agent run only: the real fire claim (claim_job_for_fire) precedes it.
        fired.append(job["id"])
        assert release.wait(10)
        jobs.mark_job_run(job["id"], True)
        return True

    monkeypatch.setattr(scheduler, "run_one_job", run_one_job)
    pool = ThreadPoolExecutor(max_workers=4)
    monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda *_a: pool)

    def make_due_job(name: str) -> str:
        job = jobs.create_job(prompt="beat", schedule="every 15m", name=name)
        past = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        jobs.update_job(job["id"], {"next_run_at": past})
        return job["id"]

    def next_run(job_id: str) -> str:
        return jobs.get_job(job_id)["next_run_at"]

    try:
        yield scheduler, make_due_job, next_run, fired, release
    finally:
        release.set()
        pool.shutdown(wait=True)


async def _tick(scheduler, gate):
    return await asyncio.to_thread(scheduler.tick, verbose=False, sync=False, can_dispatch=gate)


async def _until(predicate, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        assert asyncio.get_running_loop().time() < deadline, "condition not reached"
        await asyncio.sleep(0.02)


@pytest.mark.asyncio
async def test_tick_due_mid_restart_wait_fires_once_on_time(cron_env):
    scheduler, make_due_job, next_run, fired, release = cron_env
    runner = _Runner(other_work=1)  # e.g. a long cron job the restart is waiting on
    beat = make_due_job("ana-live-beat")

    wait = asyncio.create_task(runner.wait())
    await _until(lambda: runner._cron_dispatch_gate.restart_admission_open)

    assert await _tick(scheduler, runner._cron_dispatch_gate) == 1
    await _until(lambda: fired == [beat])
    assert await _tick(scheduler, runner._cron_dispatch_gate) == 0  # advanced: never fires twice
    assert datetime.fromisoformat(next_run(beat)) > datetime.now(timezone.utc)
    assert fired == [beat]

    # The job fired during the wait keeps the process alive: no exit mid-job.
    runner.other_work = 0
    await asyncio.sleep(0.3)
    assert not wait.done()
    release.set()
    assert await asyncio.wait_for(wait, 5) is True
    assert fired == [beat]


@pytest.mark.asyncio
async def test_handoff_to_next_process_runs_each_beat_exactly_once(cron_env):
    scheduler, make_due_job, next_run, fired, release = cron_env
    old = _Runner(other_work=1)
    fired_in_old = make_due_job("fired-in-old")

    wait = asyncio.create_task(old.wait())
    await _until(lambda: old._cron_dispatch_gate.restart_admission_open)
    assert await _tick(scheduler, old._cron_dispatch_gate) == 1
    await _until(lambda: fired == [fired_in_old])
    release.set()
    old.other_work = 0
    assert await asyncio.wait_for(wait, 5) is True

    # stop() has begun: the old process refuses and leaves the beat due, untouched, in the store.
    old._running = False
    handed_off = make_due_job("handed-off")
    pending_at = next_run(handed_off)
    assert await _tick(scheduler, old._cron_dispatch_gate) == 0
    assert next_run(handed_off) == pending_at

    # Next process: its first tick fires the handed-off beat once and never re-fires the old one.
    new = _Runner(other_work=0)
    new._draining = False
    assert await _tick(scheduler, new._cron_dispatch_gate) == 1
    await _until(lambda: len(fired) == 2)
    assert await _tick(scheduler, new._cron_dispatch_gate) == 0
    await asyncio.sleep(0.1)
    assert fired == [fired_in_old, handed_off]


@pytest.mark.asyncio
async def test_restart_wait_still_completes_with_beats_keep_falling_due(cron_env):
    scheduler, make_due_job, next_run, fired, release = cron_env
    release.set()  # fired jobs finish immediately
    runner = _Runner(other_work=1)
    gate = runner._cron_dispatch_gate

    wait = asyncio.create_task(runner.wait())
    await _until(lambda: gate.restart_admission_open)
    first = make_due_job("beat-1")
    assert await _tick(scheduler, gate) == 1

    runner.other_work = 0  # pre-restart work done: admission closes, the wait converges
    assert await asyncio.wait_for(wait, 5) is True
    assert not gate.restart_admission_open
    late = make_due_job("beat-2")
    assert await _tick(scheduler, gate) == 0  # left pending for the next process
    await _until(lambda: fired == [first])
    assert datetime.fromisoformat(next_run(late)) < datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_admission_closes_near_the_force_drain_cap(cron_env, monkeypatch):
    import gateway.run_shutdown as run_shutdown

    scheduler, make_due_job, _next_run, fired, _release = cron_env
    monkeypatch.setattr(run_shutdown, "_RESTART_CRON_ADMISSION_MARGIN_S", 0.4)
    runner = _Runner(other_work=1, timeout=1.0)
    gate = runner._cron_dispatch_gate
    wait = asyncio.create_task(runner.wait())
    await _until(lambda: gate.restart_admission_open)
    await _until(lambda: not gate.restart_admission_open, timeout=2)
    make_due_job("too-late")
    assert await _tick(scheduler, gate) == 0
    assert await asyncio.wait_for(wait, 5) is False  # the pre-existing work hit the cap, as before
    assert fired == []


def test_gate_refuses_external_drain_and_admits_normal_running():
    class R:
        _running, _draining, _external_drain_active = True, False, False

    r = R()
    gate = RestartCronDispatchGate(r)
    assert gate() is True
    r._external_drain_active = True
    assert gate() is False
    r._external_drain_active, r._draining = False, True
    assert gate() is False  # draining without a restart wait (plain shutdown)
    gate.open_restart_admission()
    assert gate() is True
    r._running = False
    assert gate() is False  # stop() has begun
