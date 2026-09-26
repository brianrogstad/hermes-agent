"""Deterministic process/clock coverage for restart admission; no live gateway."""
import asyncio
from types import SimpleNamespace

import pytest

from gateway.restart_cron_gate import RestartCronDispatchGate
from gateway import run_shutdown


@pytest.mark.parametrize("finish_original", [True, False])
def test_restart_wait_fake_clock(monkeypatch, finish_original):
    clock = SimpleNamespace(now=0.0)
    running = {"original"}
    runner = SimpleNamespace(
        _running=True, _draining=True, _external_drain_active=False,
        _restart_after_turn_timeout=10.0,
        _active_work_count=lambda: len(running),
        _awaitable_work_count=lambda: len(running),
        _wedged_agent_count=lambda: 0,
        _describe_active_work=lambda: [],
        _scale_to_zero_status=lambda *args: None,
    )
    gate = runner._cron_dispatch_gate = RestartCronDispatchGate(runner)
    events = []

    async def sleep(_delay):
        clock.now += 1.0
        assert clock.now <= 10.0, "restart failed to converge at its cap"
        if clock.now == 1.0:
            assert gate()
            with gate.hold() as admitted:
                assert admitted
                running.add("beat")
                events.append("beat admitted")
        if clock.now == 2.0 and finish_original:
            running.remove("original")
        if clock.now == 3.0 and finish_original:
            assert not gate(), "new beats must not extend the wait indefinitely"
            assert "beat" in running, "admitted work keeps the old process alive"
            running.remove("beat")
        if clock.now == 7.0:
            assert not gate(), "admission must close before the force-drain cap"

    monkeypatch.setattr(run_shutdown, "asyncio", SimpleNamespace(
        get_running_loop=lambda: SimpleNamespace(time=lambda: clock.now), sleep=sleep))
    monkeypatch.setattr(run_shutdown, "_running_cron_ids", lambda: set(running))
    result = asyncio.run(run_shutdown.GatewayShutdownMixin._await_active_work_before_restart(runner))
    assert result is finish_original
    assert events == ["beat admitted"]
    assert clock.now == (3.0 if finish_original else 10.0)
    assert not gate.restart_admission_open
    assert not gate()


def test_hold_survives_admission_close_and_releases_on_error():
    runner = SimpleNamespace(_running=True, _draining=True, _external_drain_active=False)
    gate = RestartCronDispatchGate(runner)
    gate.open_restart_admission()
    with pytest.raises(RuntimeError):
        with gate.hold() as admitted:
            assert admitted
            gate.close_restart_admission("original work completed")
            assert not gate()
            assert gate.holds_in_flight() == 1
            raise RuntimeError("submission failed")
    assert gate.holds_in_flight() == 0
