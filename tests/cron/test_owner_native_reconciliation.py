"""Fail-safe owner reconciliation shared by execution and delivery recovery."""
import pytest

from cron import executions, delivery_queue


@pytest.mark.parametrize("stored,fresh,live", [
    (None, 123, True),
    (123, 999999, True),
    (123, -456, True),
    (-123456, None, True),
    (-123456, 123456, True),  # incomparable legacy observer, not native evidence
    (-123456, -123456, True),
    (-123456, -123457, False),  # one native microsecond fences PID reuse
])
@pytest.mark.parametrize("consumer", ["execution", "delivery"])
def test_recovery_preserves_uncertain_identity(monkeypatch, tmp_path, stored, fresh, live, consumer):
    import gateway.status
    monkeypatch.setattr(gateway.status, "_pid_exists", lambda pid: True)
    monkeypatch.setattr(executions, "_process_start_time", lambda pid: stored)
    if consumer == "execution":
        monkeypatch.setattr(executions, "EXECUTIONS_FILE", tmp_path / "executions.db")
        record = executions.create_execution("identity", source="cron")
        executions.mark_execution_running(record["id"])
        with executions._transaction() as conn:
            conn.execute("UPDATE executions SET process_id='foreign', pid=424242 WHERE id=?", (record["id"],))
        recover = executions.recover_interrupted_executions
        read = lambda: executions.get_execution(record["id"])
        active = "running"
    else:
        monkeypatch.setattr(delivery_queue, "DELIVERY_DB", tmp_path / "delivery.db")
        monkeypatch.setattr(delivery_queue, "_process_start_time", lambda pid: stored)
        delivery_queue.enqueue("identity", {"id": "job"}, "brief")
        delivery_queue.claim_next()
        with delivery_queue._transaction() as conn:
            conn.execute("UPDATE deliveries SET owner_process_id='foreign', owner_pid=424242")
        recover = delivery_queue.recover_abandoned
        read = lambda: delivery_queue.get_status("identity")
        active = "delivering"
    monkeypatch.setattr(executions, "_process_start_time", lambda pid: fresh)
    assert recover() == (0 if live else 1)
    assert read()["status"] == (active if live else "unknown")


@pytest.mark.parametrize("stored", [None, 123, -123456])
def test_proven_pid_death_releases_owner(monkeypatch, stored):
    import gateway.status
    monkeypatch.setattr(gateway.status, "_pid_exists", lambda pid: False)
    assert not executions._owner_is_live(424242, stored)


def test_pid_probe_failure_preserves_owner(monkeypatch):
    import gateway.status
    def denied(pid):
        raise PermissionError("denied")
    monkeypatch.setattr(gateway.status, "_pid_exists", denied)
    assert executions._owner_is_live(424242, -123456)
