"""Legacy clock drift cannot prove owner death while its PID still exists.

Native negative birth fingerprints are covered by test_owner_native_reconciliation.
"""

from __future__ import annotations

import pytest

import cron.executions as executions_mod


@pytest.fixture(autouse=True)
def _ledger(monkeypatch, tmp_path):
    import gateway.status

    monkeypatch.setattr(executions_mod, "EXECUTIONS_FILE", tmp_path / "cron" / "executions.db")
    # The owner process exists for every row in this module; only the fingerprint decides.
    monkeypatch.setattr(gateway.status, "_pid_exists", lambda pid: True)


def _seed_external_running(monkeypatch, *, claimed_start_time: int) -> str:
    """Running row owned by a foreign process, with the given claim-time fingerprint."""
    with monkeypatch.context() as m:
        m.setattr(executions_mod, "_PROCESS_ID", "ext-proc")
        m.setattr(executions_mod, "_process_start_time", lambda pid: claimed_start_time)
        record = executions_mod.create_execution("job-drift", source="cron")
        executions_mod.mark_execution_running(record["id"])
    return record["id"]


def _status(execution_id: str) -> str:
    return executions_mod.latest_execution("job-drift")["status"]


@pytest.mark.parametrize(
    "drift,expect_live",
    [
        (100, True),  # 1 s drift: the reported macOS boot-time step (#117505)
        (3600, True),  # legacy clock drift cannot prove PID reuse
    ],
)
def test_legacy_drift_keeps_execution_running(monkeypatch, drift, expect_live):
    """Legacy fingerprints retain ownership regardless of observer clock drift."""
    claimed = 178864182760  # the exact reading from the report
    execution_id = _seed_external_running(monkeypatch, claimed_start_time=claimed)
    monkeypatch.setattr(executions_mod, "_process_start_time", lambda pid: claimed + drift)

    recovered = executions_mod.recover_interrupted_executions()

    if expect_live:
        assert recovered == 0
        assert _status(execution_id) == "running"
    else:
        assert recovered == 1
        assert _status(execution_id) == "unknown"


def test_unreadable_start_time_fail_safe(monkeypatch):
    """A start time that cannot be read now proves nothing about death: the row must survive."""
    execution_id = _seed_external_running(monkeypatch, claimed_start_time=178864182760)
    monkeypatch.setattr(executions_mod, "_process_start_time", lambda pid: None)

    assert executions_mod.recover_interrupted_executions() == 0
    assert _status(execution_id) == "running"
