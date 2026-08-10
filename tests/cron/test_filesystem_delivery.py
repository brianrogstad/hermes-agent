"""Strict create-once filesystem delivery for configured cron targets."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pytest


def _target(source_root: Path, destination_root: Path) -> dict:
    return {
        "kind": "filesystem",
        "target_id": "ana-live",
        "destination_root": str(destination_root),
        "source_roots": [str(source_root)],
        "layout": "ana-live-dated/v1",
    }


def _source(root: Path, payload: bytes = b"exact image bytes") -> Path:
    path = root / "2026" / "08" / "09" / "origin files" / "ana-live-test.png"
    path.parent.mkdir(parents=True, mode=0o700)
    path.write_bytes(payload)
    return path


def test_configured_target_resolution_is_typed_and_rejects_paths(monkeypatch, tmp_path):
    import cron.scheduler as scheduler

    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    source_root.mkdir(mode=0o700)
    destination_root.mkdir(mode=0o700)
    config = {"cron": {"filesystem_delivery_targets": {
        "ana-live": {
            "destination_root": str(destination_root),
            "source_roots": [str(source_root)],
            "layout": "ana-live-dated/v1",
        }
    }}}
    monkeypatch.setattr(scheduler, "load_config", lambda: config)

    expected = _target(source_root, destination_root)
    assert scheduler._resolve_delivery_targets({"deliver": "filesystem:ana-live"}) == [expected]
    monkeypatch.setattr("gateway.config.load_gateway_config", lambda: (_ for _ in ()).throw(
        AssertionError("filesystem preflight must not load gateway config")
    ))
    assert scheduler._preflight_check_delivery({"deliver": "filesystem:ana-live"}) is None
    with pytest.raises(ValueError, match="configured filesystem delivery target"):
        scheduler._resolve_delivery_targets({"deliver": f"filesystem:{destination_root}"})
    with pytest.raises(ValueError, match="configured filesystem delivery target"):
        scheduler._resolve_delivery_targets({"deliver": "filesystem:missing"})
    for mixed in (
        "filesystem:ana-live,origin",
        "filesystem:ana-live,all",
        "filesystem:ana-live,local",
        "filesystem:ana-live,nonesuch",
    ):
        with pytest.raises(ValueError, match="cannot be mixed"):
            scheduler._resolve_delivery_targets({"deliver": mixed})


def test_copy_is_exact_create_once_and_collision_safe(tmp_path):
    from cron.filesystem_delivery import copy_filesystem_delivery

    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    source_root.mkdir(mode=0o700)
    destination_root.mkdir(mode=0o700)
    source = _source(source_root)
    artifact = tmp_path / "owned.png"
    artifact.write_bytes(source.read_bytes())
    target = _target(source_root, destination_root)
    digest = "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()

    first = copy_filesystem_delivery(
        target=target,
        execution_id="a" * 32,
        source_path=str(source),
        artifact_path=str(artifact),
        artifact_sha256=digest,
        artifact_size_bytes=len(source.read_bytes()),
    )
    destination = destination_root / "2026" / "08" / "09" / source.name
    assert destination.read_bytes() == source.read_bytes()
    assert first == {
        "schema": "hermes-filesystem-copy/v1",
        "execution_id": "a" * 32,
        "source_path": str(source),
        "destination_path": str(destination),
        "sha256": digest,
        "size_bytes": len(source.read_bytes()),
        "byte_equal": True,
        "reused": False,
    }
    assert (destination.stat().st_mode & 0o777) == 0o600
    assert all((parent.stat().st_mode & 0o777) == 0o700 for parent in [
        destination.parent, destination.parent.parent, destination.parent.parent.parent,
    ])

    destination.chmod(0o644)
    replay = copy_filesystem_delivery(
        target=target,
        execution_id="a" * 32,
        source_path=str(source),
        artifact_path=str(artifact),
        artifact_sha256=digest,
        artifact_size_bytes=len(source.read_bytes()),
        destination_path=str(destination),
    )
    assert replay["reused"] is True
    assert (destination.stat().st_mode & 0o777) == 0o600

    destination.write_bytes(b"foreign bytes")
    with pytest.raises(FileExistsError, match="different bytes"):
        copy_filesystem_delivery(
            target=target,
            execution_id="a" * 32,
            source_path=str(source),
            artifact_path=str(artifact),
            artifact_sha256=digest,
            artifact_size_bytes=len(source.read_bytes()),
            destination_path=str(destination),
        )
    assert destination.read_bytes() == b"foreign bytes"


def test_copy_rejects_noncanonical_multiple_media_and_symlinks(tmp_path):
    from cron.filesystem_delivery import (
        copy_filesystem_delivery,
        derive_destination_path,
        normalize_configured_target,
        stable_read_source,
    )

    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    source_root.mkdir(mode=0o700)
    destination_root.mkdir(mode=0o700)
    target = _target(source_root, destination_root)
    wrong = source_root / "not-dated.png"
    wrong.write_bytes(b"x")
    with pytest.raises(ValueError, match="dated Ana Live"):
        derive_destination_path(target, wrong)

    impossible = source_root / "2026" / "02" / "31" / "invalid.png"
    impossible.parent.mkdir(parents=True, mode=0o700)
    impossible.write_bytes(b"x")
    with pytest.raises(ValueError, match="dated Ana Live"):
        derive_destination_path(target, impossible)

    real = _source(source_root)
    linked = source_root / "2026" / "08" / "09" / "linked.png"
    linked.symlink_to(real)
    with pytest.raises(ValueError, match="symlink"):
        stable_read_source(linked, [source_root])

    outside = tmp_path / "outside.png"
    outside.write_bytes(b"outside")
    lexical_escape = source_root / ".." / outside.name
    with pytest.raises(ValueError, match="normalized"):
        stable_read_source(lexical_escape, [source_root])

    nested = source_root / "2026"
    ambiguous = dict(target, source_roots=[str(source_root), str(nested)])
    nested.chmod(0o700)
    with pytest.raises(ValueError, match="distinct and non-nested"):
        derive_destination_path(ambiguous, real)

    with pytest.raises(ValueError, match="distinct"):
        normalize_configured_target("ana-live", {
            "destination_root": str(source_root),
            "source_roots": [str(source_root)],
            "layout": "ana-live-dated/v1",
        })

    empty = source_root / "2026" / "08" / "09" / "empty.png"
    empty.write_bytes(b"")
    artifact = tmp_path / "empty-owned.png"
    artifact.write_bytes(b"")
    with pytest.raises(ValueError, match="empty"):
        copy_filesystem_delivery(
            target=target,
            execution_id="a" * 32,
            source_path=str(empty),
            artifact_path=str(artifact),
            artifact_sha256="sha256:" + hashlib.sha256(b"").hexdigest(),
            artifact_size_bytes=0,
        )


def test_scheduler_filesystem_branch_never_loads_gateway_adapter(monkeypatch, tmp_path):
    import cron.scheduler as scheduler

    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    source_root.mkdir(mode=0o700)
    destination_root.mkdir(mode=0o700)
    source = _source(source_root)
    artifact = tmp_path / "owned.png"
    artifact.write_bytes(source.read_bytes())
    target = _target(source_root, destination_root)
    digest = "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()
    manifest = {
        "version": 1,
        "payload": {"path": str(artifact), "source_path": str(source), "sha256": digest,
                    "size_bytes": len(source.read_bytes())},
        "media": [{"path": str(artifact), "source_path": str(source), "sha256": digest,
                   "size_bytes": len(source.read_bytes()), "is_voice": False}],
        "filesystem": [{"target_id": "ana-live", "source_path": str(source),
                        "destination_path": str(destination_root / "2026/08/09/ana-live-test.png")}],
    }
    monkeypatch.setattr(scheduler, "read_delivery_artifact_manifest", lambda _id: manifest)
    monkeypatch.setattr("gateway.config.load_gateway_config", lambda: (_ for _ in ()).throw(
        AssertionError("gateway config must not load")
    ))

    receipts = []
    outcome = scheduler._deliver_result(
        {"id": "ana-live", "deliver": "filesystem:ana-live"},
        f"MEDIA:{artifact}",
        targets=[target],
        receipts=receipts,
        delivery_execution_id="a" * 32,
    )
    assert outcome is None
    assert receipts[0]["transport"] == "filesystem"
    assert receipts[0]["provider_receipt_id"] is None


def test_filesystem_materialization_rejects_lexical_symlink_source(tmp_path):
    import cron.scheduler as scheduler

    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    source_root.mkdir(mode=0o700)
    destination_root.mkdir(mode=0o700)
    real = _source(source_root)
    linked = real.with_name("linked.png")
    linked.symlink_to(real)

    with pytest.raises(ValueError, match="symlink|exactly one MEDIA"):
        scheduler._materialize_delivery_artifact(
            "ana-live", "a" * 32, f"MEDIA:{linked}",
            [_target(source_root, destination_root)],
        )


def test_execution_ledger_accepts_strict_filesystem_receipt_and_recovers_crash(
    monkeypatch, tmp_path,
):
    import cron.executions as executions

    monkeypatch.setattr(executions, "EXECUTIONS_FILE", tmp_path / "home/cron/executions.db")
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    source_root.mkdir(mode=0o700)
    destination_root.mkdir(mode=0o700)
    source = _source(source_root)
    target = _target(source_root, destination_root)
    digest = "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()
    producer = executions.create_execution(
        "ana-live", source="builtin", scheduled_for="2026-08-09T09:10:00+00:00",
    )
    executions.finish_execution(producer["id"], success=True)
    delivery = executions.create_delivery_execution(
        producer_execution_id=producer["id"], artifact_path=str(source),
        artifact_sha256=digest,
        media_artifacts=[{"path": str(source), "sha256": digest,
                          "size_bytes": len(source.read_bytes()), "is_voice": False}],
        delivery_targets=[target],
    )
    manifest = json.loads(delivery["artifact_manifest"])
    assert manifest["media"][0]["source_path"] == str(source)
    frozen = Path(manifest["filesystem"][0]["destination_path"])
    assert not frozen.exists()

    # Simulate a different dead process owning the running delivery. Recovery
    # must resume this exact execution and terminalize it, not create a retry.
    executions.mark_execution_running(delivery["id"])
    with sqlite3.connect(executions.EXECUTIONS_FILE) as conn:
        conn.execute(
            "UPDATE executions SET process_id=?, process_started_at=? WHERE id=?",
            ("dead-owner", -1, delivery["id"]),
        )
    assert executions.recover_interrupted_executions() == 1
    recovered = executions.get_execution(delivery["id"])
    assert recovered["status"] == "completed"
    assert recovered["delivery_status"] == "delivered"
    receipt = json.loads(recovered["delivery_receipts"])[0]
    assert receipt["filesystem_receipt"]["execution_id"] == delivery["id"]
    assert receipt["filesystem_receipt"]["reused"] is False
    assert frozen.read_bytes() == source.read_bytes()
    assert len([r for r in executions.list_executions(job_id="ana-live") if r["kind"] == "delivery"]) == 1


def test_execution_context_exports_exact_typed_target(monkeypatch, tmp_path):
    import cron.scheduler as scheduler
    from gateway.session_context import get_session_env

    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    source_root.mkdir(mode=0o700)
    destination_root.mkdir(mode=0o700)
    target = _target(source_root, destination_root)
    scheduler._install_cron_execution_context({
        "execution_id": "execution-123",
        "id": "ana-live",
        "execution_source": "builtin",
        "scheduled_for": "2026-08-09T09:10:00+00:00",
        "_resolved_delivery_targets": [target],
    })
    try:
        assert json.loads(get_session_env("HERMES_CRON_DELIVERY_TARGETS_JSON")) == [target]
    finally:
        scheduler._clear_cron_execution_context()
