"""Profile-local durable audit ledger for cron execution attempts.

The ledger records what is known about each attempt; it is not a retry queue.
Interrupted attempts become ``unknown`` only after their exact owner process is
proved gone. Terminal states are immutable.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from hermes_constants import get_hermes_home
from hermes_time import now as _hermes_now

EXECUTIONS_FILE = get_hermes_home().resolve() / "cron" / "executions.db"
MAX_TERMINAL_EXECUTIONS = 1000
_TERMINAL_STATES = ("completed", "failed", "unknown")
_lock = threading.RLock()
_PROCESS_ID = uuid.uuid4().hex
SCHEDULER_SOURCES = frozenset({"builtin", "chronos", "direct", "external"})
_CANONICAL_SCHEDULED_FOR_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00$",
)


def require_canonical_scheduled_for(value: Optional[str]) -> str:
    """Require the exact UTC spelling used as durable scheduled-run authority."""
    if not isinstance(value, str) or not _CANONICAL_SCHEDULED_FOR_RE.fullmatch(value):
        raise ValueError("producer execution requires canonical UTC scheduled_for")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            "producer execution requires canonical UTC scheduled_for",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None or parsed.isoformat() != value:
        raise ValueError("producer execution requires canonical UTC scheduled_for")
    return value


def require_scheduler_source(value: Any) -> str:
    source = str(value or "")
    if source not in SCHEDULER_SOURCES:
        raise ValueError("cron execution scheduler source is not allowlisted")
    return source


def _connect() -> sqlite3.Connection:
    EXECUTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(EXECUTIONS_FILE, timeout=5)


def _initialize_schema(conn: sqlite3.Connection) -> None:
    from hermes_state import apply_wal_with_fallback

    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    apply_wal_with_fallback(conn, db_label="cron/executions.db")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS executions (
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL,
             source TEXT NOT NULL,
             process_id TEXT NOT NULL,
             pid INTEGER NOT NULL,
             process_started_at INTEGER,
             status TEXT NOT NULL CHECK(status IN
               ('claimed','running','completed','failed','unknown')),
             claimed_at TEXT NOT NULL,
             started_at TEXT,
             finished_at TEXT,
             error TEXT,
             delivery_status TEXT CHECK(delivery_status IN
               ('suppressed','delivered','failed')),
             delivery_state TEXT,
             delivery_error TEXT,
             delivered_at TEXT,
             output_file TEXT,
             delivery_targets TEXT,
             scheduled_for TEXT,
             kind TEXT NOT NULL DEFAULT 'producer',
             parent_execution_id TEXT,
             artifact_path TEXT,
             artifact_sha256 TEXT,
             artifact_size_bytes INTEGER,
             artifact_manifest TEXT,
             authorized_delivery_targets TEXT,
             delivery_receipts TEXT
           )"""
    )
    existing_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(executions)").fetchall()
    }
    for column, definition in (
        ("delivery_status", "TEXT CHECK(delivery_status IN ('suppressed','delivered','failed'))"),
        ("delivery_state", "TEXT"),
        ("delivery_error", "TEXT"),
        ("delivered_at", "TEXT"),
        ("output_file", "TEXT"),
        ("delivery_targets", "TEXT"),
        ("scheduled_for", "TEXT"),
        ("kind", "TEXT NOT NULL DEFAULT 'producer'"),
        ("parent_execution_id", "TEXT"),
        ("artifact_path", "TEXT"),
        ("artifact_sha256", "TEXT"),
        ("artifact_size_bytes", "INTEGER"),
        ("artifact_manifest", "TEXT"),
        ("authorized_delivery_targets", "TEXT"),
        ("delivery_receipts", "TEXT"),
    ):
        if column not in existing_columns:
            conn.execute(f"ALTER TABLE executions ADD COLUMN {column} {definition}")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_job_claimed "
        "ON executions(job_id, claimed_at DESC, id DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_status_claimed "
        "ON executions(status, claimed_at DESC, id DESC)"
    )


@contextmanager
def _transaction() -> Iterator[sqlite3.Connection]:
    """Open a connection, commit/rollback on exit, always close.

    ``sqlite3.Connection.__enter__``/``__exit__`` only commit or roll back
    the transaction; it does not close the connection. Relying on that alone
    leaks a connection (and its WAL/SHM file descriptors) on every call,
    since closing then depends on the garbage collector. Schema init runs
    inside the ``try`` too, so a PRAGMA/DDL failure after a successful
    ``connect()`` still closes the connection instead of leaking it.
    """
    with _lock:
        conn = _connect()
        try:
            _initialize_schema(conn)
            with conn:
                yield conn
        finally:
            conn.close()


def _record(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    return dict(row) if row is not None else None


def _emit_execution_state(
    record: Optional[Dict[str, Any]], *, delivery_outcome: Optional[str] = None
) -> None:
    """Project durable state to monitoring without affecting ledger behavior."""
    try:
        from agent.monitoring.cron_health import emit_execution_state

        emit_execution_state(record, delivery_outcome=delivery_outcome)
    except Exception:
        pass


def get_execution(execution_id: str) -> Optional[Dict[str, Any]]:
    """Return one durable execution identity without accepting caller metadata."""
    with _transaction() as conn:
        return _record(conn.execute(
            "SELECT * FROM executions WHERE id=?", (str(execution_id),)
        ).fetchone())


def _process_start_time(pid: int) -> Optional[int]:
    try:
        from gateway.status import get_process_start_time
        return get_process_start_time(pid)
    except Exception:
        return None


def _owner_is_live(pid: int, started_at: Optional[int]) -> bool:
    try:
        from gateway.status import _pid_exists
        if not _pid_exists(pid):
            return False
    except Exception:
        return True  # fail safe: inability to prove death must not rewrite state
    if started_at is None:
        return pid == os.getpid()
    current = _process_start_time(pid)
    return current is not None and current == started_at


def _prune_unlocked(conn: sqlite3.Connection) -> None:
    limit = max(0, int(MAX_TERMINAL_EXECUTIONS))
    conn.execute(
        """DELETE FROM executions WHERE id IN (
             SELECT id FROM executions
             WHERE status IN ('completed','failed','unknown')
             ORDER BY claimed_at DESC, id DESC LIMIT -1 OFFSET ?
           )""",
        (limit,),
    )


def create_execution(
    job_id: str, *, source: str, scheduled_for: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist a claimed attempt before executor/provider dispatch."""
    normalized_source = require_scheduler_source(source)
    # This active generation predates scheduler fire-slot authority. Preserve
    # its existing callers while retaining canonical validation when supplied.
    canonical_scheduled_for = (
        require_canonical_scheduled_for(scheduled_for)
        if scheduled_for is not None else None
    )
    now = _hermes_now().isoformat()
    execution_id = uuid.uuid4().hex
    pid = os.getpid()
    with _transaction() as conn:
        conn.execute(
            """INSERT INTO executions
               (id, job_id, source, process_id, pid, process_started_at,
                status, claimed_at, scheduled_for)
               VALUES (?, ?, ?, ?, ?, ?, 'claimed', ?, ?)""",
            (execution_id, str(job_id), normalized_source, _PROCESS_ID, pid,
             _process_start_time(pid), now,
             canonical_scheduled_for),
        )
        row = conn.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone()
    record = _record(row)
    _emit_execution_state(record)
    return record  # type: ignore[return-value]


def create_delivery_execution(
    *,
    producer_execution_id: str,
    artifact_path: str,
    artifact_sha256: str,
    media_artifacts: Optional[List[Dict[str, Any]]] = None,
    delivery_targets: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Claim a delivery attempt only for completed producer bytes."""
    expected_digest = str(artifact_sha256 or "").lower()
    if not expected_digest.startswith("sha256:") or len(expected_digest) != 71:
        raise ValueError("artifact digest is invalid")
    try:
        int(expected_digest[7:], 16)
    except ValueError as exc:
        raise ValueError("artifact digest is invalid") from exc
    resolved = Path(artifact_path).expanduser().resolve(strict=True)
    payload = resolved.read_bytes()
    actual_digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    if actual_digest != expected_digest:
        raise ValueError("artifact digest does not match exact bytes")

    normalized_media = []
    for index, entry in enumerate(media_artifacts or []):
        if not isinstance(entry, dict):
            raise ValueError("media artifact is invalid")
        media_path = Path(str(entry.get("path") or "")).expanduser().resolve(strict=True)
        media_payload = media_path.read_bytes()
        media_digest = str(entry.get("sha256") or "").lower()
        actual_media_digest = "sha256:" + hashlib.sha256(media_payload).hexdigest()
        if media_digest != actual_media_digest or entry.get("size_bytes") != len(media_payload):
            raise ValueError(f"media artifact {index} does not match exact bytes")
        normalized_media.append({
            "source_path": media_path,
            "payload": media_payload,
            "sha256": actual_media_digest,
            "size_bytes": len(media_payload),
            "is_voice": bool(entry.get("is_voice", False)),
        })

    normalized_targets = [
        _normalize_target(target, label="delivery target") for target in delivery_targets
    ]
    if not normalized_targets:
        raise ValueError("delivery execution requires concrete delivery targets")

    filesystem_manifest = []
    filesystem_targets = [
        target for target in normalized_targets if target.get("kind") == "filesystem"
    ]
    if filesystem_targets:
        from cron.filesystem_delivery import derive_destination_path, stable_read_source

        if len(normalized_media) != 1 or normalized_media[0]["is_voice"]:
            raise ValueError("filesystem delivery requires exactly one MEDIA image artifact")
        media_source = normalized_media[0]["source_path"]
        for target in filesystem_targets:
            stable_path, stable_payload = stable_read_source(
                media_source, target["source_roots"],
            )
            if stable_payload != normalized_media[0]["payload"]:
                raise ValueError("filesystem source changed before delivery claim")
            filesystem_manifest.append({
                "target_id": target["target_id"],
                "source_path": str(stable_path),
                "destination_path": str(derive_destination_path(target, stable_path)),
            })

    execution_id = uuid.uuid4().hex
    artifact_dir = EXECUTIONS_FILE.parent / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    try:
        artifact_dir.chmod(0o700)
    except OSError:
        pass
    suffix = resolved.suffix if len(resolved.suffix) <= 16 else ""
    owned_path = artifact_dir / f"{execution_id}{suffix}"
    owned_paths: List[Path] = []
    now = _hermes_now().isoformat()
    pid = os.getpid()
    with _transaction() as conn:
        producer = conn.execute(
            "SELECT * FROM executions WHERE id=?", (str(producer_execution_id),)
        ).fetchone()
        if (producer is None or producer["kind"] != "producer"
                or producer["status"] not in ("completed", "failed")):
            raise ValueError("delivery execution requires a terminal producer execution")
        try:
            def write_owned(path: Path, exact_payload: bytes) -> None:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                owned_paths.append(path)
                with os.fdopen(fd, "wb") as handle:
                    handle.write(exact_payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                try:
                    path.chmod(0o400)
                except OSError:
                    pass

            write_owned(owned_path, payload)
            owned_media = []
            for index, entry in enumerate(normalized_media):
                if len(normalized_media) == 1 and entry["source_path"] == resolved:
                    media_owned_path = owned_path
                else:
                    media_suffix = entry["source_path"].suffix
                    if len(media_suffix) > 16:
                        media_suffix = ""
                    media_owned_path = artifact_dir / (
                        f"{execution_id}-media-{index:03d}{media_suffix}"
                    )
                    write_owned(media_owned_path, entry["payload"])
                owned_media.append({
                    "path": str(media_owned_path),
                    "source_path": str(entry["source_path"]),
                    "sha256": entry["sha256"],
                    "size_bytes": entry["size_bytes"],
                    "is_voice": entry["is_voice"],
                })
            manifest = {
                "version": 1,
                "payload": {
                    "path": str(owned_path),
                    "source_path": str(resolved),
                    "sha256": actual_digest,
                    "size_bytes": len(payload),
                },
                "media": owned_media,
                "filesystem": filesystem_manifest,
            }
            conn.execute(
                """INSERT INTO executions
                   (id, job_id, source, process_id, pid, process_started_at,
                    status, claimed_at, scheduled_for, kind, parent_execution_id,
                    artifact_path, artifact_sha256, artifact_size_bytes, artifact_manifest,
                    authorized_delivery_targets)
                   VALUES (?, ?, ?, ?, ?, ?, 'claimed', ?, ?, 'delivery', ?, ?, ?, ?, ?, ?)""",
                (
                    execution_id, producer["job_id"], producer["source"], _PROCESS_ID, pid,
                    _process_start_time(pid), now, producer["scheduled_for"], producer["id"],
                    str(owned_path), actual_digest, len(payload),
                    json.dumps(manifest, sort_keys=True, separators=(",", ":")),
                    json.dumps(normalized_targets, sort_keys=True, separators=(",", ":")),
                ),
            )
        except BaseException:
            for path in owned_paths:
                path.unlink(missing_ok=True)
            raise
        row = conn.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone()
    record = _record(row)
    _emit_execution_state(record)
    return record  # type: ignore[return-value]


def read_delivery_artifact(execution_id: str) -> bytes:
    """Read execution-owned bytes only after proving their durable digest and size."""
    manifest, payloads = read_delivery_artifacts(execution_id)
    return payloads[str(manifest["payload"]["path"])]


def read_delivery_artifact_manifest(execution_id: str) -> Dict[str, Any]:
    """Return the ordered owned manifest after revalidating every exact file."""
    manifest, _payloads = read_delivery_artifacts(execution_id)
    return manifest


def read_delivery_artifacts(execution_id: str) -> tuple[Dict[str, Any], Dict[str, bytes]]:
    """Read and validate the manifest and every owned payload in one pass."""
    with _transaction() as conn:
        row = conn.execute(
            """SELECT kind, artifact_path, artifact_sha256, artifact_size_bytes,
                      artifact_manifest
               FROM executions WHERE id=?""",
            (str(execution_id),),
        ).fetchone()
    if row is None or row["kind"] != "delivery" or not row["artifact_path"]:
        raise ValueError("delivery artifact does not belong to a delivery execution")
    if row["artifact_manifest"]:
        manifest = json.loads(row["artifact_manifest"])
    else:
        # Existing ledgers predate multi-artifact ownership. Their single
        # artifact remains fully verifiable through the original columns.
        manifest = {
            "version": 0,
            "payload": {
                "path": row["artifact_path"],
                "sha256": row["artifact_sha256"],
                "size_bytes": row["artifact_size_bytes"],
            },
            "media": [],
        }
    entries = [manifest.get("payload")] + list(manifest.get("media") or [])
    payloads = {}
    for entry in entries:
        if not isinstance(entry, dict) or not entry.get("path"):
            raise ValueError("delivery artifact manifest is invalid")
        exact_payload = Path(entry["path"]).read_bytes()
        digest = "sha256:" + hashlib.sha256(exact_payload).hexdigest()
        if digest != entry.get("sha256") or len(exact_payload) != entry.get("size_bytes"):
            raise ValueError("delivery artifact bytes no longer match durable proof")
        payloads[str(entry["path"])] = exact_payload
    return manifest, payloads


def _normalize_target(target: Dict[str, Any], *, label: str) -> Dict[str, Any]:
    if not isinstance(target, dict):
        raise ValueError(f"{label} is invalid")
    if target.get("kind") == "filesystem" or any(
        key in target for key in ("target_id", "destination_root", "source_roots", "layout")
    ):
        from cron.filesystem_delivery import normalize_filesystem_target

        try:
            return normalize_filesystem_target(target)
        except ValueError as exc:
            raise ValueError(f"{label} is invalid: {exc}") from exc
    platform = str(target.get("platform") or "").strip().lower()
    chat_id = str(target.get("chat_id") or "").strip()
    thread_id = target.get("thread_id")
    if not platform or not chat_id:
        raise ValueError(f"{label} is invalid")
    return {
        "platform": platform,
        "chat_id": chat_id,
        "thread_id": None if thread_id is None else str(thread_id),
    }


def _normalize_actual_target(
    target: Dict[str, Any], *, requested: Dict[str, Any], label: str,
) -> Dict[str, Any]:
    if requested.get("kind") != "filesystem":
        return _normalize_target(target, label=label)
    if not isinstance(target, dict) or set(target) != {"kind", "target_id", "destination_path"}:
        raise ValueError(f"{label} is invalid")
    if target.get("kind") != "filesystem" or target.get("target_id") != requested["target_id"]:
        raise ValueError(f"{label} is invalid")
    destination = Path(str(target.get("destination_path") or ""))
    if not destination.is_absolute():
        raise ValueError(f"{label} is invalid")
    return {
        "kind": "filesystem",
        "target_id": requested["target_id"],
        "destination_path": str(destination),
    }


def _validate_delivery_evidence(
    *,
    state: str,
    execution_id: str,
    artifact_path: str,
    artifact_sha256: str,
    artifact_size_bytes: int,
    artifact_manifest: Dict[str, Any],
    authorized_targets: List[Dict[str, Any]],
    actual_targets: Optional[List[Dict[str, Any]]],
    receipts: Optional[List[Dict[str, Any]]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Validate requested authority, actual routes, and per-target evidence once."""
    authorized = [
        _normalize_target(target, label="authorized delivery target")
        for target in authorized_targets
    ]
    normalized_receipts: List[Dict[str, Any]] = []
    for receipt in receipts or []:
        if not isinstance(receipt, dict):
            raise ValueError("delivery receipt is invalid")
        requested = _normalize_target(
            receipt.get("requested_target"), label="delivery receipt requested target",
        )
        actual = _normalize_actual_target(
            receipt.get("actual_target"), requested=requested,
            label="delivery receipt actual target",
        )
        status = str(receipt.get("status") or "")
        transport = str(receipt.get("transport") or "")
        error = None if receipt.get("error") is None else str(receipt.get("error"))
        provider_receipt_id = (
            None if receipt.get("provider_receipt_id") is None
            else str(receipt.get("provider_receipt_id")).strip() or None
        )
        is_filesystem = requested.get("kind") == "filesystem"
        if status not in ("delivered", "failed", "ambiguous"):
            raise ValueError("delivery receipt status is invalid")
        allowed_transports = ("filesystem", "none") if is_filesystem else ("live", "standalone", "none")
        if transport not in allowed_transports:
            raise ValueError("delivery receipt transport is invalid")
        if status == "delivered" and error is not None:
            raise ValueError("delivered receipt cannot carry an error")
        if status == "delivered" and transport == "none":
            raise ValueError("delivered receipt requires a dispatched transport")
        if status == "delivered" and not is_filesystem and not provider_receipt_id:
            raise ValueError("delivered receipt requires provider receipt evidence")
        if is_filesystem and provider_receipt_id is not None:
            raise ValueError("filesystem receipt cannot carry provider receipt evidence")
        if status in ("failed", "ambiguous") and not error:
            raise ValueError(f"{status} receipt requires error evidence")
        if status == "ambiguous" and transport == "none":
            raise ValueError("ambiguous receipt requires a dispatched transport")
        if requested not in authorized:
            raise ValueError("delivery receipts do not match authorized targets")

        filesystem_receipt = receipt.get("filesystem_receipt")
        if is_filesystem:
            if status == "ambiguous":
                raise ValueError("filesystem delivery outcome cannot be ambiguous")
            frozen = next((entry for entry in artifact_manifest.get("filesystem", [])
                           if entry.get("target_id") == requested["target_id"]), None)
            if frozen is None or actual["destination_path"] != frozen.get("destination_path"):
                raise ValueError("delivery receipt actual target is outside requested route")
            if status == "delivered":
                if transport != "filesystem":
                    raise ValueError("delivered filesystem receipt requires filesystem transport")
                from cron.filesystem_delivery import validate_filesystem_receipt

                filesystem_receipt = validate_filesystem_receipt(
                    filesystem_receipt,
                    target=requested,
                    execution_id=execution_id,
                    source_path=str(frozen["source_path"]),
                    destination_path=str(frozen["destination_path"]),
                    artifact_path=artifact_path,
                    artifact_sha256=artifact_sha256,
                    artifact_size_bytes=artifact_size_bytes,
                )
            elif filesystem_receipt is not None or transport != "none":
                raise ValueError("failed filesystem receipt cannot fabricate copy evidence")
        elif requested["platform"] != actual["platform"] or requested["chat_id"] != actual["chat_id"]:
            raise ValueError("delivery receipt actual target is outside requested route")

        normalized = {
            "requested_target": requested,
            "actual_target": actual,
            "status": status,
            "transport": transport,
            "error": error,
            "provider_receipt_id": provider_receipt_id,
        }
        if is_filesystem:
            normalized["filesystem_receipt"] = filesystem_receipt
        normalized_receipts.append(normalized)

    requested_evidence = [receipt["requested_target"] for receipt in normalized_receipts]
    if requested_evidence != authorized:
        raise ValueError("delivery receipts must preserve authorized requested target order")
    confirmed_actual = [
        receipt["actual_target"] for receipt in normalized_receipts
        if receipt["status"] == "delivered"
    ]
    normalized_actual = []
    for target in actual_targets or []:
        matched = next((receipt["requested_target"] for receipt in normalized_receipts
                        if receipt["actual_target"] == target), None)
        if matched is None:
            raise ValueError("delivery targets must contain only confirmed actual targets")
        normalized_actual.append(_normalize_actual_target(
            target, requested=matched, label="actual delivery target",
        ))
    delivered_provider_ids = [
        receipt["provider_receipt_id"] for receipt in normalized_receipts
        if receipt["status"] == "delivered"
        and receipt["requested_target"].get("kind") != "filesystem"
    ]
    if len(set(delivered_provider_ids)) != len(delivered_provider_ids):
        raise ValueError("delivery provider receipt IDs must be unique")
    if normalized_actual != confirmed_actual:
        raise ValueError("delivery targets must contain only confirmed actual targets")
    statuses = [receipt["status"] for receipt in normalized_receipts]
    if state == "delivered" and (not statuses or any(status != "delivered" for status in statuses)):
        raise ValueError("delivered execution requires confirmed actual-target receipts")
    if state == "failed" and ("failed" not in statuses or "ambiguous" in statuses):
        raise ValueError("failed execution requires non-ambiguous failure evidence")
    if state == "ambiguous" and "ambiguous" not in statuses:
        raise ValueError("ambiguous execution requires ambiguous receipt evidence")
    return normalized_actual, normalized_receipts


def mark_execution_running(execution_id: str) -> Optional[Dict[str, Any]]:
    """Transition one claimed attempt to running exactly once."""
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions SET status='running', started_at=?
               WHERE id=? AND status='claimed'""",
            (now, execution_id),
        )
        if cur.rowcount != 1:
            return None
        record = _record(conn.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone())
    _emit_execution_state(record)
    return record


def finish_execution(
    execution_id: str, *, success: bool, error: Optional[str] = None,
    delivery_outcome: Optional[str] = None,
    delivery_status: Optional[str] = None,
    delivery_error: Optional[str] = None,
    output_file: Optional[str] = None,
    delivery_targets: Optional[List[Dict[str, Any]]] = None,
    delivery_receipts: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """Write a terminal result once; terminal attempts cannot be rewritten."""
    now = _hermes_now().isoformat()
    status = "completed" if success else "failed"
    detail = None if success else (str(error) if error else "unknown failure")
    if delivery_status not in (None, "suppressed", "delivered", "failed"):
        raise ValueError("delivery_status is invalid")
    if delivery_status == "delivered" and delivery_error:
        raise ValueError("delivered execution cannot carry delivery_error")
    delivery_detail = str(delivery_error) if delivery_error else None
    delivered_at = now if delivery_status == "delivered" else None
    output_path = str(output_file) if output_file else None
    with _transaction() as conn:
        existing = conn.execute(
            """SELECT kind, authorized_delivery_targets, artifact_path,
                      artifact_sha256, artifact_size_bytes, artifact_manifest
               FROM executions WHERE id=?""",
            (str(execution_id),),
        ).fetchone()
        if existing is not None and existing["kind"] == "delivery" and delivery_status is None:
            raise ValueError("delivery execution requires validated terminal evidence")
        if existing is not None and existing["kind"] == "delivery":
            if delivery_status == "delivered" and not success:
                raise ValueError("success must agree with delivered delivery status")
            if delivery_status == "failed" and success:
                raise ValueError("success must agree with failed delivery status")
        normalized_targets: List[Dict[str, Any]] = []
        normalized_receipts: List[Dict[str, Any]] = []
        if delivery_status in ("delivered", "failed"):
            if existing is None or existing["kind"] != "delivery":
                raise ValueError("delivery evidence requires a delivery execution")
            authorized = json.loads(existing["authorized_delivery_targets"] or "[]")
            normalized_targets, normalized_receipts = _validate_delivery_evidence(
                state=delivery_status,
                execution_id=str(execution_id),
                artifact_path=str(existing["artifact_path"]),
                artifact_sha256=str(existing["artifact_sha256"]),
                artifact_size_bytes=int(existing["artifact_size_bytes"]),
                artifact_manifest=json.loads(existing["artifact_manifest"] or "{}"),
                authorized_targets=authorized,
                actual_targets=delivery_targets,
                receipts=delivery_receipts,
            )
        elif delivery_targets or delivery_receipts:
            raise ValueError("producer execution cannot carry delivery evidence")
        targets_json = json.dumps(
            normalized_targets, sort_keys=True, separators=(",", ":"),
        ) if normalized_targets else None
        receipts_json = json.dumps(
            normalized_receipts, sort_keys=True, separators=(",", ":"),
        ) if normalized_receipts else None
        cur = conn.execute(
            """UPDATE executions
               SET status=?, finished_at=?, error=?, delivery_status=?, delivery_state=?,
                   delivery_error=?, delivered_at=?, output_file=?, delivery_targets=?,
                   delivery_receipts=?
               WHERE id=? AND status IN ('claimed','running')""",
            (status, now, detail, delivery_status, delivery_status, delivery_detail,
             delivered_at, output_path, targets_json, receipts_json, execution_id),
        )
        if cur.rowcount != 1:
            return None
        _prune_unlocked(conn)
        record = _record(conn.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone())
    _emit_execution_state(record, delivery_outcome=delivery_outcome)
    return record


def mark_execution_ambiguous(
    execution_id: str, *, error: str,
    delivery_receipts: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """Terminalize an in-flight delivery whose external effect is unknowable."""
    now = _hermes_now().isoformat()
    detail = str(error or "delivery outcome is ambiguous")
    with _transaction() as conn:
        row = conn.execute(
            """SELECT kind, authorized_delivery_targets, artifact_path,
                      artifact_sha256, artifact_size_bytes, artifact_manifest
               FROM executions WHERE id=?""",
            (str(execution_id),),
        ).fetchone()
        if row is None or row["kind"] != "delivery":
            raise ValueError("ambiguous outcome is valid only for a delivery execution")
        actual_targets, normalized_receipts = _validate_delivery_evidence(
            state="ambiguous",
            execution_id=str(execution_id),
            artifact_path=str(row["artifact_path"]),
            artifact_sha256=str(row["artifact_sha256"]),
            artifact_size_bytes=int(row["artifact_size_bytes"]),
            artifact_manifest=json.loads(row["artifact_manifest"] or "{}"),
            authorized_targets=json.loads(row["authorized_delivery_targets"] or "[]"),
            actual_targets=[
                receipt.get("actual_target") for receipt in delivery_receipts or []
                if isinstance(receipt, dict) and receipt.get("status") == "delivered"
            ],
            receipts=delivery_receipts,
        )
        targets_json = json.dumps(
            actual_targets, sort_keys=True, separators=(",", ":"),
        ) if actual_targets else None
        receipts_json = json.dumps(
            normalized_receipts, sort_keys=True, separators=(",", ":"),
        )
        cur = conn.execute(
            """UPDATE executions
               SET status='unknown', finished_at=?, error=?, delivery_state='ambiguous',
                   delivery_error=?, delivery_targets=?, delivery_receipts=?
               WHERE id=? AND status IN ('claimed','running')""",
            (now, detail, detail, targets_json, receipts_json, str(execution_id)),
        )
        if cur.rowcount != 1:
            return None
        _prune_unlocked(conn)
        record = _record(conn.execute(
            "SELECT * FROM executions WHERE id=?", (str(execution_id),)
        ).fetchone())
    _emit_execution_state(record)
    return record


def _recover_filesystem_execution(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Resume or reconcile one abandoned filesystem-only delivery execution."""
    targets = json.loads(row.get("authorized_delivery_targets") or "[]")
    if not targets or any(target.get("kind") != "filesystem" for target in targets):
        return None
    manifest = json.loads(row.get("artifact_manifest") or "{}")
    media = manifest.get("media") or []
    frozen_entries = manifest.get("filesystem") or []
    if len(media) != 1:
        raise ValueError("filesystem recovery requires exactly one frozen MEDIA artifact")
    owned = media[0]
    receipts: List[Dict[str, Any]] = []
    actual_targets: List[Dict[str, Any]] = []
    failure: Optional[str] = None
    from cron.filesystem_delivery import copy_filesystem_delivery

    for target in targets:
        frozen = next((entry for entry in frozen_entries
                       if entry.get("target_id") == target.get("target_id")), None)
        if frozen is None:
            failure = "filesystem recovery is missing its frozen destination path"
            destination = str(Path(target["destination_root"]) / ".invalid")
        else:
            destination = str(frozen["destination_path"])
        actual = {
            "kind": "filesystem",
            "target_id": target["target_id"],
            "destination_path": destination,
        }
        if failure is None:
            try:
                proof = copy_filesystem_delivery(
                    target=target,
                    execution_id=str(row["id"]),
                    source_path=str(frozen["source_path"]),
                    artifact_path=str(owned["path"]),
                    artifact_sha256=str(owned["sha256"]),
                    artifact_size_bytes=int(owned["size_bytes"]),
                    destination_path=destination,
                )
            except Exception as exc:
                failure = f"filesystem crash recovery failed: {exc}"
            else:
                actual_targets.append(actual)
                receipts.append({
                    "requested_target": target,
                    "actual_target": actual,
                    "status": "delivered",
                    "transport": "filesystem",
                    "error": None,
                    "provider_receipt_id": None,
                    "filesystem_receipt": proof,
                })
                continue
        receipts.append({
            "requested_target": target,
            "actual_target": actual,
            "status": "failed",
            "transport": "none",
            "error": failure,
            "provider_receipt_id": None,
            "filesystem_receipt": None,
        })

    if failure is None:
        return finish_execution(
            str(row["id"]), success=True, delivery_status="delivered",
            delivery_targets=actual_targets, delivery_receipts=receipts,
        )
    return finish_execution(
        str(row["id"]), success=False, error=failure, delivery_status="failed",
        delivery_error=failure, delivery_targets=actual_targets,
        delivery_receipts=receipts,
    )


def recover_interrupted_executions() -> int:
    """Recover filesystem copies; classify other abandoned attempts as unknown."""
    with _transaction() as conn:
        candidates = [dict(row) for row in conn.execute(
            "SELECT * FROM executions WHERE status IN ('claimed','running')"
        ).fetchall()]

    changed = 0
    recovered: List[Dict[str, Any]] = []
    for row in candidates:
        if row["process_id"] == _PROCESS_ID:
            continue
        if _owner_is_live(int(row["pid"]), row["process_started_at"]):
            continue
        targets = json.loads(row.get("authorized_delivery_targets") or "[]")
        filesystem_only = (
            row.get("kind") == "delivery"
            and bool(targets)
            and all(target.get("kind") == "filesystem" for target in targets)
        )
        if filesystem_only:
            try:
                record = _recover_filesystem_execution(row)
            except Exception as exc:
                # A malformed frozen row still terminalizes as a known filesystem
                # failure; it is never broadened into provider retry semantics.
                detail = f"filesystem crash recovery failed: {exc}"
                frozen = (json.loads(row.get("artifact_manifest") or "{}")
                          .get("filesystem") or [])
                receipts = []
                for target in targets:
                    entry = next((item for item in frozen
                                  if item.get("target_id") == target.get("target_id")), {})
                    receipts.append({
                        "requested_target": target,
                        "actual_target": {
                            "kind": "filesystem",
                            "target_id": target["target_id"],
                            "destination_path": str(entry.get("destination_path") or
                                                    Path(target["destination_root"]) / ".invalid"),
                        },
                        "status": "failed", "transport": "none", "error": detail,
                        "provider_receipt_id": None, "filesystem_receipt": None,
                    })
                record = finish_execution(
                    str(row["id"]), success=False, error=detail,
                    delivery_status="failed", delivery_error=detail,
                    delivery_receipts=receipts,
                )
            if record is not None:
                changed += 1
            continue

        now = _hermes_now().isoformat()
        with _transaction() as conn:
            cur = conn.execute(
                """UPDATE executions SET status='unknown', finished_at=?, error=?
                   WHERE id=? AND status IN ('claimed','running')""",
                (now,
                 "Scheduler restarted after this execution's owner exited before a durable "
                 "terminal state; whether side effects ran is unknown.",
                 row["id"]),
            )
            if cur.rowcount:
                changed += 1
                record = _record(conn.execute(
                    "SELECT * FROM executions WHERE id=?", (row["id"],)
                ).fetchone())
                if record is not None:
                    recovered.append(record)
                _prune_unlocked(conn)
    for record in recovered:
        _emit_execution_state(record)
    return changed


def list_executions(
    *, job_id: Optional[str] = None, limit: int = 50,
    before_claimed_at: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return indexed, newest-first execution history with cursor pagination."""
    clauses: List[str] = []
    params: List[Any] = []
    if job_id is not None:
        clauses.append("job_id=?")
        params.append(str(job_id))
    if before_claimed_at is not None:
        clauses.append("claimed_at < ?")
        params.append(str(before_claimed_at))
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(max(1, min(int(limit), 500)))
    with _transaction() as conn:
        rows = conn.execute(
            "SELECT * FROM executions" + where
            + " ORDER BY claimed_at DESC, id DESC LIMIT ?",
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def latest_execution(job_id: str) -> Optional[Dict[str, Any]]:
    rows = list_executions(job_id=job_id, limit=1)
    return rows[0] if rows else None


def latest_executions(job_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Load latest execution for many jobs in one indexed query."""
    clean = [str(job_id) for job_id in dict.fromkeys(job_ids) if job_id]
    if not clean:
        return {}
    placeholders = ",".join("?" for _ in clean)
    with _transaction() as conn:
        rows = conn.execute(
            f"""SELECT e.* FROM executions e
                WHERE e.job_id IN ({placeholders})
                  AND e.id=(SELECT e2.id FROM executions e2
                            WHERE e2.job_id=e.job_id
                            ORDER BY e2.claimed_at DESC, e2.id DESC LIMIT 1)""",
            clean,
        ).fetchall()
    return {row["job_id"]: dict(row) for row in rows}
