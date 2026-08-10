"""Strict, configured, create-once filesystem delivery for cron artifacts."""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import re
import stat
import tempfile
from pathlib import Path
from typing import Any, Iterable

FILESYSTEM_TARGET_KIND = "filesystem"
FILESYSTEM_LAYOUT = "ana-live-dated/v1"
FILESYSTEM_RECEIPT_SCHEMA = "hermes-filesystem-copy/v1"
_IMAGE_SUFFIXES = frozenset({".jpeg", ".jpg", ".png", ".webp"})
_TARGET_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_TARGET_KEYS = frozenset({"kind", "target_id", "destination_root", "source_roots", "layout"})
_CONFIG_KEYS = frozenset({"destination_root", "source_roots", "layout"})


def _real_private_directory(
    path: Path, *, label: str, require_private: bool = True,
) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        raise ValueError(f"{label} must be an absolute path")
    try:
        info = expanded.lstat()
    except OSError as exc:
        raise ValueError(f"{label} must be a pre-existing directory") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ValueError(f"{label} must be a real non-symlink directory")
    if require_private and info.st_mode & 0o077:
        raise ValueError(f"{label} must be private (no group or other permissions)")
    resolved = expanded.resolve(strict=True)
    if expanded != resolved:
        raise ValueError(f"{label} must be normalized with no symlink ancestry")
    return resolved


def normalize_configured_target(target_id: str, configured: Any) -> dict[str, Any]:
    """Resolve one trusted registry entry into the exact typed target shape."""
    clean_id = str(target_id or "").strip()
    if not _TARGET_ID_RE.fullmatch(clean_id):
        raise ValueError("configured filesystem delivery target id is invalid")
    if not isinstance(configured, dict) or set(configured) != _CONFIG_KEYS:
        raise ValueError("configured filesystem delivery target is invalid")
    roots = configured.get("source_roots")
    if not isinstance(roots, list) or not roots:
        raise ValueError("configured filesystem delivery target requires source_roots")
    if configured.get("layout") != FILESYSTEM_LAYOUT:
        raise ValueError("configured filesystem delivery target layout is invalid")
    destination_root = _real_private_directory(
        Path(str(configured.get("destination_root") or "")), label="destination root",
    )
    source_roots = [
        _real_private_directory(
            Path(str(root or "")), label="source root",
        ) for root in roots
    ]
    if len(set(source_roots)) != len(source_roots):
        raise ValueError("configured filesystem delivery source roots must be unique")
    if any(
        source == destination_root
        or source in destination_root.parents
        or destination_root in source.parents
        for source in source_roots
    ):
        raise ValueError("configured filesystem delivery roots must be distinct and non-nested")
    if any(
        left in right.parents or right in left.parents
        for index, left in enumerate(source_roots)
        for right in source_roots[index + 1:]
    ):
        raise ValueError("configured filesystem delivery roots must be distinct and non-nested")
    return {
        "kind": FILESYSTEM_TARGET_KIND,
        "target_id": clean_id,
        "destination_root": str(destination_root),
        "source_roots": [str(root) for root in source_roots],
        "layout": FILESYSTEM_LAYOUT,
    }


def normalize_filesystem_target(target: Any) -> dict[str, Any]:
    if not isinstance(target, dict) or set(target) != _TARGET_KEYS:
        raise ValueError("filesystem delivery target has unknown or missing keys")
    if target.get("kind") != FILESYSTEM_TARGET_KIND:
        raise ValueError("filesystem delivery target kind is invalid")
    normalized = normalize_configured_target(
        str(target.get("target_id") or ""),
        {
            "destination_root": target.get("destination_root"),
            "source_roots": target.get("source_roots"),
            "layout": target.get("layout"),
        },
    )
    return normalized


def _assert_no_symlinks(root: Path, path: Path, *, include_leaf: bool = True) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise ValueError("path is outside configured root") from exc
    current = root
    parts = relative.parts if include_leaf else relative.parts[:-1]
    for part in parts:
        current = current / part
        try:
            info = current.lstat()
        except OSError as exc:
            raise ValueError(f"path component does not exist: {current}") from exc
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f"symlink path component is forbidden: {current}")


def _matching_source_roots(source: Path, roots: Iterable[Path]) -> list[Path]:
    matches: list[Path] = []
    for root in roots:
        try:
            source.relative_to(root)
        except ValueError:
            continue
        matches.append(root)
    return matches


def stable_read_source(path: Path | str, source_roots: Iterable[Path | str]) -> tuple[Path, bytes]:
    """Read a regular source through O_NOFOLLOW and prove its metadata stayed stable."""
    lexical = Path(path).expanduser()
    if not lexical.is_absolute():
        raise ValueError("filesystem delivery source must be absolute")
    normalized_lexical = Path(os.path.normpath(str(lexical)))
    if lexical != normalized_lexical:
        raise ValueError("filesystem delivery source must be normalized")
    roots = [Path(root).expanduser().resolve(strict=True) for root in source_roots]
    matches = _matching_source_roots(lexical, roots)
    if len(matches) != 1:
        raise ValueError("source must be physically under exactly one configured source root")
    root = matches[0]
    _assert_no_symlinks(root, lexical)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(lexical, flags)
    except OSError as exc:
        raise ValueError("filesystem delivery source could not be opened without following links") from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("filesystem delivery source must be a regular file")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(fd)
        stable_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
            raise ValueError("filesystem delivery source changed while it was read")
        payload = b"".join(chunks)
        if not payload:
            raise ValueError("filesystem delivery source must not be empty")
        if len(payload) != before.st_size:
            raise ValueError("filesystem delivery source size changed while it was read")
    finally:
        os.close(fd)
    return lexical, payload


def derive_destination_path(target: dict[str, Any], source_path: Path | str) -> Path:
    """Map YYYY/MM/DD[/origin files]/image to configured-root/YYYY/MM/DD/image."""
    normalized = normalize_filesystem_target(target)
    source = Path(source_path).expanduser()
    roots = [Path(root) for root in normalized["source_roots"]]
    matches = _matching_source_roots(source, roots)
    if len(matches) != 1:
        raise ValueError("source must be physically under exactly one configured source root")
    _assert_no_symlinks(matches[0], source)
    relative = source.relative_to(matches[0])
    parts = relative.parts
    if len(parts) == 5 and parts[3] == "origin files":
        year, month, day, _origin, filename = parts
    elif len(parts) == 4:
        year, month, day, filename = parts
    else:
        raise ValueError("source does not match the dated Ana Live image layout")
    if (
        len(year) != 4 or not year.isdigit()
        or len(month) != 2 or not month.isdigit()
        or len(day) != 2 or not day.isdigit()
        or Path(filename).name != filename
        or filename.startswith(".")
        or Path(filename).suffix.lower() not in _IMAGE_SUFFIXES
    ):
        raise ValueError("source does not match the dated Ana Live image layout")
    try:
        dt.date(int(year), int(month), int(day))
    except ValueError as exc:
        raise ValueError("source does not match the dated Ana Live image layout") from exc
    destination_root = Path(normalized["destination_root"])
    destination = destination_root / year / month / day / filename
    if destination.parent == destination_root or destination_root not in destination.parents:
        raise ValueError("derived destination escapes configured destination root")
    return destination


def _read_regular_no_follow(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError(f"delivery artifact is not a regular file: {path}")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


def _ensure_destination_parent(root: Path, parent: Path) -> None:
    relative = parent.relative_to(root)
    current = root
    for part in relative.parts:
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            current.mkdir(mode=0o700)
            info = current.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"destination component must be a real directory: {current}")
        if info.st_mode & 0o077:
            raise ValueError(f"destination directory must be private: {current}")
        if stat.S_IMODE(info.st_mode) != 0o700:
            current.chmod(0o700)


def _verified_existing(path: Path, expected: bytes, digest: str) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise FileExistsError(f"destination exists and is not a regular file: {path}")
    actual = _read_regular_no_follow(path)
    actual_digest = "sha256:" + hashlib.sha256(actual).hexdigest()
    if actual != expected or actual_digest != digest:
        raise FileExistsError(f"destination already exists with different bytes: {path}")
    return True


def copy_filesystem_delivery(
    *, target: dict[str, Any], execution_id: str, source_path: str,
    artifact_path: str, artifact_sha256: str, artifact_size_bytes: int,
    destination_path: str | None = None,
) -> dict[str, Any]:
    """Atomically create or byte-verify one execution-owned image delivery."""
    if not re.fullmatch(r"[a-f0-9]{32}", str(execution_id or "")):
        raise ValueError("filesystem delivery execution ID is invalid")
    normalized = normalize_filesystem_target(target)
    source, source_payload = stable_read_source(source_path, normalized["source_roots"])
    destination = derive_destination_path(normalized, source)
    if destination_path is not None and Path(destination_path) != destination:
        raise ValueError("frozen filesystem destination path does not match canonical mapping")
    artifact = Path(artifact_path).expanduser()
    artifact_payload = _read_regular_no_follow(artifact)
    digest = "sha256:" + hashlib.sha256(artifact_payload).hexdigest()
    if (
        digest != artifact_sha256
        or len(artifact_payload) != artifact_size_bytes
        or source_payload != artifact_payload
    ):
        raise ValueError("source and execution-owned artifact bytes do not match durable proof")

    root = _real_private_directory(Path(normalized["destination_root"]), label="destination root")
    _ensure_destination_parent(root, destination.parent)
    reused = _verified_existing(destination, artifact_payload, digest)
    if reused:
        destination.chmod(0o600)
        if stat.S_IMODE(destination.lstat().st_mode) != 0o600:
            raise ValueError("reused filesystem destination is not private")
    if not reused:
        fd, temp_name = tempfile.mkstemp(prefix=f".{execution_id}.", suffix=".tmp", dir=destination.parent)
        temp = Path(temp_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb", closefd=True) as handle:
                handle.write(artifact_payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temp, destination, follow_symlinks=False)
            except FileExistsError:
                reused = _verified_existing(destination, artifact_payload, digest)
            else:
                directory_fd = os.open(destination.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            temp.unlink(missing_ok=True)
    final = _read_regular_no_follow(destination)
    if final != artifact_payload or "sha256:" + hashlib.sha256(final).hexdigest() != digest:
        raise ValueError("published filesystem destination failed byte verification")
    return {
        "schema": FILESYSTEM_RECEIPT_SCHEMA,
        "execution_id": str(execution_id),
        "source_path": str(source),
        "destination_path": str(destination),
        "sha256": digest,
        "size_bytes": len(final),
        "byte_equal": True,
        "reused": bool(reused),
    }


def validate_filesystem_receipt(
    receipt: Any, *, target: dict[str, Any], execution_id: str,
    source_path: str, destination_path: str, artifact_path: str,
    artifact_sha256: str, artifact_size_bytes: int,
) -> dict[str, Any]:
    """Validate exact filesystem proof against source, owned, and final bytes."""
    required = {
        "schema", "execution_id", "source_path", "destination_path", "sha256",
        "size_bytes", "byte_equal", "reused",
    }
    if not isinstance(receipt, dict) or set(receipt) != required:
        raise ValueError("filesystem receipt must have the exact v1 shape")
    if receipt.get("schema") != FILESYSTEM_RECEIPT_SCHEMA:
        raise ValueError("filesystem receipt schema is invalid")
    if receipt.get("execution_id") != execution_id:
        raise ValueError("filesystem receipt execution ID does not match owning execution")
    if receipt.get("source_path") != source_path:
        raise ValueError("filesystem receipt source path does not match frozen source")
    if receipt.get("destination_path") != destination_path:
        raise ValueError("filesystem receipt destination path does not match frozen destination")
    if receipt.get("sha256") != artifact_sha256:
        raise ValueError("filesystem receipt digest does not match execution artifact")
    if receipt.get("size_bytes") != artifact_size_bytes:
        raise ValueError("filesystem receipt size does not match execution artifact")
    if receipt.get("byte_equal") is not True or type(receipt.get("reused")) is not bool:
        raise ValueError("filesystem receipt byte equality or replay evidence is invalid")
    normalized = normalize_filesystem_target(target)
    source, source_payload = stable_read_source(source_path, normalized["source_roots"])
    if str(source) != source_path or str(derive_destination_path(normalized, source)) != destination_path:
        raise ValueError("filesystem receipt paths do not match canonical mapping")
    artifact_payload = _read_regular_no_follow(Path(artifact_path))
    destination_payload = _read_regular_no_follow(Path(destination_path))
    digest = "sha256:" + hashlib.sha256(artifact_payload).hexdigest()
    if (
        source_payload != artifact_payload
        or destination_payload != artifact_payload
        or digest != artifact_sha256
        or len(artifact_payload) != artifact_size_bytes
    ):
        raise ValueError("filesystem receipt bytes do not match source, execution artifact, and destination")
    return dict(receipt)
