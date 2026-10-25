"""Narrow TeamRunner resweep prejoin custody hook; no provider/model calls."""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ADMISSION_LOG = 'logs/routine-dispatch-admission.jsonl'


def _sha256(path):
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except (OSError, TypeError):
        return None


def _record_admission(row):
    """Append one allowlisted row; telemetry never blocks or alters dispatch."""
    home = os.environ.get('HERMES_HOME') or str(Path.home() / '.hermes')
    try:
        target = Path(home) / ADMISSION_LOG
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(row, sort_keys=True) + '\n')
    except OSError:
        pass


def admit_prejoin(children, parent, live_delegation_id, live_paths):
    """TeamRunner-stage dispatch admission: record why, then checkpoint if admitted.

    Returns a refusal message (children must not run) or None. The row carries
    only process/import-generation identity, the three predicate results and the
    outcome: never the environment, prompt, task text or credentials.
    """
    routine = os.environ.get('ROUTINE_TEAM_ROUTINE') or ''
    source = os.environ.get('HERMES_SESSION_SOURCE') or ''
    depth = getattr(parent, '_delegate_depth', 0)
    predicate = {'routine': routine == 'resweep-rotation', 'source': source == 'cron-team-doer',
                 'depth': depth == 0}
    admitted = all(predicate.values())
    run_dir = os.environ.get('ROUTINE_TEAM_RUN_DIR') or ''
    dispatcher = getattr(sys.modules.get('tools.delegate_tool'), '__file__', None)
    row = {'schema_version': 1, 'recorded_at': datetime.now(timezone.utc).isoformat(),
           'pid': os.getpid(), 'ppid': os.getppid(), 'executable': sys.executable,
           'python': sys.version.split()[0], 'dispatcher_module': dispatcher,
           'dispatcher_sha256': _sha256(dispatcher), 'helper_module': __file__,
           'helper_sha256': _sha256(__file__),
           'occurrence': Path(run_dir).name if run_dir else None,
           'routine': routine[:64] if re.fullmatch(r'[a-z0-9-]*', routine) else 'unrecognized',
           'source': source if source.startswith('cron-team-') else ('unset' if not source else 'other'),
           'delegate_depth': depth if isinstance(depth, int) else None, 'predicate': predicate,
           'admitted': admitted, 'child_count': len(children or ()),
           'live_delegation_id': live_delegation_id}
    refusal = None
    if not admitted:
        row['outcome'] = 'not-admitted'
    else:
        try:
            persist_prejoin(children, parent, live_delegation_id, live_paths)
            row['outcome'] = 'published'
        except Exception as exc:
            row['outcome'] = f'refused:{type(exc).__name__}'
            refusal = f'Resweep prejoin checkpoint refused: {type(exc).__name__}'
    _record_admission(row)
    return refusal


def persist_prejoin(children, parent, live_delegation_id, live_paths):
    """Bind constructed identities before _run_batch can execute or join them.

    No-op outside the root resweep doer. Never overwrite a prior dispatch;
    construction is not execution and no completion/termination is inferred.
    """
    if (os.environ.get('ROUTINE_TEAM_ROUTINE') != 'resweep-rotation'
            or os.environ.get('HERMES_SESSION_SOURCE') != 'cron-team-doer'
            or getattr(parent, '_delegate_depth', 0) != 0):
        return
    root = os.environ.get('ROUTINE_TEAM_RUN_DIR')
    if not root or not Path(root).is_absolute():
        raise ValueError('prejoin occurrence directory missing')
    artifacts = Path(root) / 'doer-artifacts'
    receipt = json.loads((artifacts / 'delegation-receipt.json').read_text())
    members = receipt.get('members', [])
    if (not children or len(children) != receipt.get('expected_child_count')
            or len(members) != len(children) or len(live_paths) != len(children)):
        raise ValueError('prejoin selected membership/count mismatch')
    parent_id = getattr(parent, 'session_id', None)
    if not parent_id or not live_delegation_id:
        raise ValueError('prejoin parent/dispatch identity missing')
    rows = []
    sessions, targets = set(), set()
    for position, ((index, task, child), member, log) in enumerate(zip(children, members, live_paths)):
        target = member.get('target_identity')
        session = getattr(child, 'session_id', None)
        model = getattr(child, 'model', None)
        task_text = str(task.get('goal', '')) + '\n' + str(task.get('context', ''))
        if (index != position or member.get('dispatch_order') != position + 1
                or not isinstance(target, str) or not target
                or not re.search(r'(?<![\w./-])' + re.escape(target) + r'(?![\w./-])', task_text)
                or member.get('completion_status') != 'pending' or member.get('child_identity')
                or not session or not model or session in sessions or target in targets
                or not log or not Path(log).is_file()):
            raise ValueError('prejoin selected identity/order/child handle mismatch')
        sessions.add(session)
        targets.add(target)
        rows.append({'dispatch_order': position + 1, 'target_identity': target,
                     'child_kind': member.get('child_kind'), 'child_session_id': session,
                     'child_model': model, 'child_log': str(log),
                     'subagent_id': getattr(child, '_subagent_id', None),
                     'completion_status': 'constructed'})
    payload = {'schema_version': 1, 'phase': 'prejoin', 'occurrence_dir': str(Path(root).resolve()),
               'recorded_at': datetime.now(timezone.utc).isoformat(), 'parent_session_id': parent_id,
               'live_delegation_id': live_delegation_id, 'expected_child_count': len(rows), 'members': rows}
    # Write and fsync a private sibling, then link exclusively: all bytes appear
    # atomically, and a repeat dispatch cannot replace its predecessor's proof.
    fd, name = tempfile.mkstemp(prefix='.prejoin-', dir=artifacts)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(payload, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.link(name, artifacts / 'prejoin-dispatch.json')
        directory_fd = os.open(artifacts, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        Path(name).unlink(missing_ok=True)
