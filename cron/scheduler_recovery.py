"""Opt-in idle recovery command, independently fenced for every ticked home."""
import logging
import os
import subprocess
import threading
import time

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)
_INTERVAL = 60.0
_lock = threading.Lock()
_last_scan = {}
_children = {}


def maybe_dispatch():
    """Launch at most one recovery child per home; never wait on the tick thread."""
    from hermes_cli.config import load_config_readonly

    home = get_hermes_home().resolve()
    key = str(home)
    with _lock:
        child = _children.get(key)
        if child is not None and child.poll() is None:
            return
        now = time.monotonic()
        if now - _last_scan.get(key, float('-inf')) < _INTERVAL:
            return
        _last_scan[key] = now
        try:
            command = (load_config_readonly().get('cron') or {}).get('recovery_command')
            if not command:
                return
            if not isinstance(command, list) or not all(isinstance(arg, str) and arg for arg in command):
                raise ValueError('cron.recovery_command must be a nonempty argv list')
            env = os.environ.copy()
            env['HERMES_HOME'] = key
            log_dir = home / 'cron'
            log_dir.mkdir(parents=True, exist_ok=True)
            with (log_dir / 'recovery.log').open('ab') as output:
                child = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                         stdout=output, stderr=subprocess.STDOUT,
                                         start_new_session=True)
            _children[key] = child
            # Reap without waiting for the next tick, even if the home leaves the tick set.
            threading.Thread(target=child.wait, daemon=True, name='cron-recovery-reaper').start()
        except Exception:
            logger.exception('Idle recovery dispatch failed for %s', home)
