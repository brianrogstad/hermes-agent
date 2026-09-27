"""A readable nonempty file is never reported empty.

Incident (2026-08-15, indie-game-progress cron): ``read_file`` and terminal
output both called a 16 KB SKILL.md empty while an independent Python read
saw every byte. The shell read paths parsed ``wc -c`` and fell back to
``file_size = 0`` when the reply carried no usable count, so a transport that
returned exit 0 with lost/garbled stdout became "File is empty (0 bytes)".

These tests pin both halves: on a real local shell, ``read_file`` (native and
shell paths) and terminal output agree with direct Python on existence, byte
size and decoded content for a nonempty UTF-8 fixture with punctuation and
long lines; and a size probe that comes back blank or garbled is an error,
never a zero-byte file.
"""

import re
from pathlib import Path

import pytest

from tools.environments.local import LocalEnvironment
from tools.file_operations import ShellFileOperations
from tests.tools.file_ops_fakes import READ_SENTINEL_RE

pytestmark = pytest.mark.platforms("posix")

_GUTTER = re.compile(r"^\d+\|", re.MULTILINE)


def _fixture_bytes() -> bytes:
    lines = [
        "---",
        "name: indie-game-progress",
        'description: "Daily track-aware queue builder — takes one step; preserves Ana\'s prose."',
        "---",
        "",
        "## Required-source read recovery",
        "Never report a required file missing/empty from one tool result: `exists`, `st_size`, `read_bytes`.",
        "Punctuation: ¿¡ «» “quotes” ‘single’ … – — § ¶ • © ™ ✓ → ← ≠ ≤ ≥ $&*(){}[]|\\;:<>?/~`",
        "Non-ASCII: café naïve Zürich 東京 한국어 emoji-free ✅-free",
        "long: " + ("abcdefghij—" * 150),  # ~1,650 chars, well past 1 KB of UTF-8
        "tail line without trailing punctuation",
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


@pytest.fixture(scope="module")
def _local_env(tmp_path_factory):
    return LocalEnvironment(cwd=str(tmp_path_factory.mktemp("false-empty")))


@pytest.fixture
def fixture_path(tmp_path, _local_env):
    _local_env.cwd = str(tmp_path)
    p = tmp_path / "SKILL.md"
    p.write_bytes(_fixture_bytes())
    return p


def _python_truth(p: Path):
    data = p.read_bytes()
    return p.exists(), p.stat().st_size, data.decode("utf-8")


@pytest.mark.parametrize("native", ["1", "0"], ids=["native", "shell"])
def test_read_file_agrees_with_python(fixture_path, _local_env, monkeypatch, native):
    monkeypatch.setenv("HERMES_NATIVE_FILE_READ", native)
    exists, size, text = _python_truth(fixture_path)
    assert exists and size > 1000

    r = ShellFileOperations(_local_env, cwd=str(fixture_path.parent)).read_file(str(fixture_path))

    assert r.error is None
    assert r.file_size == size
    assert r.hint != "File is empty (0 bytes)."
    assert _GUTTER.sub("", r.content) + "\n" == text
    assert r.total_lines == text.count("\n")


def test_terminal_output_agrees_with_python(fixture_path, _local_env):
    exists, size, text = _python_truth(fixture_path)
    q = f"'{fixture_path}'"

    wc = _local_env.execute(f"wc -c < {q}")
    assert wc["returncode"] == 0 and int(wc["output"].strip()) == size

    cat = _local_env.execute(f"cat {q}")
    assert cat["returncode"] == 0
    assert cat["output"].strip() != ""
    assert cat["output"].rstrip("\n") == text.rstrip("\n")


class _LostStdoutEnv:
    """A backend whose shell runs (exit 0) but whose stdout never arrives, or
    arrives as noise with no byte count: the transport shape behind the incident."""

    cwd = "/"

    def __init__(self, reply=""):
        self.reply = reply

    def execute(self, command, *args, **kwargs):
        return {"output": self.reply, "returncode": 0}


@pytest.mark.parametrize("reply", ["", "Last login: Sat Aug 15\n"], ids=["blank", "noise"])
def test_lost_stdout_is_never_reported_empty(fixture_path, monkeypatch, reply):
    monkeypatch.setenv("HERMES_NATIVE_FILE_READ", "0")
    r = ShellFileOperations(_LostStdoutEnv(reply), cwd="/").read_file(str(fixture_path))
    assert r.error, "an unreadable size must surface as an error"
    assert "not an empty file" in r.error
    assert r.hint != "File is empty (0 bytes)."


def test_compound_probe_with_garbled_size_does_not_claim_zero_bytes(fixture_path, monkeypatch):
    """Sentinel-fenced reply whose size segment is garbage: the compound path must
    not parse that as 0 bytes (which discards the page as "empty")."""
    monkeypatch.setenv("HERMES_NATIVE_FILE_READ", "0")

    class _GarbledSizeEnv(_LostStdoutEnv):
        def execute(self, command, *args, **kwargs):
            m = READ_SENTINEL_RE.search(command)
            if m:
                s = m.group(0)
                page = "1|x\n"
                return {"output": f"garbled\n{s}\n{s}\n{page}{s}\n1\n{s}\n1\n{s}\n0 0\n",
                        "returncode": 0}
            return {"output": "", "returncode": 0}  # sequential fallback also loses stdout

    r = ShellFileOperations(_GarbledSizeEnv(), cwd="/").read_file(str(fixture_path))
    assert r.hint != "File is empty (0 bytes)."
    assert r.file_size != 0 or r.error
