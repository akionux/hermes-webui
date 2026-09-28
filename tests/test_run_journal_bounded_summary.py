"""Task 2: summary probes must stop paying a full-journal parse.

On the target hardware (RK3568, eMMC) an in-flight session's detail load cost
100–112s because every ``find_run_summary`` / ``latest_run_summary`` probe ran
``_read_jsonl`` — ``read_text().splitlines()`` + ``json.loads`` per line — over
a 70–122 MB / 250k-event journal, and the summary cache signature changes on
every append, i.e. cache-miss on every request while streaming.

These tests pin that the probes no longer call the full-parse reader at all:
all summary fields (last_seq/last_event_id/terminal/terminal_state/status) are
derivable from a bounded probe of the file tail + incremental fold-in of the
appended region, and nothing re-reads a region it already counted.
"""
import json
import time

from api import run_journal as rj
from api.run_journal import append_run_event, find_run_summary, latest_run_summary


class _ParseCounter:
    """Wrap _read_jsonl so tests can prove the full-parse path is never hit."""

    def __init__(self, inner):
        self.inner = inner
        self.calls = 0

    def __call__(self, path):
        self.calls += 1
        return self.inner(path)


def _make_big_journal(tmp_path, sid, rid, *, n_fillers=2000, terminal="done"):
    now = 1_750_000_000.0
    append_run_event(sid, rid, "submitted", {"content": "hi"}, session_dir=tmp_path, created_at=now)
    for i in range(n_fillers):
        append_run_event(sid, rid, "reasoning", {"text": "x" * 200}, session_dir=tmp_path,
                         created_at=now + i * 1e-4, seq=i + 2)
    if terminal:
        append_run_event(sid, rid, terminal, {}, session_dir=tmp_path,
                         created_at=now + 900, seq=n_fillers + 2)
    return now


def test_latest_run_summary_never_full_parses_a_large_journal(tmp_path, monkeypatch):
    _make_big_journal(tmp_path, "sid-t2", "stream-a")
    counter = _ParseCounter(rj._read_jsonl)
    monkeypatch.setattr(rj, "_read_jsonl", counter)

    summary = latest_run_summary("sid-t2", "stream-a", session_dir=tmp_path)

    assert counter.calls == 0, "summary probe must not re-parse the whole journal"
    assert summary["terminal"] is True
    assert summary["terminal_state"] == "completed"
    assert summary["last_seq"] == 2002
    assert summary["event_count"] >= 1
    assert summary["last_event_id"] == "stream-a:2002"


def test_running_summary_reports_last_seq_without_terminal(tmp_path, monkeypatch):
    _make_big_journal(tmp_path, "sid-t2", "stream-b", terminal=None)
    counter = _ParseCounter(rj._read_jsonl)
    monkeypatch.setattr(rj, "_read_jsonl", counter)

    summary = latest_run_summary("sid-t2", "stream-b", session_dir=tmp_path)

    assert counter.calls == 0
    assert summary["terminal"] is False
    assert summary["terminal_state"] == "running"
    assert summary["last_seq"] == 2001  # watermark from the tail's last line


def test_probe_folds_appends_without_reparsing_the_head(tmp_path, monkeypatch):
    _make_big_journal(tmp_path, "sid-t2", "stream-c", terminal=None)
    counter = _ParseCounter(rj._read_jsonl)
    monkeypatch.setattr(rj, "_read_jsonl", counter)

    first = latest_run_summary("sid-t2", "stream-c", session_dir=tmp_path)
    append_run_event("sid-t2", "stream-c", "done", {}, session_dir=tmp_path, seq=2002)
    second = latest_run_summary("sid-t2", "stream-c", session_dir=tmp_path)

    assert counter.calls == 0
    assert first["terminal"] is False and first["terminal_state"] == "running"
    assert second["terminal"] is True and second["terminal_state"] == "completed"
    assert second["last_seq"] == 2002


def test_find_run_summary_locates_and_summarizes_without_full_parse(tmp_path, monkeypatch):
    _make_big_journal(tmp_path, "sid-t2", "stream-d")
    counter = _ParseCounter(rj._read_jsonl)
    monkeypatch.setattr(rj, "_read_jsonl", counter)

    summary = find_run_summary("stream-d", session_dir=tmp_path)

    assert counter.calls == 0
    assert summary is not None and summary["session_id"] == "sid-t2"
    assert summary["terminal_state"] == "completed"


def test_empty_journal_summary_is_unknown_not_crash(tmp_path):
    summary = latest_run_summary("sid-t2", "never-existed", session_dir=tmp_path)
    assert summary["terminal"] is False
    assert summary["terminal_state"] == "unknown"
    assert summary["event_count"] == 0
