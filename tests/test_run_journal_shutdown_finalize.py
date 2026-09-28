"""Task 5: a service restart must not leave a killed turn looking 'running'.

Turns run as children of server.py, so `systemctl --user restart hermes-webui`
kills them mid-stream WITHOUT done/stream_end. Reload recovery (the
stale_interrupted_event path) synthesizes the interrupted notice from the run
summary, but the journal itself stayed non-terminal — auditors and future
callers had to reason about an "open" run that could never close. On shutdown,
every ACTIVE_RUNS entry whose journal lacks a terminal row now gets the
interrupted terminal row appended (idempotent; already-terminal runs untouched).
"""
import time

from api import run_journal as rj
from api.run_journal import append_run_event, latest_run_summary


def test_shutdown_finalize_writes_terminal_row_for_open_runs(tmp_path):
    old = time.time() - 60
    append_run_event("sid-x", "stream-x", "submitted", {"content": "go"},
                     session_dir=tmp_path, created_at=old)
    runs = {"stream-x": {"session_id": "sid-x", "phase": "running", "started_at": old}}

    res = rj.finalize_active_runs_on_shutdown(runs, session_dir=tmp_path)

    assert res["finalized"] == ["stream-x"]
    summary = latest_run_summary("sid-x", "stream-x", session_dir=tmp_path)
    assert summary["terminal"] is True
    # apperror + payload.type=interrupted classifies as a crash-style terminal.
    assert summary["terminal_state"] == "interrupted-by-crash"


def test_shutdown_finalize_skips_already_terminal_and_malformed(tmp_path):
    old = time.time() - 60
    append_run_event("sid-y", "run-done", "done", {}, session_dir=tmp_path, created_at=old)
    runs = {"run-done": {"session_id": "sid-y", "phase": "running"}}

    res = rj.finalize_active_runs_on_shutdown(runs, session_dir=tmp_path)

    assert res["finalized"] == []
    assert res["skipped_terminal"] == ["run-done"]


def test_shutdown_finalize_is_idempotent(tmp_path):
    old = time.time() - 60
    append_run_event("sid-z", "stream-z", "token", {"text": "a"}, session_dir=tmp_path, created_at=old)
    runs = {"stream-z": {"session_id": "sid-z", "phase": "running"}}

    first = rj.finalize_active_runs_on_shutdown(runs, session_dir=tmp_path)
    second = rj.finalize_active_runs_on_shutdown(runs, session_dir=tmp_path)

    assert first["finalized"] == ["stream-z"]
    assert second["finalized"] == [] and second["skipped_terminal"] == ["stream-z"]
