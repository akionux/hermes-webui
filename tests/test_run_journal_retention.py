"""Retention GC for finished run journals (turn-journal.md step 5).

The fork appends every SSE event to ``_run_journal/<sid>/<stream>.jsonl`` but,
until now, only ever deleted journals when the *session* was deleted
(``delete_run_journal`` had no other caller). On weak hardware the journal
corpus (measured ~130 MB/day) then dominates both disk and the live-snapshot
re-read path. These tests pin the app-internal retention:

* only runs whose TERMINAL event is older than the retention window disappear;
* a run still in flight (no terminal event, or whose stream is still active)
  is never touched, regardless of file age;
* the pass is idempotent and never raises on malformed/partial journals.
"""
import json
import time

from api.run_journal import prune_run_journals


def _append(sid, rid, event_name, session_dir, *, created_at, seq=1, payload=None):
    from api.run_journal import append_run_event

    return append_run_event(
        sid, rid, event_name, payload or {},
        session_dir=session_dir, seq=seq, created_at=created_at,
    )


def test_prune_deletes_only_finished_runs_older_than_retention(tmp_path):
    now = time.time()
    old = now - 3 * 86400
    _append("sid-a", "run-old", "token", tmp_path, created_at=old)
    _append("sid-a", "run-old", "done", tmp_path, created_at=old + 1, seq=2)
    _append("sid-a", "run-new", "token", tmp_path, created_at=now)
    _append("sid-a", "run-new", "done", tmp_path, created_at=now + 1, seq=2)

    removed = prune_run_journals(retention_days=1.0, session_dir=tmp_path, now=now)

    assert removed["removed"] == ["sid-a/run-old"]
    assert not (tmp_path / "_run_journal" / "sid-a" / "run-old.jsonl").exists()
    assert (tmp_path / "_run_journal" / "sid-a" / "run-new.jsonl").exists()


def test_prune_keeps_runs_without_terminal_event_regardless_of_age(tmp_path):
    # An unfinished journal may still be mid-recovery; deleting it would destroy
    # the only copy of a turn's context (test_recovered_journal_context contract).
    old = time.time() - 30 * 86400
    _append("sid-b", "run-open", "token", tmp_path, created_at=old)

    removed = prune_run_journals(retention_days=7.0, session_dir=tmp_path)

    assert removed["removed"] == []
    assert (tmp_path / "_run_journal" / "sid-b" / "run-open.jsonl").exists()


def test_prune_uses_terminal_event_time_not_file_mtime(tmp_path):
    # Old file mtime restored to the past does not matter: the terminal event is
    # fresh, so the run survives. (And vice versa: fresh mtime + old terminal
    # event still gets pruned — the journal body is authoritative.)
    old = time.time() - 30 * 86400
    now = time.time()
    _append("sid-c", "run-x", "token", tmp_path, created_at=old)
    _append("sid-c", "run-x", "done", tmp_path, created_at=now, seq=2)
    removed = prune_run_journals(retention_days=7.0, session_dir=tmp_path)
    assert removed["removed"] == []


def test_prune_idempotent_and_survives_malformed_lines(tmp_path):
    old = time.time() - 30 * 86400
    sid_dir = tmp_path / "_run_journal" / "sid-d"
    sid_dir.mkdir(parents=True)
    (sid_dir / "run-junk.jsonl").write_text("not json at all\n", encoding="utf-8")
    _append("sid-d", "run-done", "done", tmp_path, created_at=old)

    first = prune_run_journals(retention_days=1.0, session_dir=tmp_path)
    second = prune_run_journals(retention_days=1.0, session_dir=tmp_path)

    assert first["removed"] == ["sid-d/run-done"]
    assert second["removed"] == []
    # Malformed journal without a parseable terminal event is left alone.
    assert (sid_dir / "run-junk.jsonl").exists()


def test_prune_removes_session_dir_only_when_it_empties_out(tmp_path):
    old = time.time() - 30 * 86400
    _append("sid-e", "run-a", "done", tmp_path, created_at=old)
    res = prune_run_journals(retention_days=1.0, session_dir=tmp_path)
    assert res["removed"] == ["sid-e/run-a"]
    # Empty leftover dirs are cleaned so the corpus stops growing at the dir level.
    assert not (tmp_path / "_run_journal" / "sid-e").exists()


def test_maybe_prune_interval_guard_and_disable(monkeypatch, tmp_path):
    from api import run_journal as rj

    old = time.time() - 30 * 86400
    _append("sid-f", "run-old", "done", tmp_path, created_at=old)

    monkeypatch.setenv(rj._RETENTION_DAYS_ENV, "0")
    res = rj.maybe_prune_run_journals(session_dir=tmp_path)
    assert res["status"] == "disabled"
    assert (tmp_path / "_run_journal" / "sid-f" / "run-old.jsonl").exists()

    monkeypatch.setenv(rj._RETENTION_DAYS_ENV, "7")
    first = rj.maybe_prune_run_journals(session_dir=tmp_path)
    assert first["status"] == "pruned"
    assert first["removed"] == ["sid-f/run-old"]

    # Second pass inside the interval is a no-op even if new old journals appear.
    _append("sid-g", "run-old2", "done", tmp_path, created_at=old)
    again = rj.maybe_prune_run_journals(session_dir=tmp_path)
    assert again["status"] == "within-interval"
    assert (tmp_path / "_run_journal" / "sid-g" / "run-old2.jsonl").exists()
