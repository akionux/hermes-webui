"""Live-snapshot reads must be bounded + incremental (Task 2).

``_run_journal_live_snapshot`` (api/routes.py) rebuilds the in-flight transcript
via ``read_run_events`` — previously a full ``_read_jsonl`` on EVERY request,
so a streaming session's detail load re-parsed the whole multi-hundred-MB
journal per page open. The filtered reader:

* skips TELEMETRY rows (metering/context_status/anchor_activity) with a byte
  marker scan — they carry no transcript content and are never json-parsed;
* folds appended bytes incrementally: the head region parsed once is never
  re-read on the next request mid-turn.
"""
import time

from api import run_journal as rj
from api.run_journal import append_run_event, read_filtered_run_events


def _fill(sid, rid, tmp_path, *, n=2000):
    now = 1_750_000_000.0
    append_run_event(sid, rid, "submitted", {"content": "go"}, session_dir=tmp_path, created_at=now)
    for i in range(n):
        if i % 2 == 0:
            append_run_event(sid, rid, "metering", {"usage": {"u": "x" * 120}}, session_dir=tmp_path,
                             created_at=now + i * 1e-4, seq=i + 2)
        else:
            append_run_event(sid, rid, "token", {"text": "y" * 100}, session_dir=tmp_path,
                             created_at=now + i * 1e-4, seq=i + 2)
    return now


def test_filtered_reader_skips_telemetry_lines_without_parsing_them(tmp_path):
    _fill("sid-f", "stream-f", tmp_path)
    res = read_filtered_run_events("sid-f", "stream-f", session_dir=tmp_path)
    names = {e["event"] for e in res["events"]}
    assert "metering" not in names
    assert "token" in names and "submitted" in names
    # Kept rows are fully parsed events.
    tok = [e for e in res["events"] if e["event"] == "token"][0]
    assert tok["payload"]["text"].startswith("y")


def test_filtered_reader_folds_appends_without_reparsing_head(tmp_path, monkeypatch):
    _fill("sid-f", "stream-g", tmp_path)

    calls = {"n": 0}
    real = rj._read_jsonl

    def counting(path):
        calls["n"] += 1
        return real(path)

    monkeypatch.setattr(rj, "_read_jsonl", counting)
    first = read_filtered_run_events("sid-f", "stream-g", session_dir=tmp_path)
    n_first = len(first["events"])

    append_run_event("sid-f", "stream-g", "interim_assistant", {"text": "seg"},
                     session_dir=tmp_path, seq=2002)
    second = read_filtered_run_events("sid-f", "stream-g", session_dir=tmp_path)

    assert calls["n"] == 0, "filtered reader must not use the full-parse path"
    assert len(second["events"]) == n_first + 1
    assert second["events"][-1]["event"] == "interim_assistant"
    # Head events survive intact and in order.
    assert second["events"][0]["event"] == "submitted"


def test_filtered_reader_preserves_order_and_all_content_types(tmp_path):
    now = time.time()
    append_run_event("s2", "r2", "tool", {"name": "exec"}, session_dir=tmp_path, created_at=now)
    append_run_event("s2", "r2", "context_status", {"x": 1}, session_dir=tmp_path, created_at=now, seq=2)
    append_run_event("s2", "r2", "tool_complete", {"name": "exec"}, session_dir=tmp_path, created_at=now, seq=3)
    res = read_filtered_run_events("s2", "r2", session_dir=tmp_path)
    assert [e["event"] for e in res["events"]] == ["tool", "tool_complete"]
