"""Task 3: durable persistence of high-frequency events is coalesced.

A single measured turn journaled 251,745 events of which 99.1% were stream
telemetry (metering 49.8% at ~9/s, reasoning deltas 49.3%). The browser SSE
delivery is unchanged — what changes is what reaches disk:

* ``metering`` snapshots are latest-wins: the writer keeps only the most recent
  payload and writes ONE line when the next content/terminal event forces a
  flush (the replayed snapshot only ever needs the latest usage view);
* ``reasoning`` deltas are concatenated into one line per size/time window —
  the snapshot concatenates their text anyway, so reconstruction is identical;
* seq stays monotonic and gapless: skipped/buffered events never consume a
  journal seq until the coalesced row is written.
"""
import json

from api.run_journal import RunJournalWriter, read_run_events


def _lines(sid, rid, tmp_path):
    return read_run_events(sid, rid, session_dir=tmp_path)["events"]


def test_metering_lines_are_latest_wins_one_row_per_phase(tmp_path):
    w = RunJournalWriter("s-m", "r-m", session_dir=tmp_path)
    for i in range(50):
        w.append_sse_event("metering", {"usage": {"last_prompt_tokens": i}})
    w.append_sse_event("token", {"text": "hello"})

    events = _lines("s-m", "r-m", tmp_path)
    metering = [e for e in events if e["event"] == "metering"]
    # The single written row carries the LATEST snapshot (latest-wins).
    assert len(metering) == 1
    assert metering[0]["payload"]["usage"]["last_prompt_tokens"] == 49
    tokens = [e for e in events if e["event"] == "token"]
    assert tokens[0]["payload"]["text"] == "hello"


def test_metering_flushes_before_terminal_so_recovery_sees_usage(tmp_path):
    w = RunJournalWriter("s-m2", "r-m2", session_dir=tmp_path)
    w.append_sse_event("token", {"text": "a"})
    for i in range(5):
        w.append_sse_event("metering", {"usage": {"i": i}})
    w.append_sse_event("done", {})

    events = _lines("s-m2", "r-m2", tmp_path)
    names = [e["event"] for e in events]
    assert names == ["token", "metering", "done"]
    assert events[1]["payload"]["usage"]["i"] == 4


def test_reasoning_deltas_concatenate_into_fewer_rows_with_same_text(tmp_path):
    w = RunJournalWriter("s-r", "r-r", session_dir=tmp_path)
    chunk = "x" * 200
    for _ in range(10):
        w.append_sse_event("reasoning", {"text": chunk})
    w.append_sse_event("done", {})

    events = _lines("s-r", "r-r", tmp_path)
    reasoning_rows = [e for e in events if e["event"] == "reasoning"]
    assert len(reasoning_rows) < 10, "deltas must coalesce into fewer rows"
    joined = "".join(e["payload"]["text"] for e in reasoning_rows)
    assert joined == chunk * 10


def test_seq_is_gapless_when_events_are_coalesced(tmp_path):
    w = RunJournalWriter("s-s", "r-s", session_dir=tmp_path)
    w.append_sse_event("submitted", {"content": "go"})
    for i in range(30):
        w.append_sse_event("metering", {"usage": {"u": i}})
        w.append_sse_event("reasoning", {"text": "y"})
    w.append_sse_event("done", {})

    events = _lines("s-s", "r-s", tmp_path)
    seqs = [e["seq"] for e in events]
    assert seqs == list(range(1, len(seqs) + 1)), "journal rows must stay gapless"


def test_buffered_reasoning_is_flushed_by_terminal_even_without_size(tmp_path):
    w = RunJournalWriter("s-t", "r-t", session_dir=tmp_path)
    w.append_sse_event("reasoning", {"text": "tail text"})
    w.append_sse_event("stream_end", {})

    events = _lines("s-t", "r-t", tmp_path)
    assert [e["event"] for e in events] == ["reasoning", "stream_end"]
    assert events[0]["payload"]["text"] == "tail text"
