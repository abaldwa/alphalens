from pathlib import Path
from context_system.src.storage import ContextStorage
from context_system.src.summarization import summarize_session


def test_summarize_session_stores_summary(tmp_path: Path):
    db_path = tmp_path / "context_store.db"
    storage = ContextStorage(db_path=db_path)
    session_id = "sess-1"
    storage.create_session(session_id, project_id="p1")
    # append some messages
    storage.append_session_message(session_id, {"role": "user", "text": "Please implement a function to compute ROI."})
    storage.append_session_message(session_id, {"role": "assistant", "text": "Sure, I'll draft a function using numpy."})
    # summarize
    s = summarize_session(session_id, storage=storage, compression=0.5)
    assert isinstance(s, str)
    assert len(s) > 0
    # verify sessions table has the summary
    cur = storage.conn.cursor()
    cur.execute("SELECT summary FROM sessions WHERE id = ?", (session_id,))
    row = cur.fetchone()
    assert row is not None
    assert row[0] is not None and row[0] != ""
