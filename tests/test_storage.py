from pathlib import Path

from context_system.src.storage import ContextStorage


def test_save_and_load_project_context(tmp_path: Path):
    db_path = tmp_path / "context_store.db"
    storage = ContextStorage(db_path=db_path)
    cid = storage.save_project_context("architecture", "A sample arch note", tags={"team": "alpha"})
    assert isinstance(cid, int)
    rec = storage.get_project_context(cid)
    assert rec is not None
    assert rec["category"] == "architecture"
