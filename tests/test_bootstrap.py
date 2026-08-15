from pathlib import Path
from context_system.src.bootstrap import bootstrap_project
from context_system.src.storage import ContextStorage


def test_bootstrap_creates_context(tmp_path: Path):
    # create a small repo layout
    (tmp_path / "README.md").write_text("# Sample Project\nThis is a sample readme.")
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "ARCHITECTURE.md").write_text("Architecture: microservices")
    (tmp_path / "requirements.txt").write_text("numpy\npandas")
    (tmp_path / "module.py").write_text('''"""Module docstring"""\n\ndef foo():\n    pass\n''')

    db_path = tmp_path / "context_store.db"
    storage = ContextStorage(db_path=db_path)
    res = bootstrap_project(tmp_path, project_name="sample", scan_depth=3, storage=storage, generate_embeddings=False)
    # ensure files scanned > 0 and storage has contexts
    rows = storage.list_project_contexts()
    assert res["files_scanned"] >= 1
    assert len(rows) >= 1
