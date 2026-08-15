from pathlib import Path

from context_system.src.storage import ContextStorage
from context_system.src.retrieval import Retriever


def test_semantic_search_monkeypatched_embedding(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "context_store.db"
    storage = ContextStorage(db_path=db_path)
    # Add some project contexts
    ids = []
    ids.append(storage.save_project_context("architecture", "This service handles user auth"))
    ids.append(storage.save_project_context("architecture", "Batch backtesting pipeline for strategies"))
    ids.append(storage.save_project_context("decisions", "We prefer append-only registry rows"))

    retriever = Retriever(storage=storage)

    # Monkeypatch embed_texts to return deterministic vectors
    def fake_embed_texts(self, texts):
        # Return one-hot-ish vectors based on keywords
        vecs = []
        for t in texts:
            if "auth" in t or "user auth" in t:
                vecs.append([1.0, 0.0, 0.0])
            elif "backtesting" in t:
                vecs.append([0.0, 1.0, 0.0])
            else:
                vecs.append([0.0, 0.0, 1.0])
        import numpy as np

        return np.array(vecs, dtype=float)

    monkeypatch.setattr(Retriever, "embed_texts", fake_embed_texts)

    # Index embeddings for stored rows
    count = retriever.index_missing_embeddings()
    assert count == 3

    results = retriever.semantic_search("authentication and user login", top_k=2)
    assert len(results) >= 1
    # top result should be the auth entry
    top = results[0]
    assert "auth" in top["row"]["content"] or "user auth" in top["row"]["content"]
