"""Semantic retrieval utilities using local embeddings."""
from __future__ import annotations

from typing import List, Dict, Any, Optional
import numpy as np
from sentence_transformers import SentenceTransformer
from .storage import ContextStorage


class Retriever:
    def __init__(self, storage: Optional[ContextStorage] = None, model_name: str = "all-MiniLM-L6-v2"):
        self.storage = storage or ContextStorage()
        self.model_name = model_name
        self._model: Optional[SentenceTransformer] = None

    @property
    def model(self) -> SentenceTransformer:
        if self._model is None:
            self._model = SentenceTransformer(self.model_name)
        return self._model

    def embed_texts(self, texts: List[str]) -> np.ndarray:
        embs = self.model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
        return embs

    def index_missing_embeddings(self) -> int:
        """Find project_context rows without embeddings and compute + store them."""
        rows = self.storage.list_project_contexts()
        to_index = [r for r in rows if not r.get("embedding")]
        if not to_index:
            return 0
        texts = [r["content"] for r in to_index]
        embs = self.embed_texts(texts)
        for r, emb in zip(to_index, embs):
            self.storage.update_project_context_embedding(r["id"], emb.tolist())
        return len(to_index)

    def semantic_search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        q_emb = self.embed_texts([query])[0]
        rows = self.storage.list_project_contexts()
        candidates = []
        for r in rows:
            emb_json = r.get("embedding")
            if not emb_json:
                continue
            try:
                emb = np.array(r["embedding"] if isinstance(r["embedding"], list) else r["embedding"])
            except Exception:
                try:
                    emb = np.array(__import__("json").loads(r["embedding"]))
                except Exception:
                    continue
            # cosine similarity (embeddings normalized already, but handle case)
            if np.linalg.norm(emb) == 0 or np.linalg.norm(q_emb) == 0:
                score = 0.0
            else:
                score = float(np.dot(q_emb, emb) / (np.linalg.norm(q_emb) * np.linalg.norm(emb)))
            candidates.append({"row": r, "score": score})
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:top_k]
