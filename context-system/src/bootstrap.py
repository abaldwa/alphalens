"""Project bootstrap utilities to auto-extract initial context from a repository.

Scans README, docs, python files, requirements, package files and git history
and writes context chunks into the storage layer.
"""
from __future__ import annotations

from pathlib import Path
import ast
import tomllib
import json
import subprocess
from typing import List, Dict, Any, Optional

from .storage import ContextStorage


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _parse_python(path: Path) -> Optional[str]:
    try:
        src = _read_text(path)
        tree = ast.parse(src)
        doc = ast.get_docstring(tree) or ""
        classes = [n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        funcs = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        imports = []
        for n in ast.walk(tree):
            if isinstance(n, ast.Import):
                for alias in n.names:
                    imports.append(alias.name)
            elif isinstance(n, ast.ImportFrom):
                imports.append((n.module or "") )
        parts = [f"File: {path.name}"]
        if doc:
            parts.append(f"Docstring:\n{doc}")
        if classes:
            parts.append(f"Classes: {', '.join(classes)}")
        if funcs:
            parts.append(f"Functions: {', '.join(funcs[:20])}")
        if imports:
            parts.append(f"Imports: {', '.join(sorted(set(imports))[:20])}")
        return "\n\n".join(parts)
    except Exception:
        return None


def _parse_markdown(path: Path) -> str:
    return _read_text(path)


def _parse_requirements(path: Path) -> str:
    txt = _read_text(path)
    deps = [ln.strip() for ln in txt.splitlines() if ln.strip() and not ln.startswith("#")]
    return "\n".join(deps)


def _parse_package_json(path: Path) -> str:
    try:
        data = json.loads(_read_text(path))
        deps = data.get("dependencies", {})
        dev = data.get("devDependencies", {})
        return json.dumps({"dependencies": deps, "devDependencies": dev}, indent=2)
    except Exception:
        return _read_text(path)


def _parse_pyproject(path: Path) -> str:
    try:
        data = tomllib.loads(_read_text(path))
        return json.dumps(data, indent=2)
    except Exception:
        return _read_text(path)


def _git_recent_messages(root: Path, n: int = 50) -> List[str]:
    try:
        out = subprocess.check_output(["git", "-C", str(root), "log", "-n", str(n), "--pretty=%s"], text=True)
        return [line.strip() for line in out.splitlines() if line.strip()]
    except Exception:
        return []


def _detect_category(path: Path) -> str:
    name = path.name.lower()
    if "readme" in name:
        return "project_overview"
    if "arch" in name or "architecture" in name:
        return "architecture"
    if path.suffix in {"md", "markdown"} or ("docs" in path.parts):
        return "documentation"
    if path.suffix in {"py"}:
        return "codebase"
    if path.name in {"requirements.txt", "package.json", "pyproject.toml"}:
        return "dependencies"
    return "misc"


def bootstrap_project(root: Path, project_name: str, scan_depth: int = 3, storage: Optional[ContextStorage] = None, generate_embeddings: bool = True) -> Dict[str, Any]:
    root = Path(root)
    storage = storage or ContextStorage()
    collected = []

    # Walk files with limited depth
    for p in root.rglob("*"):
        try:
            if p.is_dir():
                continue
            # skip hidden and git
            if any(part.startswith(".") for part in p.relative_to(root).parts):
                continue
            rel = p.relative_to(root)
            if len(rel.parts) > scan_depth:
                continue
            ext = p.suffix.lower()
            if ext in {".md", ".markdown"} or p.name.lower().startswith("readme"):
                content = _parse_markdown(p)
                cat = _detect_category(p)
            elif ext == ".py":
                parsed = _parse_python(p)
                if not parsed:
                    continue
                content = parsed
                cat = "codebase"
            elif p.name == "requirements.txt":
                content = _parse_requirements(p)
                cat = "dependencies"
            elif p.name == "package.json":
                content = _parse_package_json(p)
                cat = "dependencies"
            elif p.name == "pyproject.toml":
                content = _parse_pyproject(p)
                cat = "dependencies"
            else:
                # skip large binaries and others
                continue
            if not content or not content.strip():
                continue
            tags = {"source": str(p.relative_to(root))}
            storage.save_project_context(cat, content, embedding=None, tags=tags)
            collected.append(p)
        except Exception:
            continue

    # Git commit messages
    commits = _git_recent_messages(root, n=50)
    for c in commits:
        storage.save_project_context("version_control", c, embedding=None, tags={"git": True})

    # Optionally generate embeddings for newly added rows
    total_indexed = 0
    if generate_embeddings:
        try:
            from .retrieval import Retriever

            retriever = Retriever(storage=storage)
            total_indexed = retriever.index_missing_embeddings()
        except Exception:
            total_indexed = 0

    return {"files_scanned": len(collected), "commits_indexed": len(commits), "embeddings_indexed": total_indexed}


if __name__ == "__main__":
    import sys
    res = bootstrap_project(Path.cwd(), project_name=sys.argv[1] if len(sys.argv) > 1 else "project", scan_depth=3)
    print(res)
