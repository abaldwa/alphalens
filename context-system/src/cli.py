"""CLI entry points for the context-system."""
import json
from pathlib import Path
from typing import Optional

import typer

from .storage import ContextStorage
from .config import DB_PATH
from .retrieval import Retriever
from .bootstrap import bootstrap_project
from .summarization import summarize_session

app = typer.Typer(help="Context management CLI")


@app.command()
def init(project: str = typer.Option(..., help="Project name")):
    """Initialize a new project context storage (creates directories and DB)."""
    ContextStorage()
    typer.echo(f"Initialized context store at {DB_PATH}")


@app.command()
def session_start(session_id: str = typer.Option(...), project_id: str = typer.Option(None)):
    storage = ContextStorage()
    storage.create_session(session_id, project_id=project_id)
    typer.echo(f"Started session {session_id}")


@app.command()
def session_end(session_id: str = typer.Option(...)):
    storage = ContextStorage()
    storage.summarize_session_placeholder(session_id)
    typer.echo(f"Ended session {session_id}. Summary saved.")


@app.command()
def search(query: str = typer.Argument(...), top_k: int = typer.Option(5, help="Top K results")):
    """Semantic search project context for a query."""
    storage = ContextStorage()
    retriever = Retriever(storage=storage)
    results = retriever.semantic_search(query, top_k=top_k)
    if not results:
        typer.echo("No matching context found.")
        raise typer.Exit()
    for r in results:
        row = r["row"]
        score = r["score"]
        typer.echo(f"[{row['id']}] score={score:.4f} category={row.get('category')}")
        typer.echo(row.get("content")[:400])
        typer.echo("---")


@app.command()
def bootstrap(project: str = typer.Option(..., help="Project name"), scan_depth: int = typer.Option(3), embed: bool = typer.Option(True)):
    """Bootstrap project context by scanning repository files and git history."""
    storage = ContextStorage()
    root = Path.cwd()
    res = bootstrap_project(root, project, scan_depth=scan_depth, storage=storage, generate_embeddings=embed)
    typer.echo(f"Bootstrapped {res['files_scanned']} files, indexed {res['commits_indexed']} commits, embeddings: {res['embeddings_indexed']}")


@app.command()
def summarize(session_id: str = typer.Option(None, help="Session ID to summarize"), compression: float = typer.Option(0.1, help="Compression ratio 0-1")):
    """Summarize a session transcript (or all sessions without summary)."""
    storage = ContextStorage()
    if session_id:
        s = summarize_session(session_id, storage=storage, compression=compression)
        typer.echo(f"Session {session_id} summarized (len={len(s)} chars)")
        raise typer.Exit()
    # summarize all sessions missing a summary
    cur = storage.conn.cursor()
    cur.execute("SELECT id FROM sessions WHERE summary IS NULL OR summary = ''")
    rows = cur.fetchall()
    for r in rows:
        sid = r[0]
        s = summarize_session(sid, storage=storage, compression=compression)
        typer.echo(f"Session {sid} summarized (len={len(s)} chars)")


@app.command()
def save(category: str = typer.Option(..., help="Context category"), content: str = typer.Option(..., help="Content to save"), tags: Optional[str] = typer.Option(None, help="JSON tags")):
    """Manually save a project context chunk."""
    storage = ContextStorage()
    parsed_tags = None
    if tags:
        try:
            parsed_tags = json.loads(tags)
        except Exception:
            parsed_tags = {"raw": tags}
    cid = storage.save_project_context(category, content, embedding=None, tags=parsed_tags)
    typer.echo(f"Saved context id={cid}")


@app.command()
def load(session_id: str = typer.Option(..., help="Session id to load")):
    """Load and print a session transcript."""
    storage = ContextStorage()
    msgs = storage.load_session_transcript(session_id)
    for m in msgs:
        print(json.dumps(m, ensure_ascii=False))


@app.command()
def export(format: str = typer.Option("json", help="Export format: json|md"), output: str = typer.Option("context_export.json", help="Output file")):
    """Export project context to a file."""
    storage = ContextStorage()
    rows = storage.list_project_contexts()
    outp = Path(output)
    if format == "json":
        outp.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
        typer.echo(f"Exported {len(rows)} chunks to {output}")
        raise typer.Exit()
    elif format == "md":
        with outp.open("w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(f"## [{r['id']}] {r.get('category')}\n\n")
                fh.write(r.get("content", "") + "\n\n---\n\n")
        typer.echo(f"Exported {len(rows)} chunks to {output}")
        raise typer.Exit()
    else:
        typer.echo("Unknown format")
        raise typer.Exit(code=2)


@app.command()
def retrieve(query: str = typer.Argument(..., help="Query to retrieve context for"), top_k: int = typer.Option(5, help="Top K results"), output: str = typer.Option("injected_context.txt", help="Output file")):
    """Retrieve top-K semantic context chunks and write to a file for injection."""
    storage = ContextStorage()
    retriever = Retriever(storage=storage)
    results = retriever.semantic_search(query, top_k=top_k)
    out_lines = []
    for r in results:
        row = r["row"]
        score = r["score"]
        out_lines.append(f"[id={row['id']}] score={score:.4f} category={row.get('category')}\n")
        out_lines.append(row.get("content", "") + "\n\n---\n\n")
    Path(output).write_text("".join(out_lines), encoding="utf-8")
    typer.echo(f"Wrote {len(results)} chunks to {output}")


def main():
    app()


if __name__ == "__main__":
    main()

