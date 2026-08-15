# Universal Context Management System (scaffold)

Initial scaffold for the Universal Context Management System described in the spec.

Structure:

- `src/` - core library and CLI
- `context_store/` - sqlite DB, session JSONL files and cache
- `tests/` - unit tests

Quickstart:

1. Create virtualenv and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Initialize the store:

```bash
python -m context_system.src.cli init --project stock-ml-platform
```
Run CLI (from repo root):

```bash
chmod +x ./context
./context bootstrap --project stock-ml-platform
```

Or run with Python directly:

```bash
python3 -m context_system.src.cli bootstrap --project stock-ml-platform
```
