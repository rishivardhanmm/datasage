# Contributing to DataSage

Thanks for your interest in contributing.

## Development Setup

1. Create a virtual environment and install dependencies.
2. Start PostgreSQL locally.
3. Seed the sample database.
4. Start Ollama and make sure the configured model is available.

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
./scripts/start_postgres.sh
./scripts/init_sample_db.sh
```

## Recommended Workflow

1. Create a branch for your change.
2. Keep changes scoped to one concern.
3. Add or update tests when behavior changes.
4. Run the local checks before opening a pull request.

```bash
make test
make eval-fast
```

## Pull Request Guidelines

- Keep pull requests focused and easy to review.
- Explain user-facing behavior changes clearly.
- Include benchmark or test evidence when changing routing or query logic.
- Avoid mixing refactors with feature work unless they are tightly related.

## Coding Guidelines

- Prefer small, readable functions over large prompt-only behavior.
- Keep deterministic routing for repeated query patterns when possible.
- Make changes that preserve local-first execution.
- Do not commit secrets, local databases, logs, or generated cache files.
