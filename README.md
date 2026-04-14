# DataSage

DataSage is a local AI data assistant for PostgreSQL. It uses Ollama for local
LLM inference, retrieves schema context with embeddings, generates SQL,
executes queries safely, and returns plain-language answers with charts,
insights, and conversational follow-ups.

## Features

- Natural language to SQL for PostgreSQL
- Schema-aware retrieval with embeddings
- Conversational memory for follow-up questions
- SQL retry and correction loop for failed queries
- Multi-step routing for complex analytical questions
- Streamlit UI and CLI chat mode
- Built-in benchmark and answer-audit scripts

## How It Works

1. Extract schema from PostgreSQL
2. Convert schema into searchable text
3. Retrieve the most relevant schema context
4. Generate or route SQL
5. Execute SQL against the database
6. Return an answer, insights, and optional charts

## Repository Layout

```text
.
├── app.py                  # Streamlit UI
├── main.py                 # CLI entrypoint
├── main_logic.py           # Core orchestration and routing
├── db.py                   # PostgreSQL connection helpers
├── schema.py               # Schema extraction and schema text
├── sql_generator.py        # LLM-driven SQL generation and repair
├── planner.py              # Plan parsing for complex questions
├── memory.py               # Conversation memory
├── insights.py             # Rule-based and AI insight helpers
├── vector_store.py         # Embedding index and schema search
├── visualize.py            # Result plotting helpers
├── schema.sql              # Sample dataset
├── evals/                  # Benchmark and answer-audit tooling
├── scripts/                # Local PostgreSQL helper scripts
└── tests/                  # Lightweight unit tests
```

## Prerequisites

- Python 3.10+
- PostgreSQL
- Ollama running locally
- A local Ollama model such as `mistral`

## Getting Started

1. Create and activate a virtual environment.
2. Install dependencies.
3. Start PostgreSQL and seed the sample database.
4. Start Ollama.
5. Run the CLI or Streamlit app.

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

./scripts/start_postgres.sh
./scripts/init_sample_db.sh

python main.py
streamlit run app.py
```

## Configuration

Environment variables are documented in `.env.example`.

```bash
PGDATABASE=sales_db
PGUSER=postgres
PGPASSWORD=postgres
PGHOST=localhost
PGPORT=5432
OLLAMA_URL=http://127.0.0.1:11434/api/generate
OLLAMA_MODEL=mistral
```

## Usage

Run the CLI chat mode:

```bash
python main.py
```

Run a single question:

```bash
python main.py "Show top products by revenue"
```

Run schema-only retrieval:

```bash
python main.py schema "Which table contains revenue?"
```

Run the web app:

```bash
streamlit run app.py
```

Example follow-up flow:

```text
Ask: Show top products by revenue
Ask: Now only for UK
Ask: Only in February
```

## Development

Common commands:

```bash
make install
make test
make eval-fast
make eval-audit
```

The benchmark suite currently covers 100+ natural-language questions across
single-shot, follow-up, and multi-step flows.

## Contributing

See `CONTRIBUTING.md` for development setup, pull request guidance, and review
expectations.

## Security

See `SECURITY.md` for responsible disclosure guidance.
