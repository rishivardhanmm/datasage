PYTHON ?= python3
PIP ?= pip3

ifneq ($(wildcard venv/bin/python),)
PYTHON := venv/bin/python
endif

ifneq ($(wildcard venv/bin/pip),)
PIP := venv/bin/pip
endif

.PHONY: install run-cli run-web test check eval-fast eval-audit db-start db-stop db-seed

install:
	$(PIP) install -r requirements.txt

run-cli:
	$(PYTHON) main.py

run-web:
	streamlit run app.py

test:
	$(PYTHON) -m unittest discover -s tests -v

check:
	$(PYTHON) -m py_compile app.py db.py insights.py llm.py main.py main_logic.py memory.py planner.py schema.py sql_generator.py vector_store.py visualize.py evals/benchmark_cases.py evals/run_benchmark.py

eval-fast:
	$(PYTHON) evals/run_benchmark.py --mode fast

eval-audit:
	$(PYTHON) evals/run_benchmark.py --mode audit

db-start:
	./scripts/start_postgres.sh

db-stop:
	./scripts/stop_postgres.sh

db-seed:
	./scripts/init_sample_db.sh
