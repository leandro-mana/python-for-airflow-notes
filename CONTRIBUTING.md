# Contributing to Python for Airflow Notes

This repository follows a professional, chapter-based workflow consistent with the
[python-for-data-science-notes](https://github.com/leandro-mana/python-for-data-science-notes),
[python-for-time-series-notes](https://github.com/leandro-mana/python-for-time-series-notes),
and other repositories in the Python Learning Series.

## Workflow

### 1. Branch per Chapter

Create a feature branch for each chapter:

```bash
git checkout -b feature/chapter-01
git checkout -b feature/chapter-02
```

### 2. Chapter Structure

Each chapter directory should contain:

```bash
src/chapter_XX/
├── __init__.py            # Module docstring describing the chapter
├── README.md              # Chapter overview, DAG table, key concepts, references
└── dags/                  # Runnable DAG files (primary content)
    ├── 01_topic_name.py
    └── 02_another_topic.py
```

### 3. DAG Files

DAGs are the primary learning material. When creating them:

1. Keep each DAG focused on one concept
2. Include docstrings explaining what the DAG demonstrates
3. Number DAGs sequentially: `01_topic_name.py`, `02_another_topic.py`
4. Use `from __future__ import annotations` at the top of every DAG
5. Set meaningful `tags` for UI filtering (e.g., `["chapter_01", "basics"]`)
6. Use modern APIs only (Airflow 2.10+, TaskFlow where appropriate)
7. Include `doc_md` with a brief description visible in the Airflow UI

### 4. Chapter READMEs

Each chapter README should include:

- **Overview**: Brief chapter summary
- **DAGs table**: All DAGs with topics covered
- **Key Concepts**: Main ideas covered in the chapter
- **References**: Credit official docs, Astronomer guides where applicable

### 5. Tests

Write tests in `tests/chapter_XX/`:

```bash
tests/
├── __init__.py
├── chapter_01/
│   └── test_hello_airflow.py
└── ...
```

- DAG validation tests (import errors, tags, structure)
- Unit tests for Python callables used in `@task` functions
- Use `pytest` fixtures for Airflow context mocking

### 6. Code Quality

Before committing, run all checks:

```bash
# Single command to check everything
make check

# Or individual checks
make lint              # Ruff linter
make format            # Code formatting
make type-check        # mypy type checking
make test              # pytest
```

### 7. Commit Messages

Write clear commit messages:

- `feat(ch01): Add hello airflow DAG with default args`
- `feat(ch05): Add TaskFlow XCom examples`
- `docs(ch04): Update operators chapter README`
- `fix(ch08): Fix ETL pipeline idempotent upsert`
- `test(ch09): Add DAG validation unit tests`

### 8. Pull Request

When ready, open a PR with:

- Clear title: `Add Chapter 05 - TaskFlow API`
- Description of DAGs included and concepts covered

## Code Style

Enforced by `make format` (ruff):

- Line length: 100 characters
- 4 spaces for indentation
- f-strings for formatting
- snake_case for functions/variables
- PascalCase for classes
- UPPER_CASE for constants
