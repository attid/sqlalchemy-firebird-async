# Contributing to sqlalchemy-firebird-async

First off, thanks for taking the time to contribute!

## Development Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/attid/sqlalchemy-firebird-async.git
   cd sqlalchemy-firebird-async
   ```

2. **Install dependencies (using `uv` or `pip`):**
   ```bash
   uv sync  # or pip install -e .[all,test]
   ```

## Running Tests

We use `pytest` and `testcontainers` (Docker) for testing. Ensure Docker is running.

### 1. Standard Tests (Recommended)
These tests cover the core functionality of the asynchronous drivers (Core, ORM, Types).

```bash
# Run tests for the default driver (fdb_async)
uv run pytest

# Run tests for the new driver (firebird_async)
TEST_DIALECT=firebird_async uv run pytest
```

### 2. SQLAlchemy Compliance Suite (Advanced)
These tests run the official SQLAlchemy test suite against our dialect. This is useful for verifying deep compatibility.

**Note:** This automatically starts a Firebird container and configures `setup.cfg`.

```bash
# Run Compliance Suite using fdb_async
PYTHONPATH=. TEST_DIALECT=firebird_async uv run pytest tests/test_compliance.py -p sqlalchemy.testing.plugin.pytestplugin
```

## Supported Drivers

*   **`fdb_async`** (Default): Uses the threaded legacy `fdb` driver. Stable but lacks some FB4 features.
*   **`firebird_async`**: Uses the modern `firebird-driver`. Supports FB4 features (INT128, TimeZones).

## Project Structure

*   `src/sqlalchemy_firebird_async/`: Source code.
    *   `fdb.py`: `fdb` based dialect.
    *   `firebird_driver.py`: `firebird-driver` based dialect.
    *   `compiler.py`: Custom type compiler patches.
*   `tests/`: Tests.
    *   `test_basic.py`: Basic CRUD and ORM tests.
    *   `test_types.py`: Comprehensive data types tests.
    *   `test_load.py`: Concurrency load test.
    *   `test_compliance.py`: Entry point for SQLAlchemy Compliance Suite.
