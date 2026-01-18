# sqlalchemy-firebird-async

![Python Version](https://img.shields.io/pypi/pyversions/sqlalchemy-firebird-async)
![License](https://img.shields.io/pypi/l/sqlalchemy-firebird-async)
![Status](https://img.shields.io/pypi/status/sqlalchemy-firebird-async)

**Asynchronous Firebird dialect for SQLAlchemy.**

This library provides proper `asyncio` support for Firebird databases in SQLAlchemy 2.0+, allowing you to write fully asynchronous code using modern Python patterns.

It supports three underlying drivers:
1. **`fdb`** (Legacy) - Runs the official C-based driver in a thread pool. Fast and stable.
2. **`firebird-driver`** (Modern) - Official driver for Firebird 3+. Also threaded (run_in_executor).
3. **`firebirdsql`** - Pure Python asyncio driver. Currently experimental due to upstream issues.

## 📦 Installation

Install using pip:

```bash
# Install with the FDB driver (Legacy, Stable)
pip install "sqlalchemy-firebird-async[fdb]"

# Install with the Firebird Driver (Modern, FB 3.0+)
pip install "sqlalchemy-firebird-async[firebird-driver]"

# Install with pure python driver (Experimental)
pip install "sqlalchemy-firebird-async[firebirdsql]"
# Note: For correct async behavior with firebirdsql, you might need a patched version:
# pip install git+https://github.com/attid/pyfirebirdsql.git
```

## 🚀 Quick Start

### 1. Using FDB Driver (Legacy)

This dialect runs the legacy `fdb` driver in a thread pool.

**URL Scheme:** `firebird+fdb_async://`

```python
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text

async def main():
    # Format: firebird+fdb_async://user:password@host:port/path/to/db
    dsn = "firebird+fdb_async://sysdba:masterkey@localhost:3050//firebird/data/employee.fdb"
    
    engine = create_async_engine(dsn, echo=True)
    # ... usage is identical for all drivers
```

### 2. Using Firebird Driver (Modern)

This dialect uses the modern `firebird-driver` package (the official driver for Firebird 3.0+), running in a thread pool. It requires Firebird 3.0 or higher.

**URL Scheme:** `firebird+firebird_async://`

```python
engine = create_async_engine(
    "firebird+firebird_async://sysdba:masterkey@localhost:3050//firebird/data/employee.fdb?charset=UTF8"
)
```

### 3. Using Native Async Driver (firebirdsql)

**Warning:** The upstream `firebirdsql` driver currently has issues with `asyncio` compatibility (bugs causing crashes or incorrect behavior).
A patched fork is available at [attid/pyfirebirdsql](https://github.com/attid/pyfirebirdsql.git), which fixes the async logic but currently exhibits significantly lower performance (approx. 4x slower than fdb).

**URL Scheme:** `firebird+firebirdsql_async://`

```python
engine = create_async_engine(
    "firebird+firebirdsql_async://sysdba:masterkey@localhost:3050//firebird/data/employee.fdb"
)
```

## 📊 Performance Comparison

We compared both drivers executing 5000 queries in 8 concurrent tasks (4 raw SQL + 4 ORM).

| Metric | **fdb (Threaded)** 🏆 | **firebirdsql (Patched)** | Difference |
| :--- | :--- | :--- | :--- |
| **Total Time** | **4.53s** | 116.20s | ~25x slower |
| **Avg Query Time (ORM)** | **2.54s** | 114.43s | ~45x slower |
| **Avg Query Time (Raw)** | **4.44s** | 116.14s | ~26x slower |
| **Parallel Ratio** | 6.16x | 7.94x | - |

*Benchmark details: 8 concurrent workers, 5000 rows each, total 40k rows.*

As seen above, `fdb` in a thread pool is significantly faster for high-load scenarios.

## 🔌 Connection String Guide

| Driver | Protocol | URL Scheme |
| :--- | :--- | :--- |
| **fdb** (Legacy) | TCP/IP | `firebird+fdb_async://user:pass@host:port/db_path` |
| **firebird-driver** (Modern) | TCP/IP | `firebird+firebird_async://user:pass@host:port/db_path` |
| **firebirdsql** | TCP/IP | `firebird+firebirdsql_async://user:pass@host:port/db_path` |

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
