#!/usr/bin/env python3
import argparse
import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from sqlalchemy import func, literal, select, text
from sqlalchemy.dialects import registry
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.sql_models import Decisions, Signatures, Signers, Transactions

BASE_SQL_QUERY = """
SELECT
    t.UUID AS TRANS_UUID,
    t.HASH AS TRANS_HASH,
    -- Вытаскиваем тяжелые поля, чтобы забить канал передачи данных
    t.BODY AS TRANS_BODY,
    s.SIGNATURE_XDR,
    --sig.USERNAME AS SIGNER_NAME,
    d.DESCRIPTION AS DECISION_DESC
FROM T_TRANSACTIONS t
-- 1. LEFT JOIN по неравенству дат.
-- Это создает огромное количество комбинаций строк (почти Cross Join),
-- так как индексов на ADD_DT нет.
LEFT JOIN T_SIGNATURES s
    ON s.ADD_DT >= t.ADD_DT

-- 2. JOIN по строковым полям с "грязным хаком" (конкатенация),
-- чтобы гарантированно не использовался прямой поиск.
--LEFT JOIN T_SIGNERS sig
--    ON (t.SOURCE_ACCOUNT || '') = sig.PUBLIC_KEY

-- 3. JOIN с таблицей решений через поиск подстроки.
-- Это очень тяжелая операция для CPU.
LEFT JOIN T_DECISIONS d
    ON d.DESCRIPTION LIKE ('%' || t.UUID || '%')

-- Фильтр, который заставит прочитать BLOB, но почти ничего не отсеет
-- (предполагаем, что BODY не пустое)
--WHERE CHAR_LENGTH(t.BODY) > 0

-- 4. Сортировка по вычисляемому полю.
-- Это не даст серверу отдавать строки "по мере готовности",
-- ему придется сначала обработать ВСЕ соединение.
ORDER BY
    CHAR_LENGTH(d.DESCRIPTION) DESC,
    s.ADD_DT ASC
"""

DEFAULT_DSN = "firebird+fdb_async://SYSDBA:sysdba@127.0.0.1///db/eurmtl.fdb"


def build_async_dsn(dsn: str) -> str:
    if "firebird+fdb_async://" in dsn:
        return dsn
    if "firebird+firebirdsql_async://" in dsn:
        return dsn
    if "firebird+firebird_async://" in dsn:
        return dsn
    if "firebird+async_fdb://" in dsn:
        return dsn.replace("firebird+async_fdb://", "firebird+fdb_async://", 1)
    if "firebird+async_pyfb://" in dsn:
        return dsn.replace("firebird+async_pyfb://", "firebird+firebirdsql_async://", 1)
    if "firebird+fdb://" in dsn:
        return dsn.replace("firebird+fdb://", "firebird+fdb_async://", 1)
    if "firebird+pyfb://" in dsn:
        return dsn.replace("firebird+pyfb://", "firebird+firebirdsql_async://", 1)
    if "firebird+firebirdsql://" in dsn:
        return dsn.replace("firebird+firebirdsql://", "firebird+firebirdsql_async://", 1)
    if dsn.startswith("firebird://"):
        return dsn.replace("firebird://", "firebird+fdb_async://", 1)
    return dsn


def force_async_scheme(dsn: str, scheme: str) -> str:
    if "://" not in dsn:
        return f"firebird+{scheme}://{dsn}"
    _, rest = dsn.split("://", 1)
    return f"firebird+{scheme}://{rest}"


_DIALECTS_REGISTERED = False


def build_engine(dsn: str):
    global _DIALECTS_REGISTERED
    if not _DIALECTS_REGISTERED:
        registry.register("firebird.fdb_async", "sqlalchemy_firebird_async.fdb", "AsyncFDBDialect")
        registry.register("firebird.firebirdsql_async", "sqlalchemy_firebird_async.firebirdsql", "AsyncFirebirdSQLDialect")
        registry.register("firebird.firebird_async", "sqlalchemy_firebird_async.firebird_driver", "AsyncFirebirdDialect")
        _DIALECTS_REGISTERED = True
    async_dsn = build_async_dsn(dsn)
    return create_async_engine(
        async_dsn,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=50,
        pool_timeout=10,
    )

def build_raw_query(rows_limit: int | None) -> str:
    query = BASE_SQL_QUERY.strip()
    if rows_limit and rows_limit > 0:
        query = f"{query}\nROWS 1 TO {rows_limit}"
    return f"{query};"


async def run_worker(engine, worker_id: int, repeats: int, raw_query: str):
    results = []
    async with engine.connect() as connection:
        worker_start = time.perf_counter()
        for i in range(repeats):
            start = time.perf_counter()
            rows = await connection.run_sync(
                lambda sync_conn: sync_conn.execute(text(raw_query)).fetchall()
            )
            duration = time.perf_counter() - start
            row_count = len(rows)
            results.append((duration, row_count))
            print(
                f"[worker {worker_id}] iteration {i + 1}/{repeats} "
                f"rows={row_count} time={duration:.2f}s"
            )
        worker_total = time.perf_counter() - worker_start
        print(f"[worker {worker_id}] done in {worker_total:.2f}s")
    return results

def build_orm_query(rows_limit: int | None):
    # Заглушка, так как у нас нет реальных моделей
    return text(build_raw_query(rows_limit))


async def run_worker_orm(session_maker, worker_id: int, repeats: int, orm_query):
    results = []
    async with session_maker() as session:
        worker_start = time.perf_counter()
        for i in range(repeats):
            start = time.perf_counter()
            rows = await session.run_sync(lambda sync_session: sync_session.execute(orm_query).fetchall())
            duration = time.perf_counter() - start
            row_count = len(rows)
            results.append((duration, row_count))
            print(
                f"[orm {worker_id}] iteration {i + 1}/{repeats} "
                f"rows={row_count} time={duration:.2f}s"
            )
        worker_total = time.perf_counter() - worker_start
        print(f"[orm {worker_id}] done in {worker_total:.2f}s")
    return results

def parse_args():
    parser = argparse.ArgumentParser(
        description="Мини-тест для нагрузки БД через параллельные SELECT.",
    )
    parser.add_argument("--threads", type=int, default=4, help="Количество потоков.")
    parser.add_argument("--orm-threads", type=int, default=0, help="Количество потоков для ORM.")
    parser.add_argument("--repeat", type=int, default=1, help="Повторов запроса в потоке.")
    parser.add_argument("--rows", type=int, default=100, help="Ограничение строк (ROWS 1 TO N).")
    parser.add_argument("--dsn", type=str, default=os.environ.get("DB_DSN"), help="DSN для БД.")
    default_env = Path(__file__).resolve().parent.parent / ".env"
    parser.add_argument(
        "--env-file",
        type=str,
        default=str(default_env),
        help="Путь до .env файла.",
    )
    return parser.parse_args()

def load_dsn_from_env_file(env_path: str) -> str | None:
    path = Path(env_path)
    if not path.exists():
        return None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("DB_DSN="):
            return line.split("=", 1)[1].strip()
    return None


async def run_test(args):
    # Облегченная версия, запускаем по очереди для каждого драйвера, который хотим проверить
    # Тут можно хардкодить то что хотим проверить
    
    # Эмуляция DSN из conftest.py если не передан
    if not args.dsn:
        # Пытаемся угадать параметры docker контейнера, если он запущен
        # Но для теста нам проще использовать переменную окружения TEST_DB_URL, если есть
        pass

    target_dialects = [
        ("fdb_async", "fdb"),
        ("firebird_async", "firebird-driver") 
    ]
    
    # Для целей отладки мы можем использовать localhost и стандартный порт
    base_dsn = args.dsn or "firebird://testuser:testpass@localhost:3050//var/lib/firebird/data/test.fdb?charset=UTF8"

    executor = ThreadPoolExecutor(max_workers=args.threads + args.orm_threads)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(executor)
    
    try:
        for scheme, label in target_dialects:
            print(f"\n{'='*20} Testing {label} ({scheme}) {'='*20}")
            try:
                engine = build_engine(force_async_scheme(base_dsn, scheme))
                
                # Простая проверка соединения
                async with engine.connect() as conn:
                     await conn.execute(text("SELECT 1 FROM rdb$database"))
                
                session_maker = async_sessionmaker(engine, expire_on_commit=False)
                # Упрощенный запрос для теста
                raw_query = "SELECT 1 FROM rdb$database" 
                orm_query = text("SELECT 1 FROM rdb$database")

                start_all = time.perf_counter()
                
                raw_tasks = [
                    run_worker(engine, worker_id + 1, args.repeat, raw_query)
                    for worker_id in range(args.threads)
                ]
                # ORM пока пропускаем или используем заглушку
                orm_tasks = []

                results = await asyncio.gather(*(raw_tasks + orm_tasks))
            except Exception as e:
                print(f"FAILED {scheme}: {e}")
            finally:
                await engine.dispose()
    finally:
        executor.shutdown(wait=True)


if __name__ == "__main__":
    asyncio.run(run_test(parse_args()))