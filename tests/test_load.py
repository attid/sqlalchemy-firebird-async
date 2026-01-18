import asyncio
import time
import pytest
import os
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Простой тяжелый запрос: декартово произведение системных таблиц
BASE_SQL_QUERY = """
SELECT count(*)
FROM rdb$fields a, rdb$fields b
"""

def force_async_scheme(dsn: str, scheme: str) -> str:
    """Подменяет схему драйвера в DSN."""
    if "://" not in dsn:
        return f"firebird+{scheme}://{dsn}"
    _, rest = dsn.split("://", 1)
    return f"firebird+{scheme}://{rest}"

def build_raw_query() -> str:
    return BASE_SQL_QUERY.strip()

async def run_worker(engine, worker_id: int, repeats: int, raw_query: str):
    results = []
    # Каждое соединение в отдельном потоке (для fdb)
    async with engine.connect() as connection:
        worker_start = time.perf_counter()
        for i in range(repeats):
            start = time.perf_counter()
            result = await connection.execute(text(raw_query))
            rows = result.fetchall()
            
            duration = time.perf_counter() - start
            row_count = rows[0][0]
            results.append((duration, row_count))
            
        worker_total = time.perf_counter() - worker_start
    return results

@pytest.mark.asyncio
async def test_load_concurrency(firebird_container):
    """
    Нагрузочный тест для проверки асинхронности (параллельности) выполнения запросов.
    Проверяет оба драйвера: fdb_async и firebird_async.
    """
    
    # Параметры теста
    THREADS = 4
    REPEATS = 20
    
    # Определяем параметры подключения на основе фикстуры контейнера
    if firebird_container:
        host = firebird_container.get_container_host_ip()
        port = firebird_container.get_exposed_port(3050)
        # Настройки из conftest.py
        db_name = "test.fdb"
        user = "testuser"
        password = "testpass"
    else:
        # Fallback если контейнер не используется (например, внешний сервер)
        host = "localhost"
        port = 3050
        db_name = "test.fdb"
        user = "testuser"
        password = "testpass"

    # Абсолютный путь к БД (важно для Firebird в Docker)
    db_path = f"//var/lib/firebird/data/{db_name}"
    base_dsn = f"firebird://{user}:{password}@{host}:{port}{db_path}?charset=UTF8"

    target_dialects = [
        ("fdb_async", "fdb"),
        ("firebird_async", "firebird-driver") 
    ]
    
    # Настраиваем executor для асинхронного цикла, чтобы fdb (threaded) мог развернуться
    executor = ThreadPoolExecutor(max_workers=THREADS * 2)
    loop = asyncio.get_running_loop()
    # Сохраняем старый executor, чтобы вернуть (хотя pytest создаст новый loop для след теста)
    loop.set_default_executor(executor)
    
    try:
        for scheme, label in target_dialects:
            print(f"\n{'='*20} Testing {label} ({scheme}) {'='*20}")
            
            dsn = force_async_scheme(base_dsn, scheme)
            engine = create_async_engine(dsn, pool_size=THREADS, max_overflow=10, echo=False)
            
            try:
                # Прогрев и проверка соединения
                async with engine.connect() as conn:
                     await conn.execute(text("SELECT 1 FROM rdb$database"))
                
                raw_query = build_raw_query()

                start_all = time.perf_counter()
                
                tasks = [
                    run_worker(engine, worker_id + 1, REPEATS, raw_query)
                    for worker_id in range(THREADS)
                ]
                
                results_per_worker = await asyncio.gather(*tasks)
                
                total_time = time.perf_counter() - start_all
                
                # Анализ результатов
                all_results = [item for sublist in results_per_worker for item in sublist]
                durations = [d for d, _ in all_results]
                sum_durations = sum(durations)
                
                print(f"Total wall time: {total_time:.4f}s")
                print(f"Sum of query times: {sum_durations:.4f}s")
                
                if total_time > 0:
                    ratio = sum_durations / total_time
                    print(f"Parallel ratio: {ratio:.2f}x (Target: close to {THREADS}x)")
                    
                    # Простейшая проверка: если ratio < 1.5 при 4 потоках, значит блокируется
                    # Для GitHub Actions или слабого CPU можно снизить порог, но 2.0+ ожидаемо.
                    if THREADS >= 2:
                        assert ratio > 1.5, f"Low concurrency detected for {label}! Ratio: {ratio:.2f}x"

            except Exception as e:
                pytest.fail(f"Driver {label} failed: {e}")
            finally:
                await engine.dispose()
    finally:
        executor.shutdown(wait=False)