import asyncio
import time
import pytest
import pytest_asyncio
from sqlalchemy import text, Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class LoadTestTable(Base):
    __tablename__ = "test_table"
    id = Column(Integer, primary_key=True)
    data = Column(String(200))

@pytest_asyncio.fixture(scope="module") # Одна таблица на модуль
async def heavy_data_engine(db_url):
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine(db_url, echo=False)
    
    # Создаем таблицу и наполняем данными
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        
        # Генерируем данные. 
        # Firebird может быть медленным на построчной вставке, используем executemany
        # Вставим 1000 строк, чтобы было что перемножать
        data = [{"id": i, "data": f"Row number {i}"} for i in range(1000)]
        await conn.execute(
            text("INSERT INTO test_table (id, data) VALUES (:id, :data)"),
            data
        )
        
    yield engine
    await engine.dispose()

async def worker(engine, worker_id, duration_threshold=0.5):
    """
    Выполняет запрос, который должен занимать некоторое время.
    Мы используем cross join таблицы самой на себя, чтобы нагрузить БД.
    """
    # Запрос: декартово произведение 1000x1000 = 1 млн строк.
    # Считаем COUNT(*), чтобы не гонять данные по сети, а грузить именно движок БД.
    query = "SELECT count(*) FROM test_table a, test_table b"
    
    start = time.perf_counter()
    async with engine.connect() as conn:
        result = await conn.execute(text(query))
        count = result.scalar()
    end = time.perf_counter()
    
    duration = end - start
    print(f"[Worker {worker_id}] Rows: {count}, Time: {duration:.4f}s")
    return duration

@pytest.mark.asyncio
async def test_concurrency(heavy_data_engine):
    """
    Запускаем 5 параллельных запросов.
    """
    workers_count = 5
    
    start_total = time.perf_counter()
    
    tasks = [worker(heavy_data_engine, i) for i in range(workers_count)]
    results = await asyncio.gather(*tasks)
    
    end_total = time.perf_counter()
    total_time = end_total - start_total
    
    print(f"\nTotal time for {workers_count} concurrent requests: {total_time:.4f}s")
    print(f"Sum of individual times: {sum(results):.4f}s")
    
    # Проверка на адекватность:
    # 1. Все запросы выполнились успешно
    assert len(results) == workers_count
    # 2. База жива
    async with heavy_data_engine.connect() as conn:
        assert await conn.scalar(text("SELECT 1 FROM rdb$database")) == 1
