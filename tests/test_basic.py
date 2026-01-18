import pytest
from sqlalchemy import text
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))

@pytest.mark.asyncio
async def test_simple_select(async_engine):
    async with async_engine.connect() as conn:
        result = await conn.execute(text("SELECT 1 FROM rdb$database"))
        assert result.scalar() == 1

@pytest.mark.asyncio
async def test_create_insert_select(async_engine):
    # 1. Создаем таблицы
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    # 2. Вставляем данные
    async with async_engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO users (id, name) VALUES (:id, :name)"),
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        )

    # 3. Выбираем данные
    async with async_engine.connect() as conn:
        result = await conn.execute(text("SELECT name FROM users ORDER BY id"))
        rows = result.fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "Alice"
        assert rows[1][0] == "Bob"
