import pytest
from sqlalchemy import text, select
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession

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
    # 1. Create tables manually (work around a create_all bug in sqlalchemy-firebird).
    async with async_engine.begin() as conn:
        # Try to drop if it exists.
        try:
            await conn.execute(text("DROP TABLE users"))
        except Exception:
            pass
        
        await conn.execute(text("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))"))

    # 2. Insert data via Core (SQL).
    async with async_engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO users (id, name) VALUES (:id, :name)"),
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        )

    # 3. Select data via Core.
    async with async_engine.connect() as conn:
        result = await conn.execute(text("SELECT name FROM users ORDER BY id"))
        rows = result.fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "Alice"
        assert rows[1][0] == "Bob"

@pytest.mark.asyncio
async def test_orm_session(async_engine):
    # Full ORM test with a session.
    
    # Prepare the table (manual, as above).
    async with async_engine.begin() as conn:
        try:
            await conn.execute(text("DROP TABLE users"))
        except Exception:
            pass
        await conn.execute(text("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))"))

    # Create a session factory.
    async_session = sessionmaker(
        async_engine, class_=AsyncSession, expire_on_commit=False
    )

    # Insert via ORM.
    async with async_session() as session:
        user1 = User(id=10, name="Charlie")
        user2 = User(id=20, name="Dave")
        session.add_all([user1, user2])
        await session.commit()

    # Query via ORM.
    async with async_session() as session:
        # select(User) -> returns model objects.
        stmt = select(User).order_by(User.id)
        result = await session.execute(stmt)
        users = result.scalars().all()
        
        assert len(users) == 2
        assert users[0].name == "Charlie"
        assert users[1].name == "Dave"
