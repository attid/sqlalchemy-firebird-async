import pytest
import enum
from sqlalchemy import Column, Integer, Enum, select
from sqlalchemy.orm import declarative_base

class MyEnum(enum.Enum):
    ONE = "one"
    TWO = "two"

Base = declarative_base()

class MyTable(Base):
    __tablename__ = 'test_enum_table_real'
    id = Column(Integer, primary_key=True)
    # native_enum=True is default, but Firebird dialect doesn't support native enums.
    # SQLAlchemy adapts this to String/VARCHAR with check constraints usually.
    # Our fix ensures the adaptation doesn't crash.
    status = Column(Enum(MyEnum), nullable=False)

@pytest.mark.asyncio
async def test_enum_round_trip(async_engine):
    """Test creating table with Enum, inserting, and selecting."""
    
    # 1. Create Table
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    # 2. Insert
    from sqlalchemy import insert
    async with async_engine.begin() as conn:
        await conn.execute(insert(MyTable).values(id=1, status=MyEnum.ONE))
        await conn.execute(insert(MyTable).values(id=2, status=MyEnum.TWO))

    # 3. Select
    async with async_engine.connect() as conn:
        stmt = select(MyTable).where(MyTable.status == MyEnum.ONE)
        result = await conn.execute(stmt)
        row = result.fetchone()
        
        assert row is not None
        assert row.id == 1
        # SQLAlchemy Enum type should convert the string back to Enum member automatically
        assert row.status == MyEnum.ONE

    # Cleanup
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
