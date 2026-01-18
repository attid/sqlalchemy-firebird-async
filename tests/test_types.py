import pytest
import decimal
import datetime
from sqlalchemy import text, select, Column, Integer, BigInteger, Float, Boolean, String, Numeric, Date, Time, DateTime
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class AllTypesTable(Base):
    __tablename__ = "all_types"
    id = Column(Integer, primary_key=True)
    f_integer = Column(Integer)
    f_bigint = Column(BigInteger)
    f_float = Column(Float)
    f_boolean = Column(Boolean)
    f_string = Column(String(100))
    f_decimal = Column(Numeric(10, 2))
    # f_date = Column(Date)
    # f_timestamp = Column(DateTime)

@pytest.mark.asyncio
async def test_primitive_types(async_engine):
    # 1. Создаем таблицу вручную
    async with async_engine.begin() as conn:
        try:
            await conn.execute(text("DROP TABLE all_types"))
        except Exception:
            pass
        
        # DDL для Firebird 3.0+ (BOOLEAN поддерживается)
        ddl = """
        CREATE TABLE all_types (
            id INTEGER PRIMARY KEY,
            f_integer INTEGER,
            f_bigint BIGINT,
            f_float FLOAT,
            f_boolean BOOLEAN,
            f_string VARCHAR(100),
            f_decimal DECIMAL(10, 2)
        )
        """
        await conn.execute(text(ddl))

    # 2. Вставка данных
    test_data = [
        {
            "id": 1,
            "f_integer": 42,
            "f_bigint": 9223372036854775807, # Max BIGINT
            "f_float": 3.14159,
            "f_boolean": True,
            "f_string": "Hello Firebird",
            "f_decimal": decimal.Decimal("123.45")
        },
        {
            "id": 2,
            "f_integer": -100,
            "f_bigint": -1000,
            "f_float": -0.001,
            "f_boolean": False,
            "f_string": "Another String",
            "f_decimal": decimal.Decimal("0.00")
        }
    ]

    async with async_engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO all_types (id, f_integer, f_bigint, f_float, f_boolean, f_string, f_decimal)
                VALUES (:id, :f_integer, :f_bigint, :f_float, :f_boolean, :f_string, :f_decimal)
            """),
            test_data
        )

    # 3. Выборка и проверка
    async with async_engine.connect() as conn:
        # Проверяем первую строку
        result = await conn.execute(text("SELECT * FROM all_types WHERE id = 1"))
        row = result.fetchone()
        
        assert row.f_integer == 42
        assert isinstance(row.f_integer, int)
        
        assert row.f_bigint == 9223372036854775807
        assert isinstance(row.f_bigint, int)
        
        # Float в FB может иметь погрешность, проверяем с допуском
        assert abs(row.f_float - 3.14159) < 0.00001
        assert isinstance(row.f_float, float)
        
        # Boolean (важно для FB3+)
        assert row.f_boolean is True
        assert isinstance(row.f_boolean, bool)
        
        assert row.f_string == "Hello Firebird"
        assert isinstance(row.f_string, str)
        
        assert row.f_decimal == decimal.Decimal("123.45")
        assert isinstance(row.f_decimal, decimal.Decimal)

        # 4. Проверка WHERE с параметрами разных типов
        # Это критично, так как здесь SQLAlchemy пытается рендерить типы для биндинга (CAST(? AS TYPE))
        
        # WHERE Integer
        res = await conn.execute(text("SELECT id FROM all_types WHERE f_integer = :val"), {"val": 42})
        assert res.scalar() == 1
        
        # WHERE Boolean
        # res = await conn.execute(text("SELECT id FROM all_types WHERE f_boolean = :val"), {"val": True})
        # assert res.scalar() == 1
        
        # WHERE String (тут мы уже фиксили баг с VARCHAR)
        res = await conn.execute(text("SELECT id FROM all_types WHERE f_string = :val"), {"val": "Hello Firebird"})
        assert res.scalar() == 1
