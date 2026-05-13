import pytest
import decimal
import datetime
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    delete,
    insert,
    text,
    update,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy_firebird_async.firebird_driver import AsyncFirebirdDialect


Base = declarative_base()


class Transactions(Base):
    __tablename__ = "t_transactions"

    hash = Column("hash", String(64), primary_key=True)
    description = Column("description", Text(4000), nullable=False)
    body = Column("body", Text(12000), nullable=False)
    add_dt = Column("add_dt", DateTime())
    updated_dt = Column("updated_dt", DateTime())
    uuid = Column("uuid", String(32))
    json = Column("json", Text(), nullable=True)
    state = Column("state", Integer())
    stellar_sequence = Column("stellar_sequence", BigInteger())
    source_account = Column("source_account", String(56))
    owner_id = Column("owner_id", BigInteger())


def test_text_with_length_insert_binds_compile_as_blob_text():
    stmt = insert(Transactions).values(
        hash="8a5ae51261b0a8ec44ef027aef5aaf94da5c39179e3c36a86194f49472e1d4b4",
        description="desc",
        body="A" * 8880,
        uuid="u" * 32,
        json="{}",
        state=0,
        stellar_sequence=1,
        source_account="G" + "A" * 55,
        owner_id=1,
    )

    compiled = str(stmt.compile(dialect=AsyncFirebirdDialect()))

    assert "CAST(:description AS BLOB SUB_TYPE TEXT)" in compiled
    assert "CAST(:body AS BLOB SUB_TYPE TEXT)" in compiled
    assert "CAST(:json AS BLOB SUB_TYPE TEXT)" in compiled
    assert "CAST(:description AS VARCHAR(2000))" not in compiled
    assert "CAST(:body AS VARCHAR(2000))" not in compiled


def test_text_with_length_update_binds_compile_as_blob_text():
    stmt = (
        update(Transactions)
        .where(Transactions.hash == "h")
        .values(description="desc", body="A" * 8880)
    )

    compiled = str(stmt.compile(dialect=AsyncFirebirdDialect()))

    assert "description=CAST(:description AS BLOB SUB_TYPE TEXT)" in compiled
    assert "body=CAST(:body AS BLOB SUB_TYPE TEXT)" in compiled
    assert "description=CAST(:description AS VARCHAR(2000))" not in compiled
    assert "body=CAST(:body AS VARCHAR(2000))" not in compiled


def test_text_with_length_delete_where_bind_does_not_compile_as_varchar():
    stmt = delete(Transactions).where(Transactions.body == "A" * 8880)

    compiled = str(stmt.compile(dialect=AsyncFirebirdDialect()))

    assert "CAST(:body_1 AS BLOB SUB_TYPE TEXT)" in compiled
    assert "CAST(:body_1 AS VARCHAR(2000))" not in compiled


@pytest.mark.asyncio
async def test_primitive_types(async_engine):
    """Test basic types: INT, BIGINT, FLOAT, BOOLEAN, VARCHAR, DECIMAL."""
    # 1. DDL
    async with async_engine.begin() as conn:
        try: await conn.execute(text("DROP TABLE all_types"))
        except Exception: pass
        
        await conn.execute(text("""
            CREATE TABLE all_types (
                id INTEGER PRIMARY KEY,
                f_integer INTEGER,
                f_bigint BIGINT,
                f_float FLOAT,
                f_boolean BOOLEAN,
                f_string VARCHAR(100),
                f_decimal DECIMAL(10, 2)
            )
        """))

    # 2. DML
    test_data = {
        "id": 1,
        "f_integer": 42,
        "f_bigint": 9223372036854775807,
        "f_float": 3.14159,
        "f_boolean": True,
        "f_string": "Hello Firebird",
        "f_decimal": decimal.Decimal("123.45")
    }

    async with async_engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO all_types (id, f_integer, f_bigint, f_float, f_boolean, f_string, f_decimal)
                VALUES (:id, :f_integer, :f_bigint, :f_float, :f_boolean, :f_string, :f_decimal)
            """),
            test_data
        )

    # 3. SELECT
    async with async_engine.connect() as conn:
        result = await conn.execute(text("SELECT * FROM all_types WHERE id = 1"))
        row = result.fetchone()
        assert row.f_integer == 42
        assert row.f_bigint == 9223372036854775807
        assert abs(row.f_float - 3.14159) < 0.00001
        assert row.f_boolean is True
        assert row.f_string == "Hello Firebird"
        assert row.f_decimal == decimal.Decimal("123.45")

@pytest.mark.asyncio
async def test_complex_types_fb4(async_engine):
    """
    Test FB4 complex types (INT128, Time Zones).
    Skipped for fdb_async because the legacy driver does not support them.
    """
    if "fdb_async" in async_engine.url.drivername:
        pytest.skip("fdb driver does not support INT128 or Time Zones")

    now_tz = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    test_val_38 = decimal.Decimal("1234567890123456789012345678.9012345678")
    test_blob = "Very large text blob " * 100

    async with async_engine.begin() as conn:
        try: await conn.execute(text("DROP TABLE complex_types"))
        except Exception: pass
        await conn.execute(text("""
            CREATE TABLE complex_types (
                id INTEGER PRIMARY KEY,
                f_numeric_38 NUMERIC(38, 10),
                f_timestamp_tz TIMESTAMP WITH TIME ZONE,
                f_blob BLOB SUB_TYPE TEXT
            )
        """))

    async with async_engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO complex_types (id, f_numeric_38, f_timestamp_tz, f_blob)
                VALUES (:id, :f_numeric_38, :f_timestamp_tz, :f_blob)
            """),
            {"id": 1, "f_numeric_38": test_val_38, "f_timestamp_tz": now_tz, "f_blob": test_blob}
        )

    async with async_engine.connect() as conn:
        result = await conn.execute(text("SELECT * FROM complex_types WHERE id = 1"))
        row = result.fetchone()
        assert row.f_numeric_38 == test_val_38
        assert isinstance(row.f_timestamp_tz, datetime.datetime)
        assert abs(row.f_timestamp_tz.timestamp() - now_tz.timestamp()) < 1.0
        assert row.f_blob == test_blob

@pytest.mark.asyncio
async def test_aggregations(async_engine):
    """Check aggregate functions."""
    if "fdb_async" in async_engine.url.drivername:
        pytest.skip("fdb driver has issues with SQLDA for aggregations in async wrapper")

    async with async_engine.begin() as conn:
        try: await conn.execute(text("DROP TABLE test_agg"))
        except Exception: pass
        await conn.execute(text("CREATE TABLE test_agg (val DECIMAL(10,2))"))
    
    async with async_engine.begin() as conn:
        for v in [10.5, 20.5, 30.0]:
            await conn.execute(text("INSERT INTO test_agg (val) VALUES (:v)"), {"v": v})
        
    async with async_engine.connect() as conn:
        res = await conn.execute(text("SELECT SUM(val), COUNT(*) FROM test_agg"))
        s, c = res.fetchone()
        assert s == decimal.Decimal("61.00")
        assert c == 3

@pytest.mark.asyncio
async def test_insert_returning(async_engine):
    """Check RETURNING."""
    async with async_engine.begin() as conn:
        try: await conn.execute(text("DROP TABLE test_ret"))
        except Exception: pass
        await conn.execute(text("CREATE TABLE test_ret (id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name VARCHAR(50))"))
    
    async with async_engine.begin() as conn:
        res = await conn.execute(
            text("INSERT INTO test_ret (name) VALUES (:name) RETURNING id"),
            {"name": "test"}
        )
        new_id = res.scalar()
        assert isinstance(new_id, int)

@pytest.mark.asyncio
async def test_statement_string_compilation(async_engine):
    """Check statement compilation to string (for print)."""
    stmt = text("SELECT 1 FROM rdb$database WHERE 1 = :id").bindparams(id=1)
    compiled_str = str(stmt.compile(async_engine.sync_engine))
    assert "1 = " in compiled_str
