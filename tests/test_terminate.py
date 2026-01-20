import pytest
from sqlalchemy import text

@pytest.mark.asyncio
async def test_engine_terminate(async_engine):
    """
    Test that connections are closed correctly during dispose.
    This should call do_terminate -> terminate.
    """
    # 1. Open a connection.
    async with async_engine.connect() as conn:
        await conn.execute(text("SELECT 1 FROM rdb$database"))
        # Do not close explicitly (the context manager will close it).
        
    # 2. Force pool closure.
    # dispose() calls pool.dispose(), which closes all connections.
    # If the pool is QueuePool (default), it may try to call terminate
    # for checked-out connections if they are considered "invalid" or on cleanup.
    
    # To guarantee terminate, you can simulate an error
    # or rely on the pool calling terminate on reset.
    
    await async_engine.dispose()
    
    # 3. Direct test of terminate on the connection object (low-level).
    # We need to reach the raw connection.
    raw_conn = await async_engine.raw_connection()
    try:
        # In SQLAlchemy 2.0+, raw_connection returns an adapter.
        # We need to call terminate, which expects the dialect.
        if hasattr(raw_conn, "terminate"):
             raw_conn.terminate()
        else:
             # If the method is missing, that is already an error
             # (AttributeError would have been raised above if it was called).
             # But the user's stack trace error was in dialect.do_terminate.
             
             # Emulate what the pool does:
             # dialect.do_terminate(dbapi_connection)
             dialect = async_engine.dialect
             # raw_conn is AsyncAdapt_dbapi_connection.
             # Inside it, .dbapi_connection is our AsyncConnection.
             
             # But terminate is called on dbapi_connection.
             # Try to call it on our AsyncConnection.
             real_conn = raw_conn.driver_connection
             
             # Check for the method, because its absence raises an error.
             # If we do not add it, AttributeError would be raised here (if called).
             
             # Emulate the call from the dialect:
             try:
                 dialect.do_terminate(real_conn)
             except AttributeError as e:
                 if "'terminate'" in str(e):
                     pytest.fail(f"Missing terminate method: {e}")
                 else:
                     raise e
    finally:
        raw_conn.close()
