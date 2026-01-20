import pytest
from sqlalchemy import text

@pytest.mark.asyncio
async def test_engine_terminate(async_engine):
    """
    Тест проверяет корректность закрытия соединений при dispose.
    Это должно вызывать do_terminate -> terminate.
    """
    # 1. Открываем соединение
    async with async_engine.connect() as conn:
        await conn.execute(text("SELECT 1 FROM rdb$database"))
        # Не закрываем явно (хотя контекст менеджер закроет)
        
    # 2. Форсируем закрытие пула
    # dispose() вызывает pool.dispose(), который закрывает все соединения.
    # Если пул был QueuePool (стандартный), он может попытаться вызвать terminate
    # для проверенных соединений, если они считаются "invalid" или при очистке.
    
    # Чтобы гарантированно вызвать terminate, можно попробовать сымитировать ошибку
    # или просто положиться на то, что пул вызывает terminate при сбросе.
    
    await async_engine.dispose()
    
    # 3. Прямой тест метода terminate на объекте соединения (низкоуровневый)
    # Нам нужно добраться до raw connection
    raw_conn = await async_engine.raw_connection()
    try:
        # В SQLAlchemy 2.0+ raw_connection возвращает адаптер
        # Нам нужно вызвать метод terminate, который ожидает диалект
        if hasattr(raw_conn, "terminate"):
             raw_conn.terminate()
        else:
             # Если метода нет, это уже ошибка (AttributeError бы вылетел выше, если бы вызов шел)
             # Но ошибка из стектрейса пользователя была в do_terminate диалекта
             
             # Эмулируем то, что делает пул:
             # dialect.do_terminate(dbapi_connection)
             dialect = async_engine.dialect
             # raw_conn это AsyncAdapt_dbapi_connection
             # Внутри него .dbapi_connection это наш AsyncConnection
             
             # Но terminate вызывается у dbapi_connection.
             # Попробуем вызвать его у нашего AsyncConnection
             real_conn = raw_conn.driver_connection
             
             # Проверяем наличие метода, так как его отсутствие вызывает ошибку
             # Если мы его не добавим, тут будет AttributeError (если вызывать)
             
             # Эмуляция вызова из диалекта:
             try:
                 dialect.do_terminate(real_conn)
             except AttributeError as e:
                 if "'terminate'" in str(e):
                     pytest.fail(f"Missing terminate method: {e}")
                 else:
                     raise e
    finally:
        raw_conn.close()
