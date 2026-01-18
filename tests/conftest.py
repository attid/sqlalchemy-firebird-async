import asyncio
import os
import time
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.core.container import DockerContainer

# Образ Firebird
FIREBIRD_IMAGE = "firebirdsql/firebird:4.0.5"
FIREBIRD_PORT = 3050
DB_USER = "testuser"
DB_PASS = "testpass"
DB_NAME = "test.fdb"

@pytest.fixture(scope="session")
def firebird_container():
    """
    Запускает контейнер Firebird на время всей сессии тестов.
    """
    if os.getenv("TEST_EXTERNAL_DB"):
        yield None
        return

    # Запускаем контейнер с параметрами для автосоздания БД
    container = DockerContainer(FIREBIRD_IMAGE)
    container.with_env("FIREBIRD_USER", DB_USER)
    container.with_env("FIREBIRD_PASSWORD", DB_PASS)
    container.with_env("FIREBIRD_DATABASE", DB_NAME)
    container.with_env("FIREBIRD_DATABASE_DEFAULT_CHARSET", "UTF8")
    
    # Можно не биндить том, если нам не нужны данные после теста
    container.with_bind_ports(FIREBIRD_PORT, None)
    
    container.start()
    
    # Даем время на инициализацию БД (можно заменить на ожидание логов)
    time.sleep(5) 
    
    try:
        yield container
    finally:
        # Если тесты упали, можно было бы вывести логи, но pytest их перехватит
        container.stop()

@pytest.fixture(scope="session")
def db_url(firebird_container):
    """
    Формирует URL подключения.
    """
    if firebird_container:
        host = firebird_container.get_container_host_ip()
        port = firebird_container.get_exposed_port(FIREBIRD_PORT)
    else: host = "localhost"; port = 3050
        
    # Выбор диалекта через переменную окружения, по умолчанию fdb_async
    dialect = os.getenv("TEST_DIALECT", "fdb_async")
    
    # Двойной слэш после порта нужен для абсолютного пути
    db_path = f"//var/lib/firebird/data/{DB_NAME}"
    url = f"firebird+[{dialect}]://{DB_USER}:{DB_PASS}@{host}:{port}{db_path}?charset=UTF8".replace("[", "").replace("]", "")
    print(f"\n[DEBUG] Connecting to: {url}")
    return url

@pytest_asyncio.fixture
async def async_engine(db_url):
    engine = create_async_engine(db_url, echo=False)
    yield engine
    await engine.dispose()