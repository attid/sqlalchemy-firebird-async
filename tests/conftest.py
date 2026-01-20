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

# Глобальная переменная для хранения контейнера compliance тестов
_compliance_container = None

def pytest_sessionstart(session):
    """
    Запускает Docker контейнер и настраивает setup.cfg для SQLAlchemy Compliance Suite.
    Этот хук выполняется до сбора тестов.
    """
    # Проверяем, есть ли тесты compliance в запуске (чтобы не поднимать докер зря)
    # Но session.config.args может быть сложным. Проще поднять всегда или проверять наличие флага.
    
    global _compliance_container
    
    # Если мы уже в CI/CD или есть внешняя база, пропускаем
    if os.getenv("TEST_EXTERNAL_DB"):
        return

    print("Starting Firebird container for Compliance Suite...")
    container = DockerContainer(FIREBIRD_IMAGE)
    container.with_env("FIREBIRD_USER", DB_USER)
    container.with_env("FIREBIRD_PASSWORD", DB_PASS)
    container.with_env("FIREBIRD_DATABASE", DB_NAME)
    container.with_env("FIREBIRD_DATABASE_DEFAULT_CHARSET", "UTF8")
    container.with_bind_ports(FIREBIRD_PORT, None)
    
    try:
        container.start()
        _compliance_container = container
        
        # Ждем старта
        time.sleep(5)
        
        # Получаем параметры
        host = container.get_container_host_ip()
        port = container.get_exposed_port(FIREBIRD_PORT)
        
        # Создаем БД для compliance тестов (отдельную от test.fdb, хотя можно и ту же)
        # test.fdb уже создана при старте контейнера (переменной FIREBIRD_DATABASE).
        # Но Compliance Suite любит удалять таблицы.
        
        # Формируем URL для fdb_async (наш эталон)
        # Важно: путь внутри контейнера //var/lib/firebird/data/test.fdb
        db_path = f"//var/lib/firebird/data/{DB_NAME}"
        url = f"firebird+fdb_async://{DB_USER}:{DB_PASS}@{host}:{port}{db_path}?charset=UTF8"
        
        print(f"Compliance Suite URL: {url}")
        
        # Генерируем setup.cfg динамически!
        setup_content = f"""
[db]
default = {url}

[sqla_testing]
requirement_cls = tests.requirements:Requirements
profile_file = .profiles.txt
"""
        with open("setup.cfg", "w") as f:
            f.write(setup_content)
            
    except Exception as e:
        print(f"Failed to start container for compliance tests: {e}")
        if _compliance_container:
            _compliance_container.stop()
            _compliance_container = None

def pytest_sessionfinish(session, exitstatus):
    """
    Останавливает контейнер после завершения всех тестов.
    """
    global _compliance_container
    if _compliance_container:
        print("Stopping Firebird container...")
        _compliance_container.stop()
        _compliance_container = None
        # Удаляем временный конфиг
        if os.path.exists("setup.cfg"):
            os.remove("setup.cfg")

@pytest.fixture(scope="session")
def firebird_container():
    """
    Фикстура для обычных тестов (test_types, test_basic).
    Если compliance контейнер уже запущен глобально, используем его!
    """
    if _compliance_container:
        yield _compliance_container
    elif os.getenv("TEST_EXTERNAL_DB"):
        yield None
    else:
        # Если compliance не запускался (например, при запуске одного файла), поднимаем свой
        container = DockerContainer(FIREBIRD_IMAGE)
        container.with_env("FIREBIRD_USER", DB_USER)
        container.with_env("FIREBIRD_PASSWORD", DB_PASS)
        container.with_env("FIREBIRD_DATABASE", DB_NAME)
        container.with_env("FIREBIRD_DATABASE_DEFAULT_CHARSET", "UTF8")
        container.with_bind_ports(FIREBIRD_PORT, None)
        container.start()
        time.sleep(5)
        try:
            yield container
        finally:
            container.stop()

@pytest.fixture(scope="session")
def db_url(firebird_container):
    """
    Формирует URL подключения для обычных тестов.
    """
    if firebird_container:
        host = firebird_container.get_container_host_ip()
        port = firebird_container.get_exposed_port(FIREBIRD_PORT)
    else: host = "localhost"; port = 3050
        
    dialect = os.getenv("TEST_DIALECT", "fdb_async")
    
    db_path = f"//var/lib/firebird/data/{DB_NAME}"
    url = f"firebird+[{dialect}]://{DB_USER}:{DB_PASS}@{host}:{port}{db_path}?charset=UTF8".replace("[", "").replace("]", "")
    print(f"\n[DEBUG] Connecting to: {url}")
    return url

@pytest_asyncio.fixture
async def async_engine(db_url):
    engine = create_async_engine(db_url, echo=False)
    yield engine
    await engine.dispose()
