import asyncio
import os
import socket
import time
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.core.container import DockerContainer

# Firebird image
FIREBIRD_IMAGE = "firebirdsql/firebird:4.0.5"
FIREBIRD_PORT = 3050
DB_USER = "testuser"
DB_PASS = "testpass"
DB_NAME = "test.fdb"

# Global variable for the compliance test container
_compliance_container = None

def _normalize_host(host: str) -> str:
    if host in ("localhost", "127.0.0.1", "::1"):
        return "127.0.0.1"
    return host

def _wait_for_port(host: str, port: int, timeout: float = 60.0, interval: float = 0.5) -> None:
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError as exc:
            last_err = exc
            time.sleep(interval)
    raise RuntimeError(
        f"Firebird did not start listening on {host}:{port} within {timeout:.1f}s: {last_err}"
    )

def _wait_for_firebird_ready(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    timeout: float = 60.0,
    interval: float = 1.0,
) -> None:
    try:
        import fdb
    except ImportError:
        return

    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            conn = fdb.connect(
                host=f"{host}/{port}",
                database=database,
                user=user,
                password=password,
                charset="UTF8",
            )
            conn.close()
            return
        except Exception as exc:
            last_err = exc
            time.sleep(interval)

    raise RuntimeError(
        f"Firebird did not accept connections on {host}:{port} within {timeout:.1f}s: {last_err}"
    )

@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    """
    Starts the Docker container and configures setup.cfg for the SQLAlchemy Compliance Suite.
    This hook runs early so the SQLAlchemy testing plugin can read the config.
    """
    # Check whether compliance tests are in this run (so we do not start Docker unnecessarily).
    # But config.args can be complex. It is simpler to always start it or check a flag.
    
    global _compliance_container
    
    # If we are already in CI/CD or using an external DB, skip.
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
        
        # Get parameters.
        host = _normalize_host(container.get_container_host_ip())
        port = int(container.get_exposed_port(FIREBIRD_PORT))
        _wait_for_port(host, port)
        _wait_for_firebird_ready(
            host=host,
            port=port,
            database=f"/var/lib/firebird/data/{DB_NAME}",
            user=DB_USER,
            password=DB_PASS,
        )
        
        # Create a DB for compliance tests (separate from test.fdb, though it could be the same).
        # test.fdb is created at container start via FIREBIRD_DATABASE.
        # Compliance Suite likes to drop tables.
        
        # Build the URL for the selected dialect.
        # Important: path inside the container is //var/lib/firebird/data/test.fdb.
        db_path = f"//var/lib/firebird/data/{DB_NAME}"
        dialect = os.getenv("TEST_DIALECT", "fdb_async")
        url = f"firebird+{dialect}://{DB_USER}:{DB_PASS}@{host}:{port}{db_path}?charset=UTF8"
        
        print(f"Compliance Suite URL: {url}")
        
        # Generate setup.cfg dynamically.
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
    Stops the container after all tests finish.
    """
    global _compliance_container
    if _compliance_container:
        print("Stopping Firebird container...")
        _compliance_container.stop()
        _compliance_container = None
        # Remove the temporary config.
        if os.path.exists("setup.cfg"):
            os.remove("setup.cfg")

@pytest.fixture(scope="session")
def firebird_container():
    """
    Fixture for regular tests (test_types, test_basic).
    If the compliance container is already running, reuse it.
    """
    if _compliance_container:
        yield _compliance_container
    elif os.getenv("TEST_EXTERNAL_DB"):
        yield None
    else:
        # If compliance did not run (e.g. when running a single file), start our own.
        container = DockerContainer(FIREBIRD_IMAGE)
        container.with_env("FIREBIRD_USER", DB_USER)
        container.with_env("FIREBIRD_PASSWORD", DB_PASS)
        container.with_env("FIREBIRD_DATABASE", DB_NAME)
        container.with_env("FIREBIRD_DATABASE_DEFAULT_CHARSET", "UTF8")
        container.with_bind_ports(FIREBIRD_PORT, None)
        container.start()
        host = _normalize_host(container.get_container_host_ip())
        port = int(container.get_exposed_port(FIREBIRD_PORT))
        _wait_for_port(host, port)
        _wait_for_firebird_ready(
            host=host,
            port=port,
            database=f"/var/lib/firebird/data/{DB_NAME}",
            user=DB_USER,
            password=DB_PASS,
        )
        try:
            yield container
        finally:
            container.stop()

@pytest.fixture(scope="session")
def db_url(firebird_container):
    """
    Builds the connection URL for regular tests.
    """
    if firebird_container:
        host = _normalize_host(firebird_container.get_container_host_ip())
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
