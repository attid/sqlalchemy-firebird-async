import asyncio
from sqlalchemy import util
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util.concurrency import await_only
from greenlet import getcurrent

import firebirdsql
import firebirdsql.aio as aio
import sqlalchemy_firebird.fdb as fdb
from .compiler import PatchedFBTypeCompiler


def _await_if_needed(value, loop):
    if asyncio.iscoroutine(value):
        if getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None):
            return await_only(value)
        else:
            # We are in a sync context (e.g. run_sync), but need to await a coroutine.
            # Since the loop is running in another thread, we can use run_coroutine_threadsafe.
            future = asyncio.run_coroutine_threadsafe(value, loop)
            return future.result()
    return value


class AsyncPyfbCursor:
    def __init__(self, async_cursor, loop):
        self._async_cursor = async_cursor
        self._loop = loop

    def execute(self, operation, parameters=None):
        if parameters is None:
            _await_if_needed(self._async_cursor.execute(operation), self._loop)
        else:
            _await_if_needed(self._async_cursor.execute(operation, parameters), self._loop)
        return self

    def executemany(self, operation, seq_of_parameters):
        _await_if_needed(self._async_cursor.executemany(operation, seq_of_parameters), self._loop)
        return self

    def fetchone(self):
        return _await_if_needed(self._async_cursor.fetchone(), self._loop)

    def fetchmany(self, size=None):
        return _await_if_needed(self._async_cursor.fetchmany(size), self._loop)

    def fetchall(self):
        return _await_if_needed(self._async_cursor.fetchall(), self._loop)

    def close(self):
        return _await_if_needed(self._async_cursor.close(), self._loop)

    async def _async_soft_close(self):
        pass

    def __getattr__(self, name):
        return getattr(self._async_cursor, name)


class AsyncPyfbConnection:
    def __init__(self, async_connection, loop):
        self._async_connection = async_connection
        self._loop = loop

    def cursor(self):
        return AsyncPyfbCursor(self._async_connection.cursor(), self._loop)

    def commit(self):
        _await_if_needed(self._async_connection.commit(), self._loop)

    def rollback(self):
        _await_if_needed(self._async_connection.rollback(), self._loop)

    def close(self):
        try:
            return _await_if_needed(self._async_connection.close(), self._loop)
        except BlockingIOError:
            # Fallback logic from original code
            sock = getattr(self._async_connection, "sock", None)
            raw_sock = getattr(sock, "_sock", None)
            if raw_sock is not None:
                try:
                    raw_sock.setblocking(True)
                except Exception:
                    pass
            try:
                # Synchronous close attempt if possible? 
                # firebirdsql.aio connection close is async.
                # If we are here, something is wrong.
                # Just ignore for now or try standard close if it has one?
                pass
            except Exception:
                pass
            return None

    def __getattr__(self, name):
        return getattr(self._async_connection, name)


class AsyncPyfbDBAPI:
    def __init__(self):
        self.paramstyle = getattr(firebirdsql, "paramstyle", "qmark")
        self.apilevel = getattr(firebirdsql, "apilevel", "2.0")
        self.threadsafety = getattr(firebirdsql, "threadsafety", 0)
        for attr in (
            "Warning",
            "Error",
            "InterfaceError",
            "DatabaseError",
            "DataError",
            "OperationalError",
            "IntegrityError",
            "InternalError",
            "ProgrammingError",
            "NotSupportedError",
        ):
            if hasattr(firebirdsql, attr):
                setattr(self, attr, getattr(firebirdsql, attr))

    def connect(self, *args, **kwargs):
        async_creator_fn = kwargs.pop("async_creator_fn", None)
        loop = asyncio.get_running_loop()
        
        # We are likely in a greenlet here (engine.connect)
        if async_creator_fn is None:
            async_creator_fn = aio.connect
            
        async_connection = await_only(async_creator_fn(*args, **kwargs))
        return AsyncPyfbConnection(async_connection, loop)

    def Binary(self, value):
        return firebirdsql.Binary(value)


class AsyncFirebirdSQLDialect(fdb.FBDialect_fdb):
    name = "firebird.firebirdsql_async"
    driver = "firebirdsql_async"
    is_async = True
    supports_statement_cache = False
    poolclass = AsyncAdaptedQueuePool
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type_compiler_instance = PatchedFBTypeCompiler(self)
        self.type_compiler = self.type_compiler_instance

    @classmethod
    def import_dbapi(cls):
        return AsyncPyfbDBAPI()

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username="user")
        opts.update(url.query)
        util.coerce_kw_type(opts, "port", int)
        return ([], opts)

    def _get_server_version_info(self, connection):
        # Override to avoid issues with async scalar execution during dialect initialization
        try:
            # We can try to use the connection to execute SQL
            # But in async mode, this might be tricky if not in greenlet.
            # Assuming connection is AsyncPyfbConnection wrapper.
            
            # Simple workaround: return a dummy version or suppress error
            # Or try to execute:
            res = connection.exec_driver_sql(
                "select rdb$get_context('SYSTEM','ENGINE_VERSION') from rdb$database"
            )
            val = res.scalar() 
            # exec_driver_sql returns CursorResult. scalar() triggers fetchone().
            # Our fetchone handles both sync/async contexts now.
            version_str = val
                
        except Exception:
            return (0, 0)
            
        if not version_str:
            return (0, 0)
            
        parts = str(version_str).split(".")
        try:
            major = int(parts[0])
            minor = int(parts[1]) if len(parts) > 1 else 0
            return (major, minor)
        except (ValueError, IndexError):
            return (0, 0)