import asyncio

from sqlalchemy import util
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util.concurrency import await_only

import firebirdsql
import firebirdsql.aio as aio
import sqlalchemy_firebird.fdb as fdb


def _await_if_needed(value):
    if asyncio.iscoroutine(value):
        try:
            return await_only(value)
        except StopAsyncIteration:
            return None
    return value


class AsyncPyfbCursor:
    def __init__(self, async_cursor):
        self._async_cursor = async_cursor

    def execute(self, operation, parameters=None):
        if parameters is None:
            _await_if_needed(self._async_cursor.execute(operation))
        else:
            _await_if_needed(self._async_cursor.execute(operation, parameters))
        return self

    def executemany(self, operation, seq_of_parameters):
        _await_if_needed(self._async_cursor.executemany(operation, seq_of_parameters))
        return self

    def fetchone(self):
        return _await_if_needed(self._async_cursor.fetchone())

    def fetchmany(self, size=None):
        # firebirdsql fetchmany signature is fetchmany(self, size=None)
        # but if size is passed it must be used.
        return _await_if_needed(self._async_cursor.fetchmany(size))

    def fetchall(self):
        return _await_if_needed(self._async_cursor.fetchall())

    def close(self):
        return _await_if_needed(self._async_cursor.close())

    async def _async_soft_close(self):
        # SQLAlchemy 2.0 calls this.
        pass

    def __getattr__(self, name):
        return getattr(self._async_cursor, name)


class AsyncPyfbConnection:
    def __init__(self, async_connection):
        self._async_connection = async_connection

    def cursor(self):
        return AsyncPyfbCursor(self._async_connection.cursor())

    def commit(self):
        _await_if_needed(self._async_connection.commit())

    def rollback(self):
        _await_if_needed(self._async_connection.rollback())

    def close(self):
        try:
            return _await_if_needed(self._async_connection.close())
        except BlockingIOError:
            sock = getattr(self._async_connection, "sock", None)
            raw_sock = getattr(sock, "_sock", None)
            if raw_sock is not None:
                try:
                    raw_sock.setblocking(True)
                except Exception:
                    pass
            try:
                return self._async_connection.close()
            except Exception:
                if raw_sock is not None:
                    try:
                        raw_sock.close()
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
        if async_creator_fn is None:
            async_creator_fn = aio.connect
        async_connection = await_only(async_creator_fn(*args, **kwargs))
        return AsyncPyfbConnection(async_connection)

    def Binary(self, value):
        return firebirdsql.Binary(value)


class AsyncFirebirdSQLDialect(fdb.FBDialect_fdb):
    name = "firebird.firebirdsql_async"
    driver = "firebirdsql_async"
    is_async = True
    supports_statement_cache = False
    poolclass = AsyncAdaptedQueuePool

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
        try:
            # We must use exec_driver_sql and await the result because scalar() 
            # on async connection returns a coroutine.
            # But wait, 'connection' passed here is likely an AsyncAdapt_dbapi_connection
            # wrapper which mimics sync interface but executes via greenlet_spawn?
            # Or is it a raw connection?
            # In asyncio dialect, _get_server_version_info is called in a sync context.
            
            # Let's try standard way, but safe:
            res = connection.exec_driver_sql(
                "select rdb$get_context('SYSTEM','ENGINE_VERSION') from rdb$database"
            )
            # res.scalar() is a coroutine if we are in async mode?
            # Actually, exec_driver_sql on AsyncConnection returns a CursorResult that
            # has sync-like interface if used inside run_sync, but here we are inside dialect method.
            
            # Safe way:
            val = res.scalar()
            if asyncio.iscoroutine(val):
                version_str = await_only(val)
            else:
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
