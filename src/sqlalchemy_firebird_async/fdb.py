import asyncio
from functools import partial

from sqlalchemy.util.concurrency import await_only


class AsyncCursor:
    def __init__(self, sync_cursor, loop):
        self._sync_cursor = sync_cursor
        self._loop = loop

    def execute(self, operation, parameters=None):
        if parameters is None:
            func = partial(self._sync_cursor.execute, operation)
        else:
            func = partial(self._sync_cursor.execute, operation, parameters)
        await_only(self._loop.run_in_executor(None, func))
        return self

    def executemany(self, operation, seq_of_parameters):
        func = partial(self._sync_cursor.executemany, operation, seq_of_parameters)
        await_only(self._loop.run_in_executor(None, func))
        return self

    def fetchone(self):
        return await_only(self._loop.run_in_executor(None, self._sync_cursor.fetchone))

    def fetchmany(self, size=None):
        if size is None:
            return await_only(self._loop.run_in_executor(None, self._sync_cursor.fetchmany))
        func = partial(self._sync_cursor.fetchmany, size)
        return await_only(self._loop.run_in_executor(None, func))

    def fetchall(self):
        return await_only(self._loop.run_in_executor(None, self._sync_cursor.fetchall))

    def close(self):
        return await_only(self._loop.run_in_executor(None, self._sync_cursor.close))

    def __getattr__(self, name):
        return getattr(self._sync_cursor, name)


class AsyncConnection:
    def __init__(self, sync_connection, loop):
        self._sync_connection = sync_connection
        self._loop = loop

    def cursor(self):
        return AsyncCursor(self._sync_connection.cursor(), self._loop)

    def commit(self):
        await_only(self._loop.run_in_executor(None, self._sync_connection.commit))

    def rollback(self):
        await_only(self._loop.run_in_executor(None, self._sync_connection.rollback))

    def close(self):
        await_only(self._loop.run_in_executor(None, self._sync_connection.close))

    def __getattr__(self, name):
        return getattr(self._sync_connection, name)


class AsyncDBAPI:
    def __init__(self, sync_dbapi):
        self._sync_dbapi = sync_dbapi
        self.paramstyle = getattr(sync_dbapi, "paramstyle", "qmark")
        self.apilevel = getattr(sync_dbapi, "apilevel", "2.0")
        self.threadsafety = getattr(sync_dbapi, "threadsafety", 0)
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
            if hasattr(sync_dbapi, attr):
                setattr(self, attr, getattr(sync_dbapi, attr))

    def connect(self, *args, **kwargs):
        async_creator_fn = kwargs.pop("async_creator_fn", None)
        loop = asyncio.get_running_loop()
        if async_creator_fn is not None:
            sync_conn = await_only(async_creator_fn(*args, **kwargs))
        else:
            func = partial(self._sync_dbapi.connect, *args, **kwargs)
            sync_conn = await_only(loop.run_in_executor(None, func))
        return AsyncConnection(sync_conn, loop)

from sqlalchemy.pool import AsyncAdaptedQueuePool
import sqlalchemy_firebird.fdb as fdb


class AsyncFDBDialect(fdb.FBDialect_fdb):
    name = "firebird.fdb_async"
    driver = "fdb_async"
    is_async = True
    supports_statement_cache = False
    poolclass = AsyncAdaptedQueuePool

    @classmethod
    def import_dbapi(cls):
        import fdb as sync_fdb

        return AsyncDBAPI(sync_fdb)

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()
