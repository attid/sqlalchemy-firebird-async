import asyncio
from functools import partial
from sqlalchemy.util.concurrency import await_only
from greenlet import getcurrent
from sqlalchemy.pool import AsyncAdaptedQueuePool
import sqlalchemy_firebird.firebird as firebird_sync
import firebird.driver as sync_driver


class AsyncCursor:
    def __init__(self, sync_cursor, loop):
        self._sync_cursor = sync_cursor
        self._loop = loop

    def _exec(self, func, *args, **kwargs):
        if getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None):
            return await_only(self._loop.run_in_executor(None, partial(func, *args, **kwargs)))
        else:
            return func(*args, **kwargs)

    def execute(self, operation, parameters=None):
        if parameters is None:
            return self._exec(self._sync_cursor.execute, operation)
        else:
            return self._exec(self._sync_cursor.execute, operation, parameters)

    def executemany(self, operation, seq_of_parameters):
        return self._exec(self._sync_cursor.executemany, operation, seq_of_parameters)

    def fetchone(self):
        return self._exec(self._sync_cursor.fetchone)

    def fetchmany(self, size=None):
        if size is None:
            return self._exec(self._sync_cursor.fetchmany)
        return self._exec(self._sync_cursor.fetchmany, size)

    def fetchall(self):
        return self._exec(self._sync_cursor.fetchall)

    def close(self):
        return self._exec(self._sync_cursor.close)
    
    async def _async_soft_close(self):
        pass
    
    def nextset(self):
        return self._exec(self._sync_cursor.nextset)

    def __getattr__(self, name):
        return getattr(self._sync_cursor, name)


class AsyncConnection:
    def __init__(self, sync_connection, loop):
        self._sync_connection = sync_connection
        self._loop = loop

    def _exec(self, func, *args, **kwargs):
        if getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None):
            return await_only(self._loop.run_in_executor(None, partial(func, *args, **kwargs)))
        else:
            return func(*args, **kwargs)

    def cursor(self):
        return AsyncCursor(self._sync_connection.cursor(), self._loop)

    def commit(self):
        return self._exec(self._sync_connection.commit)

    def rollback(self):
        return self._exec(self._sync_connection.rollback)

    def close(self):
        return self._exec(self._sync_connection.close)
    
    def terminate(self):
        return self._exec(self._sync_connection.close)

    def __getattr__(self, name):
        return getattr(self._sync_connection, name)


class AsyncDBAPI:
    def __init__(self, sync_dbapi):
        self._sync_dbapi = sync_dbapi
        self.paramstyle = getattr(sync_dbapi, "paramstyle", "qmark")
        self.apilevel = getattr(sync_dbapi, "apilevel", "2.0")
        self.threadsafety = getattr(sync_dbapi, "threadsafety", 1)
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
        
        if getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None):
            sync_conn = await_only(loop.run_in_executor(None, partial(self._sync_dbapi.connect, *args, **kwargs)))
        else:
            sync_conn = self._sync_dbapi.connect(*args, **kwargs)
            
        return AsyncConnection(sync_conn, loop)


from .compiler import PatchedFBTypeCompiler


class AsyncFirebirdDialect(firebird_sync.FBDialect_firebird):
    name = "firebird.firebird_async"
    driver = "firebird_async"
    is_async = True
    supports_statement_cache = False
    poolclass = AsyncAdaptedQueuePool
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type_compiler_instance = PatchedFBTypeCompiler(self)
        self.type_compiler = self.type_compiler_instance

    @classmethod
    def import_dbapi(cls):
        return AsyncDBAPI(sync_driver)

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()