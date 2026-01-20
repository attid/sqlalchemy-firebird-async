import asyncio
from functools import partial
from sqlalchemy.util.concurrency import await_only
from greenlet import getcurrent


class AsyncCursor:
    def __init__(self, sync_cursor, loop):
        self._sync_cursor = sync_cursor
        self._loop = loop

    def _exec(self, func, *args, **kwargs):
        # Проверяем, находимся ли мы в контексте greenlet, созданном SQLAlchemy
        if getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None):
            return await_only(self._loop.run_in_executor(None, partial(func, *args, **kwargs)))
        else:
            # Если нет, вызываем синхронно (например, внутри run_sync)
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
        
        def _connect():
            if async_creator_fn is not None:
                # Здесь мы не можем просто так вызвать await_only, если creator асинхронный
                # Но fdb синхронный, так что всё ок.
                # Если передан async_creator, то это для firebirdsql, но мы в fdb.py
                return async_creator_fn(*args, **kwargs) # вернет корутину? нет, это коллбек
            else:
                return self._sync_dbapi.connect(*args, **kwargs)

        if getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None):
            # Если async_creator_fn возвращает корутину, то await_only ее дождется
            # Но для fdb это синхронный вызов, поэтому run_in_executor
             sync_conn = await_only(loop.run_in_executor(None, partial(self._sync_dbapi.connect, *args, **kwargs)))
        else:
             sync_conn = self._sync_dbapi.connect(*args, **kwargs)
            
        return AsyncConnection(sync_conn, loop)

from sqlalchemy.pool import AsyncAdaptedQueuePool
import sqlalchemy_firebird.fdb as fdb
from .compiler import PatchedFBTypeCompiler


class AsyncFDBDialect(fdb.FBDialect_fdb):
    name = "firebird.fdb_async"
    driver = "fdb_async"
    is_async = True
    supports_statement_cache = False
    poolclass = AsyncAdaptedQueuePool
    # Explicitly set type compiler to ensure our patch is used
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type_compiler_instance = PatchedFBTypeCompiler(self)
        self.type_compiler = self.type_compiler_instance

    def is_disconnect(self, e, connection, cursor):
        # Handle fdb disconnect errors which store error code in args[1]
        # Base implementation checks for self.driver == "fdb"
        if isinstance(e, self.dbapi.DatabaseError):
             # We are essentially fdb
             return (e.args[1] in (335546001, 335546003, 335546005)) or \
                    ("Error writing data to the connection" in str(e))
        return super().is_disconnect(e, connection, cursor)

    @classmethod
    def import_dbapi(cls):
        import fdb as sync_fdb

        return AsyncDBAPI(sync_fdb)

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()
