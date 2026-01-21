import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from sqlalchemy.util.concurrency import await_only
from greenlet import getcurrent


class AsyncCursor:
    def __init__(self, sync_cursor, loop, executor=None):
        self._sync_cursor = sync_cursor
        self._loop = loop
        self._executor = executor

    def _exec(self, func, *args, **kwargs):
        # Check whether we are in a SQLAlchemy-created greenlet context.
        in_greenlet = getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None)
        if self._executor is not None:
            if in_greenlet:
                return await_only(
                    self._loop.run_in_executor(
                        self._executor, partial(func, *args, **kwargs)
                    )
                )
            return self._executor.submit(func, *args, **kwargs).result()
        if in_greenlet:
            return await_only(
                self._loop.run_in_executor(None, partial(func, *args, **kwargs))
            )
        # If not, call synchronously (e.g. inside run_sync).
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
    def __init__(self, sync_connection, loop, executor=None):
        self._sync_connection = sync_connection
        self._loop = loop
        self._executor = executor

    def _exec(self, func, *args, **kwargs):
        in_greenlet = getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None)
        if self._executor is not None:
            if in_greenlet:
                return await_only(
                    self._loop.run_in_executor(
                        self._executor, partial(func, *args, **kwargs)
                    )
                )
            return self._executor.submit(func, *args, **kwargs).result()
        if in_greenlet:
            return await_only(
                self._loop.run_in_executor(None, partial(func, *args, **kwargs))
            )
        return func(*args, **kwargs)

    def cursor(self):
        sync_cursor = self._exec(self._sync_connection.cursor)
        return AsyncCursor(sync_cursor, self._loop, self._executor)

    def begin(self):
        return self._exec(self._sync_connection.begin)

    def commit(self):
        return self._exec(self._sync_connection.commit)

    def rollback(self):
        return self._exec(self._sync_connection.rollback)

    def close(self):
        try:
            return self._exec(self._sync_connection.close)
        finally:
            if self._executor is not None:
                self._executor.shutdown(wait=False)
                self._executor = None
    
    def terminate(self):
        return self.close()

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
        executor = None
        
        def _connect():
            if async_creator_fn is not None:
                # We cannot call await_only directly if the creator is async.
                # But fdb is synchronous, so it is fine.
                # If async_creator is provided, it is for firebirdsql, but we are in fdb.py.
                return async_creator_fn(*args, **kwargs) # Returns a coroutine? No, this is a callback.
            else:
                return self._sync_dbapi.connect(*args, **kwargs)

        if getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None):
            # If async_creator_fn returns a coroutine, await_only will wait for it.
            # But for fdb this is sync, so use run_in_executor.
             executor = ThreadPoolExecutor(max_workers=1)
             try:
                 sync_conn = await_only(loop.run_in_executor(executor, _connect))
             except Exception:
                 executor.shutdown(wait=False)
                 raise
        else:
             sync_conn = _connect()
            
        return AsyncConnection(sync_conn, loop, executor)

from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy_firebird.base import FBExecutionContext
import sqlalchemy_firebird.fdb as fdb
from .compiler import PatchedFBCompiler, PatchedFBDDLCompiler, PatchedFBTypeCompiler
from .types import FBCHARCompat, FBVARCHARCompat
from sqlalchemy import String, DateTime, Time, TIMESTAMP, VARCHAR, CHAR
from .types import _FBSafeString, FBDateTime, FBTime, FBTimestamp


class AsyncFDBExecutionContext(FBExecutionContext):
    def post_exec(self):
        super().post_exec()
        if self.isddl:
            # Firebird with fdb requires a new transaction to see DDL changes.
            dbapi_conn = self._dbapi_connection
            driver_conn = getattr(dbapi_conn, "driver_connection", None)
            if driver_conn is None:
                driver_conn = getattr(dbapi_conn, "dbapi_connection", dbapi_conn)
            try:
                self.cursor.close()
            except Exception:
                pass
            driver_conn.commit()
            driver_conn.begin()


class AsyncFDBDialect(fdb.FBDialect_fdb):
    name = "firebird.fdb_async"
    driver = "fdb_async"
    is_async = True
    supports_statement_cache = False
    poolclass = AsyncAdaptedQueuePool
    statement_compiler = PatchedFBCompiler
    ddl_compiler = PatchedFBDDLCompiler
    execution_ctx_cls = AsyncFDBExecutionContext
    ischema_names = fdb.FBDialect_fdb.ischema_names.copy()
    ischema_names["TEXT"] = FBCHARCompat
    ischema_names["VARYING"] = FBVARCHARCompat
    ischema_names["CSTRING"] = FBVARCHARCompat
    
    colspecs = fdb.FBDialect_fdb.colspecs.copy()
    colspecs[String] = _FBSafeString
    colspecs[VARCHAR] = _FBSafeString
    colspecs[CHAR] = _FBSafeString
    colspecs[DateTime] = FBDateTime
    colspecs[Time] = FBTime
    colspecs[TIMESTAMP] = FBTimestamp

    # Explicitly set type compiler to ensure our patch is used
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type_compiler_instance = PatchedFBTypeCompiler(self)
        self.type_compiler = self.type_compiler_instance

    def dbapi_exception_translation(self, exception, statement, parameters, context):
        from sqlalchemy import exc
        
        msg = str(exception).lower()
        if "violation" in msg and ("primary" in msg or "unique" in msg or "foreign" in msg or "constraint" in msg):
             return exc.IntegrityError(statement, parameters, exception)
             
        return super().dbapi_exception_translation(exception, statement, parameters, context)

    def wrap_dbapi_exception(self, e, statement, parameters, cursor, context):
        from sqlalchemy import exc
        
        msg = str(e).lower()
        if "violation" in msg and ("primary" in msg or "unique" in msg or "foreign" in msg or "constraint" in msg):
             return exc.IntegrityError(statement, parameters, e)
             
        return super().wrap_dbapi_exception(e, statement, parameters, cursor, context)

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
