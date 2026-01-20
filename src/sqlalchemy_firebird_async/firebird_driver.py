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
        self._buffered_rows = None
        self._buffered_index = 0
        self._buffered_description = None

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
        if self._buffered_rows is not None:
            if self._buffered_index >= len(self._buffered_rows):
                return None
            row = self._buffered_rows[self._buffered_index]
            self._buffered_index += 1
            return row
        return self._exec(self._sync_cursor.fetchone)

    def fetchmany(self, size=None):
        if self._buffered_rows is not None:
            if size is None:
                size = getattr(self._sync_cursor, "arraysize", 1)
            start = self._buffered_index
            end = min(start + size, len(self._buffered_rows))
            rows = self._buffered_rows[start:end]
            self._buffered_index = end
            return rows
        if size is None:
            return self._exec(self._sync_cursor.fetchmany)
        return self._exec(self._sync_cursor.fetchmany, size)

    def fetchall(self):
        if self._buffered_rows is not None:
            rows = self._buffered_rows[self._buffered_index :]
            self._buffered_index = len(self._buffered_rows)
            return rows
        return self._exec(self._sync_cursor.fetchall)

    def close(self):
        return self._exec(self._sync_cursor.close)
    
    async def _async_soft_close(self):
        pass
    
    def nextset(self):
        return self._exec(self._sync_cursor.nextset)

    def _set_buffered_rows(self, rows, description):
        self._buffered_rows = list(rows)
        self._buffered_index = 0
        self._buffered_description = description

    @property
    def description(self):
        if self._buffered_description is not None:
            return self._buffered_description
        return self._sync_cursor.description

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


from .compiler import PatchedFBCompiler
from .compiler import PatchedFBTypeCompiler
from sqlalchemy import String
from .types import FBCHARCompat, FBVARCHARCompat, _FBSafeString


class AsyncFirebirdDialect(firebird_sync.FBDialect_firebird):
    name = "firebird.firebird_async"
    driver = "firebird_async"
    is_async = True
    supports_statement_cache = False
    poolclass = AsyncAdaptedQueuePool
    statement_compiler = PatchedFBCompiler
    insert_executemany_returning = True
    insert_executemany_returning_sort_by_parameter_order = True
    
    colspecs = firebird_sync.FBDialect_firebird.colspecs.copy()
    colspecs[String] = _FBSafeString

    ischema_names = firebird_sync.FBDialect_firebird.ischema_names.copy()
    ischema_names["TEXT"] = FBCHARCompat
    ischema_names["VARYING"] = FBVARCHARCompat
    ischema_names["CSTRING"] = FBVARCHARCompat

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type_compiler_instance = PatchedFBTypeCompiler(self)
        self.type_compiler = self.type_compiler_instance

    def initialize(self, connection):
        super().initialize(connection)
        reserved = set(self.preparer.reserved_words)
        reserved.update({"asc", "key"})
        self.preparer.reserved_words = reserved

    def _commit_ddl(self, cursor, context):
        if not context or not getattr(context, "isddl", False):
            return

        conn = getattr(cursor, "connection", None)
        if conn is None:
            return

        if getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None):
            loop = asyncio.get_running_loop()
            await_only(loop.run_in_executor(None, conn.commit))
        else:
            conn.commit()

    def do_execute(self, cursor, statement, parameters, context=None):
        super().do_execute(cursor, statement, parameters, context)
        self._commit_ddl(cursor, context)

    def do_execute_no_params(self, cursor, statement, context=None):
        super().do_execute_no_params(cursor, statement, context)
        self._commit_ddl(cursor, context)

    def do_executemany(self, cursor, statement, parameters, context=None):
        if (
            context
            and getattr(context, "isinsert", False)
            and getattr(context, "compiled", None) is not None
            and getattr(context.compiled, "effective_returning", None)
        ):
            rows = []
            description = None
            for params in parameters:
                super().do_execute(cursor, statement, params, context)
                batch_rows = cursor.fetchall()
                if description is None:
                    description = cursor.description
                if batch_rows:
                    rows.extend(batch_rows)
            if hasattr(cursor, "_set_buffered_rows"):
                cursor._set_buffered_rows(rows, description)
            return
        super().do_executemany(cursor, statement, parameters, context)

    @classmethod
    def import_dbapi(cls):
        return AsyncDBAPI(sync_driver)

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()
