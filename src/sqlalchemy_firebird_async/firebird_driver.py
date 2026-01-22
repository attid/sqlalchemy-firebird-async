import asyncio
import sys
import os
from functools import partial
from sqlalchemy.util.concurrency import await_only
from greenlet import getcurrent
from sqlalchemy.pool import AsyncAdaptedQueuePool
import sqlalchemy_firebird.firebird as firebird_sync
import firebird.driver as sync_driver

# Global list to hold references to objects during shutdown to prevent Segfaults
_zombies = []

class AsyncCursor:
    def __init__(self, sync_cursor, loop):
        self._sync_cursor = sync_cursor
        self._loop = loop
        self._buffered_rows = None
        self._buffered_index = 0
        self._buffered_description = None
        self._rowcount = -1

    def _exec(self, func, *args, **kwargs):
        if getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None):
            return await_only(self._loop.run_in_executor(None, partial(func, *args, **kwargs)))
        else:
            return func(*args, **kwargs)

    def execute(self, operation, parameters=None):
        try:
            if parameters is None:
                return self._exec(self._sync_cursor.execute, operation)
            else:
                return self._exec(self._sync_cursor.execute, operation, parameters)
        finally:
            if self._sync_cursor is not None:
                # DBAPI compliance: rowcount should be -1 for SELECT statements
                # firebird-driver returns 0 until rows are fetched.
                # SQLAlchemy expects -1 if it's not a DML statement.
                op_upper = operation.strip().upper()
                if op_upper.startswith("SELECT") or op_upper.startswith("WITH"):
                    self._rowcount = -1
                else:
                    self._rowcount = self._sync_cursor.rowcount

    def executemany(self, operation, seq_of_parameters):
        try:
            return self._exec(self._sync_cursor.executemany, operation, seq_of_parameters)
        finally:
             if self._sync_cursor is not None:
                # Firebird driver rowcount behavior for executemany is unreliable (e.g. returns 1)
                # so we force -1 (undefined) which is DBAPI compliant and accepted by SQLAlchemy
                self._rowcount = -1

    def fetchone(self):
        if self._buffered_rows is not None:
            if self._buffered_index >= len(self._buffered_rows):
                return None
            row = self._buffered_rows[self._buffered_index]
            self._buffered_index += 1
            return row
        result = self._exec(self._sync_cursor.fetchone)
        return result

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

    def close(self, sys=sys, zombies=_zombies):
        if self._sync_cursor is None:
            return
        
        is_finalizing = False
        try:
            if sys is None or sys.is_finalizing():
                is_finalizing = True
        except Exception:
            is_finalizing = True

        if is_finalizing:
            if zombies is not None:
                zombies.append(self._sync_cursor)
            self._sync_cursor = None
            return

        # Only attempt close if we are in a greenlet context AND loop is running.
        if getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None) and not self._loop.is_closed():
            try:
                self._exec(self._sync_cursor.close)
                self._sync_cursor = None
                return
            except Exception:
                pass
        
        # Fallback: zombie
        if zombies is not None:
            zombies.append(self._sync_cursor)
        self._sync_cursor = None

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

    @property
    def rowcount(self):
        return self._rowcount

    def __getattr__(self, name):
        return getattr(self._sync_cursor, name)

    def __del__(self):
        self.close()


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

    def close(self, sys=sys, zombies=_zombies):
        if self._sync_connection is None:
            return

        is_finalizing = False
        try:
            if sys is None or sys.is_finalizing():
                is_finalizing = True
        except Exception:
            is_finalizing = True

        if is_finalizing:
            if zombies is not None:
                zombies.append(self._sync_connection)
            self._sync_connection = None
            return

        # Only attempt close if we are in a greenlet context AND loop is running.
        can_exec = getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None) and not self._loop.is_closed()
        
        if can_exec:
            try:
                self._exec(self._sync_connection.close)
                self._sync_connection = None
                return
            except Exception as e:
                pass
        
        # Fallback: zombie
        if zombies is not None:
            zombies.append(self._sync_connection)
        self._sync_connection = None

    def terminate(self):
        self.close()

    def __getattr__(self, name):
        return getattr(self._sync_connection, name)

    def __del__(self):
        self.close()


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
        loop = asyncio.get_running_loop()
        
        if getattr(getcurrent(), "__sqlalchemy_greenlet_provider__", None):
            sync_conn = await_only(loop.run_in_executor(None, partial(self._sync_dbapi.connect, *args, **kwargs)))
        else:
            sync_conn = self._sync_dbapi.connect(*args, **kwargs)
            
        return AsyncConnection(sync_conn, loop)


from .compiler import PatchedFBCompiler, PatchedFBDDLCompiler
from .compiler import PatchedFBTypeCompiler
from sqlalchemy import String, DateTime, Time, TIMESTAMP, VARCHAR, CHAR
from .types import FBCHARCompat, FBVARCHARCompat, _FBSafeString, FBDateTime, FBTime, FBTimestamp


class AsyncFirebirdDialect(firebird_sync.FBDialect_firebird):
    name = "firebird.firebird_async"
    driver = "firebird_async"
    is_async = True
    supports_statement_cache = False
    poolclass = AsyncAdaptedQueuePool
    statement_compiler = PatchedFBCompiler
    ddl_compiler = PatchedFBDDLCompiler
    insert_returning = True
    insert_executemany_returning = True
    insert_executemany_returning_sort_by_parameter_order = True
    
    colspecs = firebird_sync.FBDialect_firebird.colspecs.copy()
    colspecs[String] = _FBSafeString
    colspecs[VARCHAR] = _FBSafeString
    colspecs[CHAR] = _FBSafeString
    colspecs[DateTime] = FBDateTime
    colspecs[Time] = FBTime
    colspecs[TIMESTAMP] = FBTimestamp

    ischema_names = firebird_sync.FBDialect_firebird.ischema_names.copy()
    ischema_names["TEXT"] = FBCHARCompat
    ischema_names["VARYING"] = FBVARCHARCompat
    ischema_names["CSTRING"] = FBVARCHARCompat

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type_compiler_instance = PatchedFBTypeCompiler(self)
        self.type_compiler = self.type_compiler_instance
        self.postfetch_lastrowid = False
        self.server_version_info = (4, 0, 0)
        self.supports_identity_columns = True

    def initialize(self, connection):
        super().initialize(connection)
        # Force flags to ensure correct behavior with async driver
        self.server_version_info = (4, 0, 0) # Assume modern Firebird
        self.postfetch_lastrowid = False
        self.preexecute_autoincrement_sequences = False
        self.supports_identity_columns = True

        reserved = set(self.preparer.reserved_words)
        reserved.update({"asc", "key"})
        self.preparer.reserved_words = reserved

    def is_disconnect(self, e, connection, cursor):
        return super().is_disconnect(e, connection, cursor)

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
        try:
            super().do_execute(cursor, statement, parameters, context)
            self._commit_ddl(cursor, context)
        except Exception as e:
            # Force integrity error translation here as it seems _handle_dbapi_exception hook is failing
            from sqlalchemy import exc
            msg = str(e).lower()
            if "violation" in msg and ("primary" in msg or "unique" in msg or "foreign" in msg or "constraint" in msg):
                 raise exc.IntegrityError(statement, parameters, e) from e
            raise

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
            total_rowcount = 0
            for params in parameters:
                super().do_execute(cursor, statement, params, context)
                if hasattr(cursor, "rowcount") and cursor.rowcount > 0:
                    total_rowcount += cursor.rowcount
                batch_rows = cursor.fetchall()
                if description is None:
                    description = cursor.description
                if batch_rows:
                    rows.extend(batch_rows)
            if hasattr(cursor, "_set_buffered_rows"):
                cursor._set_buffered_rows(rows, description)
            if hasattr(cursor, "_rowcount"):
                cursor._rowcount = total_rowcount
            return
        
        super().do_executemany(cursor, statement, parameters, context)

    @classmethod
    def import_dbapi(cls):
        return AsyncDBAPI(sync_driver)

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()