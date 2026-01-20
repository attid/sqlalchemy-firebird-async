from sqlalchemy import CHAR, String, VARCHAR
from sqlalchemy_firebird.types import FBCHAR, FBVARCHAR, _FBString

class _FBSafeString(_FBString):
    def __init__(self, length=None, charset=None, collation=None, **kwargs):
        # Consume _enums if present (passed by SQLAlchemy Enum)
        self._enums = kwargs.pop("_enums", None)
        
        # Pass remaining arguments to super (which might not accept kwargs, so be careful)
        # _FBString.__init__ takes (length=None, charset=None, collation=None)
        super().__init__(length=length, charset=charset, collation=collation)

    def bind_processor(self, dialect):
        super_proc = super().bind_processor(dialect)
        
        if not self._enums:
            return super_proc

        def process(value):
            if value is None:
                return None
            if hasattr(value, "value"):
                # Handle Enum objects (extract value)
                value = value.value
            
            if super_proc:
                return super_proc(value)
            return value
        
        return process

    def result_processor(self, dialect, coltype):
        super_proc = super().result_processor(dialect, coltype)
        
        if not self._enums:
            return super_proc
        
        # _enums is the tuple of arguments passed to Enum constructor.
        # If initialized with an Enum class (e.g. Enum(MyEnum)), it will be (MyEnum,).
        enum_class = None
        if self._enums and len(self._enums) == 1 and isinstance(self._enums[0], type):
             enum_class = self._enums[0]
        
        if not enum_class:
            return super_proc

        def process(value):
            if super_proc:
                value = super_proc(value)
            
            if value is None:
                return None
            
            # Convert string back to Enum
            try:
                return enum_class(value)
            except ValueError:
                return value
        
        return process


class FBCHARCompat(FBCHAR, CHAR):
    def __init__(self, length=None, charset=None, collation=None):
        super().__init__(length=length, charset=charset, collation=collation)


class FBVARCHARCompat(FBVARCHAR, VARCHAR):
    def __init__(self, length=None, charset=None, collation=None):
        super().__init__(length=length, charset=charset, collation=collation)