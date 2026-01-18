from sqlalchemy_firebird.base import FBTypeCompiler

class PatchedFBTypeCompiler(FBTypeCompiler):
    def _render_string_type(self, type_, name, length_override=None):
        # Fix for TypeError: unsupported operand type(s) for +: 'int' and 'str'
        if not isinstance(name, str):
            # Attempt to restore type name from the type object itself
            if hasattr(type_, "__visit_name__"):
                name = type_.__visit_name__.upper()
            else:
                name = "VARCHAR"
        return super()._render_string_type(type_, name, length_override)
