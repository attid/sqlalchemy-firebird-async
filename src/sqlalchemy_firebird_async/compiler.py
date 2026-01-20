from sqlalchemy_firebird.base import FBCompiler
from sqlalchemy_firebird.base import FBTypeCompiler
from sqlalchemy.sql import elements
from sqlalchemy.sql import expression
from sqlalchemy.sql import operators
from sqlalchemy.sql.compiler import OPERATORS

class PatchedFBTypeCompiler(FBTypeCompiler):
    def _render_string_type(self, type_, name=None, length_override=None, **kwargs):
        if name is None and isinstance(type_, str):
            name = type_
            type_ = None

        if length_override is None and "length" in kwargs:
            length_override = kwargs.pop("length")

        collation = kwargs.pop("collation", None)

        if type_ is None:
            text = name
            if length_override:
                text += f"({length_override})"
            if collation:
                text += f" COLLATE {collation}"
            return text

        # Fix for TypeError: unsupported operand type(s) for +: 'int' and 'str'
        if not isinstance(name, str):
            # Attempt to restore type name from the type object itself
            if hasattr(type_, "__visit_name__"):
                name = type_.__visit_name__.upper()
            else:
                name = "VARCHAR"
        return super()._render_string_type(type_, name, length_override)

    def visit_VARCHAR(self, type_, **kw):
        return self._render_string_type(type_, "VARCHAR", length_override=type_.length)

    def visit_CHAR(self, type_, **kw):
        return self._render_string_type(type_, "CHAR", length_override=type_.length)

    def visit_NVARCHAR(self, type_, **kw):
        return self._render_string_type(type_, "NVARCHAR", length_override=type_.length)

    def visit_NCHAR(self, type_, **kw):
        return self._render_string_type(type_, "NCHAR", length_override=type_.length)

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE PRECISION"

    def visit_DOUBLE_PRECISION(self, type_, **kw):
        return "DOUBLE PRECISION"


class PatchedFBCompiler(FBCompiler):
    def order_by_clause(self, select, **kw):
        if isinstance(select, expression.CompoundSelect):
            return self._compound_order_by_clause(select, **kw)
        return super().order_by_clause(select, **kw)

    def _compound_order_by_clause(self, select, **kw):
        if not select._order_by_clauses:
            return ""

        pos_by_name = {}
        for idx, col in enumerate(select.selected_columns, 1):
            for key in (getattr(col, "key", None), getattr(col, "name", None)):
                if key and key not in pos_by_name:
                    pos_by_name[key] = idx

        clauses = []
        for clause in select._order_by_clauses:
            elem = clause
            direction = None

            if isinstance(elem, elements.UnaryExpression) and elem.modifier in (
                operators.asc_op,
                operators.desc_op,
            ):
                direction = elem.modifier
                elem = elem.element

            if isinstance(elem, elements._label_reference):
                elem = elem.element
                if isinstance(elem, elements.UnaryExpression) and elem.modifier in (
                    operators.asc_op,
                    operators.desc_op,
                ):
                    direction = elem.modifier
                    elem = elem.element

            if isinstance(elem, elements._textual_label_reference):
                key = elem.element
            else:
                key = getattr(elem, "key", None) or getattr(elem, "name", None)

            if key in pos_by_name:
                position = expression.literal_column(str(pos_by_name[key]))
                if direction is operators.desc_op:
                    position = position.desc()
                elif direction is operators.asc_op:
                    position = position.asc()
                clauses.append(position)
                continue

            clauses.append(clause)

        order_by = self._generate_delimited_list(
            clauses, OPERATORS[operators.comma_op], **kw
        )
        if order_by:
            return " ORDER BY " + order_by
        return ""
