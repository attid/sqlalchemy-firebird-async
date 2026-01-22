from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions

class Requirements(SuiteRequirements):
    @property
    def table_reflection(self):
        return exclusions.open()

    @property
    def primary_key_reflection(self):
        return exclusions.open()

    @property
    def foreign_key_reflection(self):
        return exclusions.open()

    @property
    def temp_table_reflection(self):
        return exclusions.closed()

    @property
    def temporary_tables(self):
        return exclusions.closed()

    @property
    def indices_reflection(self):
        return exclusions.open()

    @property
    def reflects_pk_names(self):
        return exclusions.open()

    @property
    def unique_constraints_reflect_as_index(self):
        return exclusions.open()

    @property
    def unique_constraint_reflection(self):
        return exclusions.open()

    @property
    def datetime_microseconds(self):
        return exclusions.closed()

    @property
    def timestamp_microseconds(self):
        return exclusions.closed()

    @property
    def time_microseconds(self):
        return exclusions.closed()

    @property
    def precision_numerics_enormous_scale(self):
        return exclusions.closed()

    @property
    def views(self):
        return exclusions.open()

    @property
    def schemas(self):
        return exclusions.closed()

    @property
    def sequences(self):
        return exclusions.open()

    @property
    def reflects_generate_rows(self):
        return exclusions.open()

    @property
    def independent_connections(self):
        return exclusions.open()
    
    @property
    def boolean_col_expressions(self):
        return exclusions.open()

    @property
    def nulls_ordering(self):
        return exclusions.open()

    @property
    def update_returning(self):
        return exclusions.closed()

    @property
    def delete_returning(self):
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_w_limit_offset(self):
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        return exclusions.closed()

    @property
    def indexes_with_ascdesc(self):
        return exclusions.closed()

    @property
    def reflect_indexes_with_ascdesc(self):
        return exclusions.closed()

    @property
    def reflect_indexes_with_ascdesc_as_expression(self):
        return exclusions.closed()
