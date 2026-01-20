from sqlalchemy.testing.suite import *
from sqlalchemy.testing import fixtures
from sqlalchemy import testing

# Requirement setup
from tests.requirements import Requirements

# SQLAlchemy testing suite glue
class FirebirdAsyncTest(fixtures.TestBase):
    __backend__ = True

# Load tests (can enable one-by-one at first)
from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import OrderByLabelTest as _OrderByLabelTest

class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
    pass

class OrderByLabelTest(_OrderByLabelTest):
    pass
