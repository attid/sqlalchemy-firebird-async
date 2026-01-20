from sqlalchemy.testing.suite import *
from sqlalchemy.testing import fixtures
from sqlalchemy import testing

# Настройка требований
from tests.requirements import Requirements

# Магия SQLAlchemy testing suite
class FirebirdAsyncTest(fixtures.TestBase):
    __backend__ = True

# Подгружаем тесты (можно по одному для начала)
from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import OrderByLabelTest as _OrderByLabelTest

class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
    pass

class OrderByLabelTest(_OrderByLabelTest):
    pass
