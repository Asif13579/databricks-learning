import pytest
from src.utilities import get_spark

@pytest.fixture(scope="session")
def spark():
    session=get_spark()
    yield session
    session.stop()

