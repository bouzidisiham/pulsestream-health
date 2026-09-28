"""Configuration pytest : fixtures partagées entre tous les tests."""
import pytest
from pulsestream.utils.spark import get_spark_session


@pytest.fixture(scope="session")
def spark():
    """SparkSession partagée pour toute la session de tests.
    """
    session = get_spark_session("PulseStream-Tests")
    yield session
    session.stop()