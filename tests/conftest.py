import pytest
import logging
from unittest.mock import patch, MagicMock

from koladapy.api import KoladaAPI


def pytest_configure(config):
    """Configure pytest to display logs."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


@pytest.fixture(scope="session")
def sample_kpi_data():
    """Return sample KPI data for testing."""
    return {
        "id": "N00001",
        "title": "Test KPI",
        "description": "This is a sample KPI for testing",
        "is_divided_by_gender": True,
        "municipality_type": "K",
        "auspice": "X",
        "operating_area": "Economy",
        "perspective": "Resources",
        "prel_publication_date": None,
        "publication_date": "2023-01-01",
        "publ_period": "2023",
        "has_ou_data": False
    }


@pytest.fixture(scope="session")
def sample_municipality_data():
    """Return sample municipality data for testing."""
    return {
        "id": "1480",
        "title": "Göteborg",
        "type": "K"
    }


@pytest.fixture(scope="session")
def sample_ou_data():
    """Return sample organizational unit data for testing."""
    return {
        "id": "V11001",
        "title": "Sample School",
        "municipality": "1480"
    }


@pytest.fixture(scope="session")
def sample_data_values():
    """Return sample data values for testing."""
    return [
        {
            "gender": "T",
            "count": 1,
            "status": "OK",
            "value": 42.5,
            "isdeleted": False
        },
        {
            "gender": "M",
            "count": 1,
            "status": "OK",
            "value": 40.2,
            "isdeleted": False
        },
        {
            "gender": "K",
            "count": 1,
            "status": "OK",
            "value": 44.8,
            "isdeleted": False
        }
    ]


@pytest.fixture(scope="session")
def sample_data_response(sample_data_values):
    """Return a sample data response object for testing."""
    return {
        "kpi": "N00001",
        "period": 2022,
        "municipality": "1480",
        "values": sample_data_values
    }


@pytest.fixture(scope="session")
def sample_ou_data_response(sample_data_values):
    """Return a sample organizational unit data response for testing."""
    return {
        "kpi": "N00001",
        "period": 2022,
        "ou": "V11001",
        "values": sample_data_values[:1]  # Just use the first value
    }


@pytest.fixture
def mock_kolada_api():
    """
    Return a fully mocked KoladaAPI instance for testing.
    
    All external API requests will be mocked.
    """
    with patch('koladapy.api.requests.Session'):
        api = KoladaAPI()
        api._make_request = MagicMock()
        api._paginate_request = MagicMock()
        api._batch_request = MagicMock()
        api.get_values = MagicMock()
        api.last_request_time = 0  # Reset the timer
        return api