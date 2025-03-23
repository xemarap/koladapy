import pytest
import datetime
import json
import pandas as pd
import requests
from unittest.mock import patch, MagicMock, call

from koladapy.api import KoladaAPI
from koladapy.exceptions import KoladaAPIError, KoladaRateLimitError
from koladapy.utils import parse_date, flatten_data, select_and_reorder_columns, get_entity_type

# Sample response data for mocking
SAMPLE_KPI_RESPONSE = {
    "values": [
        {
            "id": "N00001",
            "title": "Sample KPI",
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
    ],
    "next_url": None,
    "previous_url": None,
    "count": 1
}

SAMPLE_MUNICIPALITY_RESPONSE = {
    "values": [
        {
            "id": "1480",
            "title": "Göteborg",
            "type": "K"
        }
    ],
    "next_url": None,
    "previous_url": None,
    "count": 1
}

SAMPLE_DATA_RESPONSE = {
    "values": [
        {
            "kpi": "N00001",
            "period": 2022,
            "municipality": "1480",
            "values": [
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
        }
    ],
    "next_url": None,
    "previous_url": None,
    "count": 1
}

SAMPLE_OU_RESPONSE = {
    "values": [
        {
            "id": "V11001",
            "title": "Sample School",
            "municipality": "1480"
        }
    ],
    "next_url": None,
    "previous_url": None,
    "count": 1
}

SAMPLE_OUDATA_RESPONSE = {
    "values": [
        {
            "kpi": "N00001",
            "period": 2022,
            "ou": "V11001",
            "values": [
                {
                    "gender": "T",
                    "count": 1,
                    "status": "OK",
                    "value": 38.7,
                    "isdeleted": False
                }
            ]
        }
    ],
    "next_url": None,
    "previous_url": None,
    "count": 1
}

# Test fixtures
@pytest.fixture
def kolada_api():
    """Return a KoladaAPI instance with mocked requests."""
    with patch('koladapy.api.requests.Session'):
        api = KoladaAPI()
        api._make_request = MagicMock()
        api._paginate_request = MagicMock()
        api._batch_request = MagicMock()
        api.last_request_time = 0  # Reset the timer
        return api


class TestKoladaAPI:
    """Test the KoladaAPI class."""

    def test_initialization(self):
        """Test that the KoladaAPI class initializes correctly."""
        api = KoladaAPI()
        assert api.base_url == "https://api.kolada.se/v3/"
        assert api.max_batch_size == 25
        assert isinstance(api.session, requests.Session)

        # Test custom initialization
        custom_api = KoladaAPI(
            base_url="https://custom-api.example.com/",
            max_retries=5,
            timeout=60,
            max_requests_per_second=10.0,
            max_batch_size=10
        )
        assert custom_api.base_url == "https://custom-api.example.com/"
        assert custom_api.timeout == 60
        assert custom_api.max_retries == 5
        assert custom_api.min_request_interval == 0.1  # 1/10
        assert custom_api.max_batch_size == 10

    def test_search_kpis(self, kolada_api):
        """Test searching for KPIs."""
        # Setup
        kolada_api._paginate_request.return_value = SAMPLE_KPI_RESPONSE["values"]

        # Execute
        result = kolada_api.search_kpis(query="education")

        # Verify
        kolada_api._paginate_request.assert_called_once_with('kpi', {'title': 'education'})
        assert result == SAMPLE_KPI_RESPONSE["values"]

        # Test with dataframe output
        kolada_api._paginate_request.reset_mock()
        kolada_api._paginate_request.return_value = SAMPLE_KPI_RESPONSE["values"]
        result = kolada_api.search_kpis(as_dataframe=True)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_get_kpi(self, kolada_api):
        """Test getting a specific KPI by ID."""
        # Setup
        kolada_api._make_request.return_value = SAMPLE_KPI_RESPONSE

        # Execute
        result = kolada_api.get_kpi("N00001")

        # Verify
        kolada_api._make_request.assert_called_once_with('kpi/N00001')
        assert result == SAMPLE_KPI_RESPONSE["values"][0]

        # Test error handling when KPI not found
        kolada_api._make_request.return_value = {"values": []}
        with pytest.raises(KoladaAPIError):
            kolada_api.get_kpi("NONEXISTENT")

    def test_get_kpis(self, kolada_api):
        """Test getting multiple KPIs by ID."""
        # Setup
        kolada_api._paginate_request.return_value = SAMPLE_KPI_RESPONSE["values"]

        # Execute - small batch
        result = kolada_api.get_kpis(["N00001", "N00002"])

        # Verify
        kolada_api._paginate_request.assert_called_once_with('kpi/N00001,N00002')
        assert result == SAMPLE_KPI_RESPONSE["values"]

        # Test with large batch
        kolada_api._paginate_request.reset_mock()
        large_batch = [f"N{i:05}" for i in range(1, 30)]  # 29 KPIs, exceeds max_batch_size
        kolada_api.get_kpis(large_batch)
        
        # Should be split into multiple requests
        assert kolada_api._paginate_request.call_count > 1

        # Test with dataframe output
        kolada_api._paginate_request.reset_mock()
        kolada_api._paginate_request.return_value = SAMPLE_KPI_RESPONSE["values"]
        result = kolada_api.get_kpis(["N00001"], as_dataframe=True)
        assert isinstance(result, pd.DataFrame)

    def test_get_municipalities(self, kolada_api):
        """Test getting municipalities."""
        # Setup
        kolada_api._paginate_request.return_value = SAMPLE_MUNICIPALITY_RESPONSE["values"]

        # Execute
        result = kolada_api.get_municipalities(query="Göte", municipality_type="K")

        # Verify
        kolada_api._paginate_request.assert_called_once_with('municipality', {'title': 'Göte', 'type': 'K'})
        assert result == SAMPLE_MUNICIPALITY_RESPONSE["values"]

        # Test with dataframe output
        kolada_api._paginate_request.reset_mock()
        result = kolada_api.get_municipalities(as_dataframe=True)
        assert isinstance(result, pd.DataFrame)

    def test_get_municipality(self, kolada_api):
        """Test getting a specific municipality by ID."""
        # Setup
        kolada_api._make_request.return_value = SAMPLE_MUNICIPALITY_RESPONSE

        # Execute
        result = kolada_api.get_municipality("1480")

        # Verify
        kolada_api._make_request.assert_called_once_with('municipality/1480')
        assert result == SAMPLE_MUNICIPALITY_RESPONSE["values"][0]

        # Test error handling when municipality not found
        kolada_api._make_request.return_value = {"values": []}
        with pytest.raises(KoladaAPIError):
            kolada_api.get_municipality("9999")

    def test_get_organizational_units(self, kolada_api):
        """Test getting organizational units."""
        # Setup
        kolada_api._paginate_request.return_value = SAMPLE_OU_RESPONSE["values"]

        # Execute
        result = kolada_api.get_organizational_units(query="School", municipality="1480")

        # Verify
        kolada_api._paginate_request.assert_called_once_with('ou', {'title': 'School', 'municipality': '1480'})
        assert result == SAMPLE_OU_RESPONSE["values"]

        # Test with dataframe output
        kolada_api._paginate_request.reset_mock()
        result = kolada_api.get_organizational_units(as_dataframe=True)
        assert isinstance(result, pd.DataFrame)

    def test_get_organizational_unit(self, kolada_api):
        """Test getting a specific organizational unit by ID."""
        # Setup
        kolada_api._make_request.return_value = SAMPLE_OU_RESPONSE

        # Execute
        result = kolada_api.get_organizational_unit("V11001")

        # Verify
        kolada_api._make_request.assert_called_once_with('ou/V11001')
        assert result == SAMPLE_OU_RESPONSE["values"][0]

        # Test error handling when OU not found
        kolada_api._make_request.return_value = {"values": []}
        with pytest.raises(KoladaAPIError):
            kolada_api.get_organizational_unit("NONEXISTENT")

    def test_get_values(self, kolada_api):
        """Test getting data values."""
        # Setup
        kolada_api._paginate_request.return_value = SAMPLE_DATA_RESPONSE["values"]

        # Execute - municipality data
        result = kolada_api.get_values(
            kpi_id="N00001",
            municipality_id="1480",
            years=2022
        )

        # Verify
        kolada_api._paginate_request.assert_called_once_with(
            'data/',
            {'kpi_id': ['N00001'], 'municipality_id': ['1480'], 'year': [2022]}
        )
        assert result == SAMPLE_DATA_RESPONSE["values"]

        # Test with OU data
        kolada_api._paginate_request.reset_mock()
        kolada_api._paginate_request.return_value = SAMPLE_OUDATA_RESPONSE["values"]
        result = kolada_api.get_values(
            kpi_id="N00001",
            ou_id="V11001",
            years=2022
        )
        kolada_api._paginate_request.assert_called_once_with(
            'oudata/',
            {'kpi_id': ['N00001'], 'ou_id': ['V11001'], 'year': [2022]}
        )
        assert result == SAMPLE_OUDATA_RESPONSE["values"]

        # Test batching for large parameter lists
        kolada_api._batch_request.reset_mock()
        kolada_api._batch_request.return_value = SAMPLE_DATA_RESPONSE["values"]
        large_kpi_list = [f"N{i:05}" for i in range(1, 30)]  # 29 KPIs, exceeds max_batch_size
        kolada_api.get_values(kpi_id=large_kpi_list, years=2022)
        assert kolada_api._batch_request.called

    def test_get_data_as_dataframe(self, kolada_api):
        """Test getting data as a DataFrame."""
        # Setup - mock the get_values method to return sample data
        with patch.object(kolada_api, 'get_values', return_value=SAMPLE_DATA_RESPONSE["values"]):
            # Execute
            result = kolada_api.get_data_as_dataframe(
                kpi_id="N00001",
                municipality_id="1480",
                years=2022
            )

            # Verify
            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert 'kpi' in result.columns
            assert 'municipality' in result.columns
            assert 'period' in result.columns
            assert 'value' in result.columns

            # Test with metadata
            # For this we need to mock the get_kpis and get_municipalities methods
            with patch.object(kolada_api, 'get_kpis', return_value=pd.DataFrame(SAMPLE_KPI_RESPONSE["values"])), \
                 patch.object(kolada_api, 'get_municipalities', return_value=pd.DataFrame(SAMPLE_MUNICIPALITY_RESPONSE["values"])):
                
                result = kolada_api.get_data_as_dataframe(
                    kpi_id="N00001",
                    municipality_id="1480",
                    years=2022,
                    include_metadata=True
                )
                
                assert isinstance(result, pd.DataFrame)
                assert not result.empty
                # Check metadata columns are included
                if 'kpi_title' in result.columns:
                    assert 'kpi_title' in result.columns

        # Test with OU data
        with patch.object(kolada_api, 'get_values', return_value=SAMPLE_OUDATA_RESPONSE["values"]):
            result = kolada_api.get_data_as_dataframe(
                kpi_id="N00001",
                ou_id="V11001",
                years=2022
            )
            
            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert 'ou' in result.columns

    def test_make_request_retries(self, kolada_api):
        """Test request retry behavior."""
        # Restore the real _make_request method for this test
        real_make_request = KoladaAPI._make_request
        kolada_api._make_request = real_make_request.__get__(kolada_api, KoladaAPI)
        
        # Mock the session.get method to simulate rate limit then success
        mock_response_rate_limit = MagicMock()
        mock_response_rate_limit.status_code = 429
        mock_response_rate_limit.headers = {"Retry-After": "1"}
        mock_response_rate_limit.raise_for_status.side_effect = requests.exceptions.HTTPError()
        
        mock_response_success = MagicMock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {"values": []}
        
        kolada_api.session.get = MagicMock(side_effect=[
            mock_response_rate_limit,  # First call - rate limit
            mock_response_success      # Second call - success
        ])
        
        # Use a monkeypatch to make the backoff retry immediately
        with patch('time.sleep', return_value=None):
            # Execute
            result = kolada_api._make_request('kpi/N00001')
            
            # Verify
            assert kolada_api.session.get.call_count == 2
            assert result == {"values": []}


class TestUtilsFunctions:
    """Test utility functions."""

    def test_parse_date(self):
        """Test parsing date strings and objects."""
        # Test with string
        assert parse_date("2023-01-01") == "2023-01-01"
        
        # Test with date object
        date_obj = datetime.date(2023, 1, 1)
        assert parse_date(date_obj) == "2023-01-01"
        
        # Test error handling
        with pytest.raises(ValueError):
            parse_date("01/01/2023")  # Wrong format
            
        with pytest.raises(TypeError):
            parse_date(20230101)  # Wrong type

    def test_flatten_data(self):
        """Test flattening nested data structures."""
        # Test with municipality data
        flat_data = flatten_data(SAMPLE_DATA_RESPONSE["values"])
        assert len(flat_data) == 3  # Three gender categories
        assert flat_data[0]['kpi'] == "N00001"
        assert flat_data[0]['municipality'] == "1480"
        assert flat_data[0]['period'] == 2022
        assert flat_data[0]['gender'] == "T"
        assert flat_data[0]['value'] == 42.5
        
        # Test with OU data
        flat_data = flatten_data(SAMPLE_OUDATA_RESPONSE["values"])
        assert len(flat_data) == 1
        assert flat_data[0]['kpi'] == "N00001"
        assert flat_data[0]['ou'] == "V11001"
        assert flat_data[0]['period'] == 2022
        assert flat_data[0]['value'] == 38.7

    def test_get_entity_type(self):
        """Test determining entity types from IDs."""
        assert get_entity_type("N00001") == "kpi"
        assert get_entity_type("U12345") == "kpi"
        assert get_entity_type("1480") == "municipality"
        assert get_entity_type("V11001") == "ou"
        assert get_entity_type("INVALID") == "unknown"
        assert get_entity_type("") == "unknown"
        assert get_entity_type(None) == "unknown"

    def test_select_and_reorder_columns(self):
        """Test column selection and reordering for DataFrames."""
        # Test with municipality data
        df = pd.DataFrame({
            'kpi': ['N00001'],
            'period': [2022],
            'municipality': ['1480'],
            'municipality_title': ['Göteborg'],
            'municipality_type': ['K'],
            'gender': ['T'],
            'value': [42.5],
            'kpi_title': ['Sample KPI'],
            'count': [1],
            'status': ['OK']
        })
        
        result = select_and_reorder_columns(df, include_metadata=True)
        # Check all expected columns are present and in correct order
        expected_columns = [
            'kpi', 'period', 'municipality', 'municipality_title',
            'municipality_type', 'gender', 'value', 'kpi_title', 'count', 'status'
        ]
        assert list(result.columns) == expected_columns
        
        # Test with OU data
        df = pd.DataFrame({
            'kpi': ['N00001'],
            'period': [2022],
            'ou': ['V11001'],
            'ou_title': ['Sample School'],
            'ou_municipality': ['1480'],
            'gender': ['T'],
            'value': [38.7],
            'kpi_title': ['Sample KPI']
        })
        
        result = select_and_reorder_columns(df, include_metadata=True)
        # Check the expected columns for OU data
        assert 'ou' in result.columns
        assert 'ou_title' in result.columns
        assert 'ou_municipality' in result.columns
        
        # Test with no metadata
        result = select_and_reorder_columns(df, include_metadata=False)
        assert result.equals(df)  # Should return the original DF unchanged


class TestExceptions:
    """Test exception handling."""

    def test_api_error_hierarchy(self):
        """Test the exception hierarchy."""
        # KoladaRateLimitError should be a subclass of KoladaAPIError
        assert issubclass(KoladaRateLimitError, KoladaAPIError)
        
        # Test creating and raising exceptions
        api_error = KoladaAPIError("General API error")
        rate_limit_error = KoladaRateLimitError("Rate limit exceeded")
        
        assert str(api_error) == "General API error"
        assert str(rate_limit_error) == "Rate limit exceeded"
        
        # Test exception handling
        try:
            raise KoladaRateLimitError("Rate limit test")
        except KoladaAPIError as e:
            assert isinstance(e, KoladaRateLimitError)
            assert str(e) == "Rate limit test"