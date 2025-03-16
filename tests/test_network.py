import pytest
import responses
import requests
import json
from urllib.parse import parse_qs, urlparse
import re
from unittest.mock import patch, MagicMock

from koladapy.api import KoladaAPI
from koladapy.exceptions import KoladaAPIError, KoladaRateLimitError


class TestNetworkInteractions:
    """Test actual network interactions with the Kolada API."""

    @pytest.fixture
    def base_url(self):
        """Return the base URL for the Kolada API."""
        return "https://api.kolada.se/v3/"

    @pytest.fixture
    def kolada_api(self):
        """Return a KoladaAPI instance for testing."""
        return KoladaAPI()

    @responses.activate
    def test_make_request_success(self, base_url, kolada_api):
        """Test successful API request."""
        # Mock a successful API response
        responses.add(
            responses.GET,
            f"{base_url}kpi/N00001",
            json={"values": [{"id": "N00001", "title": "Test KPI"}], "count": 1},
            status=200
        )

        # Execute
        response = kolada_api._make_request("kpi/N00001")

        # Verify
        assert responses.calls[0].request.url == f"{base_url}kpi/N00001"
        assert response["values"][0]["id"] == "N00001"
        assert response["values"][0]["title"] == "Test KPI"

    @responses.activate
    def test_make_request_rate_limit(self, base_url, kolada_api):
        """Test rate limit handling."""
        # Mock a rate limit response followed by a success
        responses.add(
            responses.GET,
            f"{base_url}kpi/N00001",
            status=429,
            headers={"Retry-After": "1"}
        )
        responses.add(
            responses.GET,
            f"{base_url}kpi/N00001",
            json={"values": [{"id": "N00001"}], "count": 1},
            status=200
        )

        # Mock time.sleep to avoid actual waiting
        with patch("time.sleep", return_value=None):
            # With backoff, it should retry and eventually succeed
            result = kolada_api._make_request("kpi/N00001")
            
            # Verify the result is from the success response
            assert "values" in result
            assert result["values"][0]["id"] == "N00001"
            assert len(responses.calls) == 2  # Should have made two calls

    @responses.activate
    def test_make_request_http_error(self, base_url, kolada_api):
        """Test handling of HTTP errors."""
        # Mock a 404 error response
        responses.add(
            responses.GET,
            f"{base_url}kpi/NONEXISTENT",
            json={"error": "Not found"},
            status=404
        )

        # Execute and verify
        with pytest.raises(KoladaAPIError):
            kolada_api._make_request("kpi/NONEXISTENT")

    @responses.activate
    def test_paginate_request_pagination(self, base_url, kolada_api):
        """Test pagination through multiple pages of results."""
        # Mock first page - without using matchers which are causing problems
        responses.add(
            responses.GET,
            f"{base_url}kpi?page=1&per_page=2",
            json={
                "values": [{"id": "N00001"}, {"id": "N00002"}],
                "next_url": f"{base_url}kpi?page=2&per_page=2",
                "previous_url": None,
                "count": 4
            },
            status=200
        )

        # Mock second page
        responses.add(
            responses.GET,
            f"{base_url}kpi?page=2&per_page=2",
            json={
                "values": [{"id": "N00003"}, {"id": "N00004"}],
                "next_url": None,
                "previous_url": f"{base_url}kpi?page=1&per_page=2",
                "count": 4
            },
            status=200
        )

        # Mock tqdm to avoid progress bar output
        with patch("koladapy.api.tqdm", return_value=DummyContextManager()):
            
            # Execute
            result = kolada_api._paginate_request("kpi", {"per_page": 2})

        # Verify
        assert len(result) == 4
        assert result[0]["id"] == "N00001"
        assert result[1]["id"] == "N00002"
        assert result[2]["id"] == "N00003"
        assert result[3]["id"] == "N00004"
        assert len(responses.calls) == 2

        # Verify
        assert len(result) == 4
        assert result[0]["id"] == "N00001"
        assert result[1]["id"] == "N00002"
        assert result[2]["id"] == "N00003"
        assert result[3]["id"] == "N00004"
        assert len(responses.calls) == 2

    @responses.activate
    def test_search_kpis_with_query(self, base_url, kolada_api):
        """Test searching for KPIs with a query parameter."""
        # Mock the API response
        responses.add(
            responses.GET,
            re.compile(f"{base_url}kpi.*"),
            json={
                "values": [
                    {"id": "N00001", "title": "Education Test"},
                    {"id": "N00002", "title": "Test Education"}
                ],
                "next_url": None,
                "previous_url": None,
                "count": 2
            },
            status=200
        )

        # Mock tqdm to avoid progress bar output
        with patch("koladapy.api.tqdm", return_value=DummyContextManager()):
            
            # Execute
            result = kolada_api.search_kpis(query="education")

        # Verify
        assert len(result) == 2
        assert result[0]["id"] == "N00001"
        assert result[1]["id"] == "N00002"
        
        # Verify the request URL contains the right parameters
        request_url = responses.calls[0].request.url
        query_params = parse_qs(urlparse(request_url).query)
        assert "title" in query_params
        assert query_params["title"][0] == "education"

    @responses.activate
    def test_get_data_as_dataframe(self, base_url, kolada_api):
        """Test getting data as a DataFrame with actual network requests."""
        # Mock the main data request
        responses.add(
            responses.GET,
            re.compile(f"{base_url}data/.*"),
            json={
                "values": [
                    {
                        "kpi": "N00001",
                        "municipality": "1480",
                        "period": 2022,
                        "values": [
                            {
                                "gender": "T",
                                "value": 42.5,
                                "count": 1,
                                "status": "OK",
                                "isdeleted": False
                            }
                        ]
                    }
                ],
                "next_url": None,
                "previous_url": None,
                "count": 1
            },
            status=200
        )

        # Mock the KPI metadata request for the include_metadata option
        responses.add(
            responses.GET,
            re.compile(f"{base_url}kpi/N00001"),
            json={
                "values": [
                    {
                        "id": "N00001",
                        "title": "Test KPI",
                        "description": "Test Description",
                        "is_divided_by_gender": True,
                        "municipality_type": "K",
                        "auspice": "X",
                        "operating_area": "Test Area",
                        "perspective": "Test Perspective",
                        "prel_publication_date": None,
                        "publication_date": "2023-01-01",
                        "publ_period": "2023",
                        "has_ou_data": False
                    }
                ],
                "next_url": None,
                "previous_url": None,
                "count": 1
            },
            status=200
        )

        # Mock the municipality metadata request
        responses.add(
            responses.GET,
            re.compile(f"{base_url}municipality.*"),
            json={
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
            },
            status=200
        )

        # Mock tqdm to avoid progress bar output
        with patch("koladapy.api.tqdm", return_value=DummyContextManager()):
            
            # Execute - first without metadata
            df = kolada_api.get_data_as_dataframe(
                kpi_id="N00001",
                municipality_id="1480",
                years=2022
            )
            
            # Then with metadata
            df_with_metadata = kolada_api.get_data_as_dataframe(
                kpi_id="N00001",
                municipality_id="1480",
                years=2022,
                include_metadata=True
            )

        # Verify basic dataframe
        assert not df.empty
        assert "kpi" in df.columns
        assert "municipality" in df.columns
        assert "value" in df.columns
        assert df.iloc[0]["kpi"] == "N00001"
        assert df.iloc[0]["municipality"] == "1480"
        assert df.iloc[0]["value"] == 42.5
        
        # Verify dataframe with metadata
        assert not df_with_metadata.empty
        assert "kpi_title" in df_with_metadata.columns
        assert "municipality_title" in df_with_metadata.columns
        assert df_with_metadata.iloc[0]["kpi_title"] == "Test KPI"
        assert df_with_metadata.iloc[0]["municipality_title"] == "Göteborg"


# Utility classes
class DummyContextManager:
    """A dummy context manager for mocking tqdm."""
    def __enter__(self):
        return DummyProgressBar()
    
    def __exit__(self, *args):
        pass


class DummyProgressBar:
    """A dummy progress bar for mocking tqdm."""
    def update(self, n=1):
        pass