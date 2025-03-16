import pytest
import time
from unittest.mock import patch, MagicMock, call
import requests

from koladapy.api import KoladaAPI
from koladapy.exceptions import KoladaAPIError, KoladaRateLimitError


class TestPagination:
    """Test pagination and batching functionality."""

    @pytest.fixture
    def kolada_api(self):
        """Return a KoladaAPI instance with specific mocks for pagination testing."""
        with patch('koladapy.api.requests.Session'):
            api = KoladaAPI()
            # We'll keep the real _make_request method but mock the session response
            api.session = MagicMock()
            api.last_request_time = 0  # Reset the timer
            return api

    def test_paginate_request_single_page(self, kolada_api):
        """Test pagination when there's only a single page of results."""
        # Setup - mock session response for a single page of results
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "values": [{"id": "item1"}, {"id": "item2"}],
            "next_url": None,
            "previous_url": None,
            "count": 2
        }
        kolada_api.session.get.return_value = mock_response

        # Execute
        with patch('koladapy.api.tqdm') as mock_tqdm:
            result = kolada_api._paginate_request('test-endpoint', {'param': 'value'})

        # Verify
        kolada_api.session.get.assert_called_once()
        assert len(result) == 2
        assert result[0]['id'] == 'item1'
        assert result[1]['id'] == 'item2'

    def test_paginate_request_multiple_pages(self, kolada_api):
        """Test pagination with multiple pages of results."""
        # Setup - mock session responses for three pages
        page1_response = MagicMock()
        page1_response.status_code = 200
        page1_response.json.return_value = {
            "values": [{"id": "page1-item1"}, {"id": "page1-item2"}],
            "next_url": "https://api.kolada.se/v3/test-endpoint?page=2",
            "previous_url": None,
            "count": 6
        }

        page2_response = MagicMock()
        page2_response.status_code = 200
        page2_response.json.return_value = {
            "values": [{"id": "page2-item1"}, {"id": "page2-item2"}],
            "next_url": "https://api.kolada.se/v3/test-endpoint?page=3",
            "previous_url": "https://api.kolada.se/v3/test-endpoint?page=1",
            "count": 6
        }

        page3_response = MagicMock()
        page3_response.status_code = 200
        page3_response.json.return_value = {
            "values": [{"id": "page3-item1"}, {"id": "page3-item2"}],
            "next_url": None,
            "previous_url": "https://api.kolada.se/v3/test-endpoint?page=2",
            "count": 6
        }

        kolada_api.session.get.side_effect = [page1_response, page2_response, page3_response]

        # Execute
        with patch('koladapy.api.tqdm') as mock_tqdm:
            mock_pbar = MagicMock()
            mock_tqdm.return_value.__enter__.return_value = mock_pbar
            result = kolada_api._paginate_request('test-endpoint', {'param': 'value'})

        # Verify
        assert kolada_api.session.get.call_count == 3
        assert len(result) == 6
        
        # Check the items are combined in the correct order
        assert result[0]['id'] == 'page1-item1'
        assert result[1]['id'] == 'page1-item2'
        assert result[2]['id'] == 'page2-item1'
        assert result[3]['id'] == 'page2-item2'
        assert result[4]['id'] == 'page3-item1'
        assert result[5]['id'] == 'page3-item2'
        
        # Verify progress bar updates
        assert mock_pbar.update.call_count == 3
        mock_pbar.update.assert_has_calls([call(2), call(2), call(2)])

    def test_paginate_request_empty_response(self, kolada_api):
        """Test pagination with an empty response."""
        # Setup - mock session response with no items
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "values": [],
            "next_url": None,
            "previous_url": None,
            "count": 0
        }
        kolada_api.session.get.return_value = mock_response

        # Execute
        with patch('koladapy.api.tqdm') as mock_tqdm:
            result = kolada_api._paginate_request('test-endpoint')

        # Verify
        kolada_api.session.get.assert_called_once()
        assert len(result) == 0

    def test_paginate_request_error_handling(self, kolada_api):
        """Test error handling in pagination."""
        # Setup - mock session response to simulate an error
        mock_response = MagicMock()
        mock_response.status_code = 404
        # Use a requests.HTTPError instead of a generic Exception
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Not found")
        kolada_api.session.get.return_value = mock_response

        # Execute and verify
        with patch('koladapy.api.tqdm'), pytest.raises(KoladaAPIError):
            kolada_api._paginate_request('test-endpoint')

    def test_throttling_mechanism(self, kolada_api):
        """Test that the throttling mechanism works correctly."""
        # Setup - prepare for request throttling test
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "values": [{"id": "item1"}],
            "next_url": None,
            "previous_url": None,
            "count": 1
        }
        kolada_api.session.get.return_value = mock_response
        
        # Set max_requests_per_second to a low value for testing
        kolada_api.min_request_interval = 0.5  # 2 requests per second

        # Execute multiple requests and time them
        with patch('koladapy.api.tqdm'):
            start_time = time.time()
            kolada_api._paginate_request('test-endpoint')
            kolada_api._paginate_request('test-endpoint')
            elapsed = time.time() - start_time

        # Verify that throttling occurred (elapsed time should be at least min_request_interval)
        assert elapsed >= kolada_api.min_request_interval


class TestBatching:
    """Test batching functionality for large parameter lists."""

    @pytest.fixture
    def kolada_api(self):
        """Return a KoladaAPI instance with specific mocks for batching testing."""
        with patch('koladapy.api.requests.Session'):
            api = KoladaAPI(max_batch_size=3)  # Small batch size for testing
            api._paginate_request = MagicMock()
            api.last_request_time = 0  # Reset the timer
            return api

    def test_batch_request_no_batching_needed(self, kolada_api):
        """Test when no batching is needed (small parameter lists)."""
        # Setup
        params = {'kpi_id': ['N00001', 'N00002'], 'year': [2022]}
        kolada_api._paginate_request.return_value = [{'id': 'result1'}]

        # Execute
        result = kolada_api._batch_request('test-endpoint', params, ['kpi_id', 'year'])

        # Verify - use assert_called_once without checking the params
        assert kolada_api._paginate_request.call_count == 1
        # And check that the correct result is returned
        assert result == [{'id': 'result1'}]
    
        # If we need to check the params in more detail, we can do:
        args, kwargs = kolada_api._paginate_request.call_args
        assert args[0] == 'test-endpoint'
        assert 'kpi_id' in args[1]
        assert 'year' in args[1]
        assert args[1]['kpi_id'] == ['N00001', 'N00002']
        assert args[1]['year'] == [2022]

    def test_batch_request_single_parameter_batching(self, kolada_api):
        """Test batching a single parameter that exceeds the max batch size."""
        # Setup - kpi_id list that exceeds max_batch_size
        kpi_ids = ['N00001', 'N00002', 'N00003', 'N00004', 'N00005']
        params = {'kpi_id': kpi_ids, 'year': [2022]}
        
        # Mock paginate_request to return different results for each batch
        batch1_results = [{'id': 'batch1-item1'}, {'id': 'batch1-item2'}]
        batch2_results = [{'id': 'batch2-item1'}, {'id': 'batch2-item2'}]
        kolada_api._paginate_request.side_effect = [batch1_results, batch2_results]

        # Execute
        with patch('koladapy.api.tqdm') as mock_tqdm:
            mock_pbar = MagicMock()
            mock_tqdm.return_value.__enter__.return_value = mock_pbar
            result = kolada_api._batch_request('test-endpoint', params, ['kpi_id'])

        # Verify
        assert kolada_api._paginate_request.call_count == 2
        # Should be called with first batch [N00001, N00002, N00003], then second batch [N00004, N00005]
        kolada_api._paginate_request.assert_has_calls([
            call('test-endpoint', {'kpi_id': ['N00001', 'N00002', 'N00003'], 'year': [2022]}),
            call('test-endpoint', {'kpi_id': ['N00004', 'N00005'], 'year': [2022]})
        ])
        
        # Results should be combined
        assert len(result) == 4
        assert result[0]['id'] == 'batch1-item1'
        assert result[2]['id'] == 'batch2-item1'

    def test_batch_request_multiple_parameter_batching(self, kolada_api):
        """Test batching multiple parameters that exceed the max batch size."""
        # Setup - multiple parameters that need batching
        params = {
            'kpi_id': ['N00001', 'N00002', 'N00003', 'N00004'],
            'municipality_id': ['1480', '1481', '1482', '1483'],
            'year': [2020, 2021, 2022]  # Within batch size limit
        }
        
        # With batch size 3, we'd expect 2 batches for kpi_id and 2 for municipality_id,
        # resulting in 4 total batch combinations
        batch_results = [
            [{'id': 'batch1-item1'}],  # Batch 1: kpi_id[0:3], municipality_id[0:3]
            [{'id': 'batch2-item1'}],  # Batch 2: kpi_id[0:3], municipality_id[3:4]
            [{'id': 'batch3-item1'}],  # Batch 3: kpi_id[3:4], municipality_id[0:3]
            [{'id': 'batch4-item1'}]   # Batch 4: kpi_id[3:4], municipality_id[3:4]
        ]
        kolada_api._paginate_request.side_effect = batch_results

        # Execute
        with patch('koladapy.api.tqdm') as mock_tqdm:
            mock_pbar = MagicMock()
            mock_tqdm.return_value.__enter__.return_value = mock_pbar
            result = kolada_api._batch_request('test-endpoint', params, ['kpi_id', 'municipality_id'])

        # Verify
        assert kolada_api._paginate_request.call_count == 4
        # Check each batch request had correct parameters
        # First batch should have first 3 KPIs and first 3 municipalities
        # Second batch should have first 3 KPIs and 4th municipality
        # Third batch should have 4th KPI and first 3 municipalities
        # Fourth batch should have 4th KPI and 4th municipality
        kolada_api._paginate_request.assert_has_calls([
            call('test-endpoint', {
                'kpi_id': ['N00001', 'N00002', 'N00003'], 
                'municipality_id': ['1480', '1481', '1482'],
                'year': [2020, 2021, 2022]
            }),
            call('test-endpoint', {
                'kpi_id': ['N00001', 'N00002', 'N00003'], 
                'municipality_id': ['1483'],
                'year': [2020, 2021, 2022]
            }),
            call('test-endpoint', {
                'kpi_id': ['N00004'], 
                'municipality_id': ['1480', '1481', '1482'],
                'year': [2020, 2021, 2022]
            }),
            call('test-endpoint', {
                'kpi_id': ['N00004'], 
                'municipality_id': ['1483'],
                'year': [2020, 2021, 2022]
            })
        ], any_order=True)
        
        # Results should be combined from all batches
        assert len(result) == 4