import pytest
import datetime
import pandas as pd
import numpy as np

from koladapy.utils import (
    parse_date, 
    flatten_data, 
    group_data_by_period, 
    get_entity_type,
    select_and_reorder_columns
)


class TestParseDate:
    """Test the parse_date function."""

    def test_parse_date_string(self):
        """Test parsing date from a string."""
        assert parse_date("2023-01-15") == "2023-01-15"
        assert parse_date("2023-12-31") == "2023-12-31"

    def test_parse_date_object(self):
        """Test parsing date from a date object."""
        date_obj = datetime.date(2023, 1, 15)
        assert parse_date(date_obj) == "2023-01-15"
        
        # Test with datetime object
        datetime_obj = datetime.datetime(2023, 1, 15, 12, 30, 0)
        assert parse_date(datetime_obj) == "2023-01-15"

    def test_parse_date_invalid_format(self):
        """Test parsing date with invalid format."""
        with pytest.raises(ValueError):
            parse_date("01/15/2023")  # MM/DD/YYYY format
        
        with pytest.raises(ValueError):
            parse_date("15-01-2023")  # DD-MM-YYYY format
            
        with pytest.raises(ValueError):
            parse_date("2023.01.15")  # YYYY.MM.DD format

    def test_parse_date_invalid_type(self):
        """Test parsing date with invalid type."""
        with pytest.raises(TypeError):
            parse_date(20230115)  # Integer
            
        with pytest.raises(TypeError):
            parse_date(None)  # None
            
        with pytest.raises(TypeError):
            parse_date(["2023-01-15"])  # List


class TestFlattenData:
    """Test the flatten_data function."""

    def test_flatten_municipality_data(self, sample_data_response):
        """Test flattening municipality data."""
        # Create a list with a single data response
        data = [sample_data_response]
        
        # Flatten the data
        flattened = flatten_data(data)
        
        # Check the result
        assert len(flattened) == 3  # 3 gender categories in sample data
        
        # Check the common fields
        for item in flattened:
            assert item["kpi"] == "N00001"
            assert item["municipality"] == "1480"
            assert item["period"] == 2022
            
        # Check the gender-specific values
        gender_values = {item["gender"]: item["value"] for item in flattened}
        assert gender_values["T"] == 42.5
        assert gender_values["M"] == 40.2
        assert gender_values["K"] == 44.8

    def test_flatten_ou_data(self, sample_ou_data_response):
        """Test flattening organizational unit data."""
        # Create a list with a single OU data response
        data = [sample_ou_data_response]
        
        # Flatten the data
        flattened = flatten_data(data)
        
        # Check the result
        assert len(flattened) == 1  # 1 value in sample OU data
        
        # Check the fields
        assert flattened[0]["kpi"] == "N00001"
        assert flattened[0]["ou"] == "V11001"
        assert flattened[0]["period"] == 2022
        assert flattened[0]["gender"] == "T"
        assert flattened[0]["value"] == 42.5

    def test_flatten_empty_data(self):
        """Test flattening empty data."""
        # Empty list
        assert flatten_data([]) == []
        
        # List with empty values
        data = [{"kpi": "N00001", "municipality": "1480", "period": 2022, "values": []}]
        flattened = flatten_data(data)
        assert len(flattened) == 1
        assert flattened[0]["kpi"] == "N00001"
        assert flattened[0]["municipality"] == "1480"
        assert flattened[0]["period"] == 2022
        assert "gender" not in flattened[0]
        assert "value" not in flattened[0]

    def test_flatten_missing_fields(self):
        """Test flattening data with missing fields."""
        # Missing municipality and ou
        data = [{"kpi": "N00001", "period": 2022, "values": [{"gender": "T", "value": 42.5}]}]
        flattened = flatten_data(data)
        assert len(flattened) == 1
        assert flattened[0]["kpi"] == "N00001"
        assert flattened[0]["period"] == 2022
        assert "municipality" not in flattened[0]
        assert "ou" not in flattened[0]
        assert flattened[0]["gender"] == "T"
        assert flattened[0]["value"] == 42.5


class TestGroupDataByPeriod:
    """Test the group_data_by_period function."""

    def test_group_data_single_kpi(self):
        """Test grouping data with a single KPI."""
        # Create sample flattened data
        data = [
            {"kpi": "N00001", "period": 2020, "gender": "T", "value": 40.0},
            {"kpi": "N00001", "period": 2021, "gender": "T", "value": 42.5},
            {"kpi": "N00001", "period": 2022, "gender": "T", "value": 45.0}
        ]
        
        # Group by period
        grouped = group_data_by_period(data)
        
        # Check the result
        assert len(grouped) == 3  # 3 periods
        assert 2020 in grouped
        assert 2021 in grouped
        assert 2022 in grouped
        
        # Check the values
        assert grouped[2020]["N00001_T"] == 40.0
        assert grouped[2021]["N00001_T"] == 42.5
        assert grouped[2022]["N00001_T"] == 45.0

    def test_group_data_multiple_kpis(self):
        """Test grouping data with multiple KPIs."""
        # Create sample flattened data with multiple KPIs
        data = [
            {"kpi": "N00001", "period": 2022, "gender": "T", "value": 45.0},
            {"kpi": "N00002", "period": 2022, "gender": "T", "value": 30.0},
            {"kpi": "N00001", "period": 2021, "gender": "T", "value": 42.5},
            {"kpi": "N00002", "period": 2021, "gender": "T", "value": 28.5}
        ]
        
        # Group by period
        grouped = group_data_by_period(data)
        
        # Check the result
        assert len(grouped) == 2  # 2 periods (2021, 2022)
        assert 2021 in grouped
        assert 2022 in grouped
        
        # Check each period has both KPIs
        assert "N00001_T" in grouped[2021]
        assert "N00002_T" in grouped[2021]
        assert "N00001_T" in grouped[2022]
        assert "N00002_T" in grouped[2022]
        
        # Check the values
        assert grouped[2021]["N00001_T"] == 42.5
        assert grouped[2021]["N00002_T"] == 28.5
        assert grouped[2022]["N00001_T"] == 45.0
        assert grouped[2022]["N00002_T"] == 30.0

    def test_group_data_with_gender(self):
        """Test grouping data with gender breakdown."""
        # Create sample flattened data with gender breakdown
        data = [
            {"kpi": "N00001", "period": 2022, "gender": "T", "value": 45.0},
            {"kpi": "N00001", "period": 2022, "gender": "M", "value": 43.0},
            {"kpi": "N00001", "period": 2022, "gender": "K", "value": 47.0}
        ]
        
        # Group by period
        grouped = group_data_by_period(data)
        
        # Check the result
        assert len(grouped) == 1  # 1 period (2022)
        assert 2022 in grouped
        
        # Check all gender categories
        assert "N00001_T" in grouped[2022]
        assert "N00001_M" in grouped[2022]
        assert "N00001_K" in grouped[2022]
        
        # Check the values
        assert grouped[2022]["N00001_T"] == 45.0
        assert grouped[2022]["N00001_M"] == 43.0
        assert grouped[2022]["N00001_K"] == 47.0

    def test_group_data_custom_value_column(self):
        """Test grouping data with a custom value column."""
        # Create sample flattened data
        data = [
            {"kpi": "N00001", "period": 2022, "gender": "T", "value": 45.0, "count": 100},
            {"kpi": "N00001", "period": 2021, "gender": "T", "value": 42.5, "count": 95}
        ]
        
        # Group by period using the count column instead of value
        grouped = group_data_by_period(data, value_col="count")
        
        # Check the result
        assert len(grouped) == 2  # 2 periods
        assert grouped[2021]["N00001_T"] == 95
        assert grouped[2022]["N00001_T"] == 100

    def test_group_data_empty_data(self):
        """Test grouping empty data."""
        # Empty list
        assert group_data_by_period([]) == {}
        
        # List with missing values
        data = [{"kpi": "N00001", "period": 2022}]  # Missing value
        grouped = group_data_by_period(data)
        assert len(grouped) == 1
        assert grouped[2022]["N00001"] is None


class TestGetEntityType:
    """Test the get_entity_type function."""

    def test_kpi_entity_types(self):
        """Test identifying KPI entity types."""
        assert get_entity_type("N00001") == "kpi"
        assert get_entity_type("N12345") == "kpi"
        assert get_entity_type("U00001") == "kpi"
        assert get_entity_type("U12345") == "kpi"
        
        # Invalid KPI formats
        assert get_entity_type("N0001") != "kpi"  # Too short
        assert get_entity_type("N123456") != "kpi"  # Too long
        assert get_entity_type("X00001") != "kpi"  # Wrong prefix

    def test_municipality_entity_types(self):
        """Test identifying municipality entity types."""
        assert get_entity_type("1480") == "municipality"
        assert get_entity_type("0123") == "municipality"
        assert get_entity_type("9999") == "municipality"
        
        # Invalid municipality formats
        assert get_entity_type("123") != "municipality"  # Too short
        assert get_entity_type("12345") != "municipality"  # Too long
        assert get_entity_type("A123") != "municipality"  # Not all digits

    def test_ou_entity_types(self):
        """Test identifying organizational unit entity types."""
        assert get_entity_type("V11001") == "ou"
        assert get_entity_type("V17123") == "ou"
        assert get_entity_type("V23456") == "ou"
        
        # Invalid OU formats
        assert get_entity_type("V1") != "ou"  # Too short
        assert get_entity_type("X11001") != "ou"  # Wrong prefix
        assert get_entity_type("V1A001") != "ou"  # Non-digit after prefix

    def test_unknown_entity_types(self):
        """Test identifying unknown entity types."""
        assert get_entity_type("") == "unknown"
        assert get_entity_type(None) == "unknown"
        assert get_entity_type("ABC123") == "unknown"
        assert get_entity_type(12345) == "unknown"  # Integer, not string


class TestSelectAndReorderColumns:
    """Test the select_and_reorder_columns function."""

    def test_without_metadata(self):
        """Test when include_metadata is False."""
        # Create a sample DataFrame
        df = pd.DataFrame({
            'kpi': ['N00001'],
            'period': [2022],
            'municipality': ['1480'],
            'gender': ['T'],
            'value': [42.5],
            'kpi_title': ['Test KPI'],
            'municipality_title': ['Göteborg']
        })
        
        # Test without metadata (should return the original DataFrame)
        result = select_and_reorder_columns(df, include_metadata=False)
        assert result.equals(df)

    def test_with_municipality_data(self):
        """Test with municipality data and include_metadata=True."""
        # Create a sample DataFrame with municipality data
        df = pd.DataFrame({
            'kpi': ['N00001'],
            'period': [2022],
            'municipality': ['1480'],
            'municipality_title': ['Göteborg'],
            'municipality_type': ['K'],
            'gender': ['T'],
            'value': [42.5],
            'kpi_title': ['Test KPI'],
            'count': [1],
            'status': ['OK'],
            'extra_column': ['Extra']  # This column should be excluded
        })
        
        # Test with metadata
        result = select_and_reorder_columns(df, include_metadata=True)
        
        # Check that only the expected columns are included, in the right order
        expected_columns = [
            'kpi', 'period', 'municipality', 'municipality_title',
            'municipality_type', 'gender', 'value', 'kpi_title',
            'count', 'status'
        ]
        assert list(result.columns) == expected_columns
        assert 'extra_column' not in result.columns

    def test_with_ou_data(self):
        """Test with OU data and include_metadata=True."""
        # Create a sample DataFrame with OU data
        df = pd.DataFrame({
            'kpi': ['N00001'],
            'period': [2022],
            'ou': ['V11001'],
            'ou_title': ['Sample School'],
            'ou_municipality': ['1480'],
            'gender': ['T'],
            'value': [38.7],
            'kpi_title': ['Test KPI'],
            'count': [1],
            'status': ['OK']
        })
        
        # Test with metadata
        result = select_and_reorder_columns(df, include_metadata=True)
        
        # Check that only the expected columns are included
        assert 'ou' in result.columns
        assert 'ou_title' in result.columns
        assert 'ou_municipality' in result.columns
        assert 'kpi_title' in result.columns
        
        # Check the order
        # kpi and period should be first
        assert list(result.columns)[0] == 'kpi'
        assert list(result.columns)[1] == 'period'
        # gender, value should be after ou_* columns
        gender_idx = list(result.columns).index('gender')
        ou_title_idx = list(result.columns).index('ou_title')
        assert gender_idx > ou_title_idx

    def test_with_missing_columns(self):
        """Test with some expected columns missing."""
        # Create a sample DataFrame with missing columns
        df = pd.DataFrame({
            'kpi': ['N00001'],
            'period': [2022],
            'municipality': ['1480'],
            'value': [42.5]
            # missing: gender, municipality_title, etc.
        })
        
        # Test with metadata
        result = select_and_reorder_columns(df, include_metadata=True)
        
        # Check that only the available columns are included
        assert list(result.columns) == ['kpi', 'period', 'municipality', 'value']

    def test_with_no_data(self):
        """Test with an empty DataFrame."""
        # Create an empty DataFrame
        df = pd.DataFrame(columns=['kpi', 'period', 'value'])
        
        # Test with metadata
        result = select_and_reorder_columns(df, include_metadata=True)
        
        # Check that the result is still an empty DataFrame with the same columns
        assert result.empty
        assert list(result.columns) == ['kpi', 'period', 'value']