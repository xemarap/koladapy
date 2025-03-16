import datetime
import pandas as pd
from typing import List, Dict, Union, Any, Optional


def parse_date(date_obj: Union[str, datetime.date]) -> str:
    """
    Parse a date object or string into a string format that the API accepts.
    
    Args:
        date_obj: Date as a string or datetime.date object
        
    Returns:
        Date string in format 'YYYY-MM-DD'
    """
    if isinstance(date_obj, str):
        # Check if the date is in the correct format
        try:
            datetime.datetime.strptime(date_obj, '%Y-%m-%d')
            return date_obj
        except ValueError:
            raise ValueError(f"Date string '{date_obj}' is not in the format 'YYYY-MM-DD'")
    elif isinstance(date_obj, datetime.date):
        return date_obj.strftime('%Y-%m-%d')
    else:
        raise TypeError("date_obj must be a string or datetime.date object")


def flatten_data(data: List[Dict]) -> List[Dict]:
    """
    Flatten the nested structure of the Kolada API data response.
    
    The API returns data in a nested format where each item has a 'values' list
    containing multiple data points. This function flattens that structure.
    
    Args:
        data: List of data dictionaries from the API
        
    Returns:
        Flattened list of dictionaries
    """
    flattened = []
    
    for item in data:
        base_item = {
            'kpi': item.get('kpi'),
            'period': item.get('period')
        }
        
        # Add municipality or ou depending on which one is present
        if 'municipality' in item:
            base_item['municipality'] = item.get('municipality')
        elif 'ou' in item:
            base_item['ou'] = item.get('ou')
        
        # Extract and flatten the values
        values = item.get('values', [])
        
        # If there are no values, still add the base item
        if not values:
            flattened.append(base_item.copy())
            continue
        
        for value_item in values:
            item_copy = base_item.copy()
            
            # Add the values
            item_copy['gender'] = value_item.get('gender')
            item_copy['count'] = value_item.get('count')
            item_copy['status'] = value_item.get('status')
            item_copy['value'] = value_item.get('value')
            item_copy['isdeleted'] = value_item.get('isdeleted')
            
            flattened.append(item_copy)
    
    return flattened


def group_data_by_period(data: List[Dict], value_col: str = 'value') -> Dict[int, Dict[str, Any]]:
    """
    Group data by period (year) for easier analysis.
    
    Args:
        data: Flattened data from flatten_data()
        value_col: Name of the column containing the value to use
        
    Returns:
        Dictionary mapping periods to dictionaries of values
    """
    result = {}
    
    for item in data:
        period = item.get('period')
        if period not in result:
            result[period] = {}
        
        # Create identifier based on KPI and gender
        identifier = item.get('kpi', '')
        if item.get('gender'):
            identifier += f"_{item.get('gender')}"
            
        result[period][identifier] = item.get(value_col)
    
    return result


def get_entity_type(entity_id: str) -> str:
    """
    Determine the type of entity based on its ID format.
    
    Args:
        entity_id: Entity ID string
        
    Returns:
        Type of entity ('kpi', 'municipality', 'ou', or 'unknown')
    """
    if not entity_id or not isinstance(entity_id, str):
        return 'unknown'
        
    if entity_id[0] in ('N', 'U') and len(entity_id) == 6 and entity_id[1:].isdigit():
        return 'kpi'
    elif len(entity_id) == 4 and entity_id.isdigit():
        return 'municipality'
    elif entity_id[0] == 'V' and len(entity_id) >= 3 and entity_id[1:3].isdigit():
        return 'ou'
    else:
        return 'unknown'
    

def select_and_reorder_columns(df: pd.DataFrame, include_metadata: bool = False) -> pd.DataFrame:
    """
    Select and reorder DataFrame columns based on whether OU data is included
    and whether metadata is requested.
    
    Args:
        df: Input DataFrame with Kolada data
        include_metadata: Whether metadata columns should be included
        
    Returns:
        DataFrame with selected and reordered columns
    """
    if not include_metadata:
        return df
        
    if 'ou' in df.columns:
        # When OU data is included
        columns_to_keep = [
            'kpi', 'period', 
            'ou_municipality',
            'ou', 'ou_title', 
            'gender', 'value',
            'kpi_title',
            'kpi_auspice',
            'kpi_operating_area', 
            'kpi_perspective'
            
        ]
    else:
        # When only municipality data is included
        columns_to_keep = [
            'kpi', 'period', 
            'municipality', 'municipality_title',
            'municipality_type', 
            'gender', 'value',
            'kpi_title',
            'kpi_auspice',
            'kpi_operating_area', 
            'kpi_perspective'
        ]

    # Keep only columns that exist in the DataFrame
    existing_columns = [col for col in columns_to_keep if col in df.columns]

    # Add any important columns that weren't in our predefined list
    other_important_cols = ['count', 'status']
    for col in other_important_cols:
        if col in df.columns and col not in existing_columns:
            existing_columns.append(col)

    # Return the DataFrame with only the selected columns in the specified order
    return df[existing_columns]