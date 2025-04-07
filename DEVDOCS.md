# KoladaPy Batching and Data Compilation Process

## Overview

This document describes the batching and data compilation mechanisms implemented in the KoladaPy library, which are essential for handling large API calls to the Kolada API while maintaining data integrity. The Kolada API imposes limitations on the number of parameters (e.g., KPI IDs, municipality IDs) that can be included in a single request, making these mechanisms necessary for comprehensive data analysis.

## The Batching Process

### Core Implementation

The batching process is primarily implemented in the `_batch_request` method of the `KoladaAPI` class. This method handles the following key functions:

1. **Parameter Extraction and Batching**
   - Identifies parameters that need batching
   - Splits large parameter lists into manageable batches
   - Generates all necessary combinations of batched parameters

2. **Batch Processing**
   - Processes each batch combination individually
   - Tracks progress with visual feedback
   - Handles errors to ensure process completion

```python
def _batch_request(self, endpoint: str, params: Dict, batch_params: List[str]) -> List[Dict]:
    """
    Handle batching for large parameter lists to prevent API errors.
    
    Args:
        endpoint: The API endpoint to request
        params: URL parameters for the request
        batch_params: List of parameter names that should be batched
        
    Returns:
        Combined list of items from all batched requests
    """
    # Extract parameters that need batching
    batch_param_values = {}
    for param in batch_params:
        if param in params and params[param] and isinstance(params[param], list):
            batch_param_values[param] = params[param]
            params.pop(param)
    
    # If no parameters need batching, just do a normal paginated request
    if not batch_param_values:
        return self._paginate_request(endpoint, params)
        
    # Generate batches for each parameter
    param_batches = {}
    for param, values in batch_param_values.items():
        # Split the list into batches of max_batch_size
        param_batches[param] = [
            values[i:i + self.max_batch_size] 
            for i in range(0, len(values), self.max_batch_size)
        ]
    
    # Generate all combinations of batches
    param_names = list(param_batches.keys())
    batch_combinations = list(itertools.product(
        *[range(len(param_batches[p])) for p in param_names]
    ))
    
    all_items = []
    total_batches = len(batch_combinations)
    
    with tqdm(desc=f"Processing batches for {endpoint}", 
              total=total_batches, unit="batch") as pbar:
        for batch_indices in batch_combinations:
            # Create a new params dict for this batch
            batch_params = params.copy()
            
            # Add the current batch values for each parameter
            for i, param_name in enumerate(param_names):
                batch_idx = batch_indices[i]
                batch_params[param_name] = param_batches[param_name][batch_idx]
            
            try:
                # Make the paginated request for this batch
                batch_items = self._paginate_request(endpoint, batch_params)
                all_items.extend(batch_items)
            except Exception as e:
                logger.error(f"Error in batch request for {batch_params}: {str(e)}")
                # Continue with other batches even if one fails
            
            pbar.update(1)
    
    return all_items
```

### Batch Size Configuration

The maximum batch size is configurable:

```python
class KoladaAPI:
    # Default constants
    MAX_BATCH_SIZE = 25  # Maximum number of IDs to include in a single request
    
    def __init__(self, 
                 base_url: str = None, 
                 max_retries: int = 3,
                 timeout: int = 30,
                 max_requests_per_second: float = 5.0,
                 max_batch_size: int = None):
        # Initialize with custom or default batch size
        self.max_batch_size = max_batch_size or self.MAX_BATCH_SIZE
```

## Data Compilation Process

After retrieving data in batches, KoladaPy compiles the results into a unified dataset through several stages:

### 1. Result Aggregation

Each batch response is added to a cumulative list in the `_batch_request` method:

```python
all_items = []
# ...
for batch_indices in batch_combinations:
    # ...
    batch_items = self._paginate_request(endpoint, batch_params)
    all_items.extend(batch_items)
# ...
return all_items
```

### 2. Data Flattening

The nested structure of the API response is flattened using the `flatten_data` function:

```python
def flatten_data(data: List[Dict]) -> List[Dict]:
    """
    Flatten the nested structure of the Kolada API data response.
    
    The API returns data in a nested format where each item has a 'values' list
    containing multiple data points. This function flattens that structure.
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
```

### 3. DataFrame Conversion

In methods like `get_data_as_dataframe`, the flattened data is converted to a pandas DataFrame:

```python
def get_data_as_dataframe(self, ...):
    # ...
    data = self.get_values(
        kpi_id=kpi_id,
        municipality_id=municipality_id,
        years=years,
        ou_id=ou_id,
        updated_since=updated_since
    )
    
    # Flatten the nested data structure
    flattened_data = flatten_data(data)
    
    if not flattened_data:
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(flattened_data)
    # ...
```

### 4. Metadata Enrichment

If requested, metadata about KPIs, municipalities, or organizational units is added:

```python
# Include metadata if requested
if include_metadata and df.shape[0] > 0:
    # Get unique KPI IDs
    unique_kpis = df['kpi'].unique().tolist()
    
    # Fetch KPI metadata
    kpi_metadata = self.get_kpis(unique_kpis, as_dataframe=True)
    
    # Rename columns to avoid conflicts
    kpi_metadata = kpi_metadata.rename(columns={
        col: f'kpi_{col}' for col in kpi_metadata.columns if col != 'id'
    })
    
    # Merge metadata with the data
    df = df.merge(kpi_metadata, left_on='kpi', right_on='id', how='left')
    
    # Similar code for municipality and organizational unit metadata...
```

### 5. Column Selection and Reordering

Finally, columns are selected and reordered for better readability:

```python
def select_and_reorder_columns(df: pd.DataFrame, include_metadata: bool = False) -> pd.DataFrame:
    """
    Select and reorder DataFrame columns based on whether OU data is included
    and whether metadata is requested.
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
            # other metadata columns...
        ]
    else:
        # When only municipality data is included
        columns_to_keep = [
            'kpi', 'period', 
            'municipality', 'municipality_title',
            'municipality_type', 
            'gender', 'value',
            'kpi_title',
            # other metadata columns...
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
```

## Data Integrity Mechanisms

Several mechanisms ensure data accuracy and integrity throughout the process:

1. **Complete Data Coverage**: The batching algorithm generates all necessary combinations to ensure no data points are missed.

2. **Consistent Data Structure**: The same flattening function is applied to all batches, ensuring consistent structure.

3. **Relationship Preservation**: The flattening process maintains the relationships between entities.

4. **Error Handling**: Individual batch errors are logged but don't prevent the completion of the overall process.

5. **Metadata Consistency**: When metadata is included, it's consistently applied across all data points.

## Usage Example

```python
from koladapy import KoladaAPI

# Initialize the API client
kolada = KoladaAPI()

# Get data for multiple KPIs and municipalities
df = kolada.get_data_as_dataframe(
    kpi_id=["N00001", "N00002", "N00003", "N00004", ...],  # Many KPI IDs
    municipality_id=["1480", "1481", "1482", ...],         # Many municipality IDs
    years=[2020, 2021, 2022],
    include_metadata=True
)

# The resulting DataFrame contains all combinations of KPIs and municipalities
# for the specified years, with metadata, compiled from multiple batched requests
```

## Performance Considerations

- **Throttling**: Requests are throttled to respect API rate limits
- **Progress Tracking**: Visual feedback shows progress through multiple batches
- **Memory Management**: Data is processed in batches rather than all at once
- **Error Resilience**: The process continues even if individual batches fail

## Conclusion

The batching and data compilation process in KoladaPy enables handling of large-scale data requests while maintaining data integrity, providing a robust foundation for comprehensive analysis of Swedish municipality data through the Kolada API.