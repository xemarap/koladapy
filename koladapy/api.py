import json
import datetime
import logging
from typing import Dict, List, Union, Optional, Any, Tuple
import time
from urllib.parse import urljoin
import itertools

import requests
import pandas as pd
from tqdm import tqdm
import backoff

from .exceptions import KoladaAPIError, KoladaRateLimitError
from .utils import flatten_data, parse_date, select_and_reorder_columns

logger = logging.getLogger(__name__)


class KoladaAPI:
    """
    A Python wrapper for the Kolada API (V3).
    
    This class provides methods to interact with the Kolada API,
    which contains key performance indicators for Swedish municipalities.
    """
    
    BASE_URL = "https://api.kolada.se/v3/"
    DEFAULT_PER_PAGE = 5000  # Maximum allowed by the API
    MAX_BATCH_SIZE = 25  # Maximum number of IDs to include in a single request
    
    def __init__(self, 
                 base_url: str = None, 
                 max_retries: int = 3,
                 timeout: int = 30,
                 max_requests_per_second: float = 5.0,
                 max_batch_size: int = None):
        """
        Initialize the Kolada API client.
        
        Args:
            base_url: The base URL for the API. Defaults to https://api.kolada.se/v3/
            max_retries: Maximum number of retries for failed requests
            timeout: Request timeout in seconds
            max_requests_per_second: Maximum number of requests per second
            max_batch_size: Maximum number of IDs to include in a single batch (default: 5)
        """
        self.base_url = base_url or self.BASE_URL
        self.session = requests.Session()
        self.timeout = timeout
        self.max_retries = max_retries
        self.min_request_interval = 1.0 / max_requests_per_second
        self.last_request_time = 0
        self.max_batch_size = max_batch_size or self.MAX_BATCH_SIZE
    
    def _throttle_request(self):
        """Throttle requests to respect the rate limit."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    @backoff.on_exception(backoff.expo, 
                         (requests.exceptions.RequestException, KoladaRateLimitError),
                         max_tries=5)
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Make a request to the Kolada API.
        
        Args:
            endpoint: The API endpoint to request
            params: URL parameters for the request
            
        Returns:
            The parsed JSON response
            
        Raises:
            KoladaAPIError: If the API returns an error
        """
        self._throttle_request()
        
        url = urljoin(self.base_url, endpoint)
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            
            # Check for rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
                raise KoladaRateLimitError(f"Rate limit exceeded. Retry after {retry_after}s")
                
            # Check for other errors
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to {url}: {str(e)}")
            raise KoladaAPIError(f"Error making request to {url}: {str(e)}") from e
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response from {url}: {str(e)}")
            raise KoladaAPIError(f"Error parsing JSON response from {url}: {str(e)}") from e
    
    def _paginate_request(self, endpoint: str, params: Dict = None) -> List[Dict]:
        """
        Handle pagination for API requests.
        
        Args:
            endpoint: The API endpoint to request
            params: URL parameters for the request
            
        Returns:
            List of items from all pages
        """
        params = params or {}
        params['page'] = params.get('page', 1)
        params['per_page'] = params.get('per_page', self.DEFAULT_PER_PAGE)
        
        all_items = []
        total_count = None
        
        with tqdm(desc=f"Fetching {endpoint}", unit="items", disable=None) as pbar:
            while True:
                response = self._make_request(endpoint, params)
                items = response.get('values', [])
                all_items.extend(items)
                
                # Update progress bar if we know the total count
                if total_count is None and 'count' in response:
                    total_count = response['count']
                    pbar.total = total_count
                
                pbar.update(len(items))
                
                # Check if there are more pages
                next_url = response.get('next_url')
                if not next_url:
                    break
                
                # Update page parameter for next request
                params['page'] += 1
        
        return all_items

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
        
        with tqdm(desc=f"Processing batches for {endpoint}", total=total_batches, unit="batch") as pbar:
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

    #
    # KPI Methods
    #
    
    def search_kpis(self, 
               query: str = None, 
               publication_date: Union[str, datetime.date] = None,
               operating_area: str = None,
               as_dataframe: bool = False) -> Union[List[Dict], pd.DataFrame]:
        """
        Search for KPIs in the Kolada database.
    
        Args:
            query: Search terms to filter KPIs by title
            publication_date: Filter KPIs by publication date (format: YYYY-MM-DD)
            operating_area: Filter KPIs by operating area (e.g., "Hälso- och sjukvård")
            as_dataframe: If True, return results as a Pandas DataFrame
        
        Returns:
            List of KPI dictionaries or a Pandas DataFrame
        """
        params = {}
    
        if query:
            params['title'] = query
        
        kpis = self._paginate_request('kpi', params)
    
        # Filter by publication date if specified
        if publication_date:
            pub_date = parse_date(publication_date)
            kpis = [k for k in kpis if k.get('publication_date') == pub_date]
        
        # Filter by operating area if specified
        if operating_area:
            kpis = [k for k in kpis if k.get('operating_area') == operating_area]
    
        if as_dataframe:
            return pd.DataFrame(kpis)
    
        return kpis
    
    def get_kpi(self, kpi_id: str) -> Dict:
        """
        Get a specific KPI by ID.
        
        Args:
            kpi_id: The ID of the KPI to retrieve
            
        Returns:
            KPI dictionary
        """
        response = self._make_request(f'kpi/{kpi_id}')
        kpis = response.get('values', [])
        
        if not kpis:
            raise KoladaAPIError(f"KPI with ID {kpi_id} not found")
            
        return kpis[0]
    
    def get_kpis(self,
                 kpi_ids: List[str],
                 as_dataframe: bool = False) -> Union[List[Dict], pd.DataFrame]:
        """
        Get multiple KPIs by their IDs.
        
        Args:
            kpi_ids: List of KPI IDs to retrieve
            as_dataframe: If True, return results as a Pandas DataFrame
            
        Returns:
            List of KPI dictionaries or a Pandas DataFrame
        """
        if len(kpi_ids) <= self.max_batch_size:
            # The API accepts comma-separated IDs
            ids_param = ','.join(kpi_ids)
            kpis = self._paginate_request(f'kpi/{ids_param}')
        else:
            # Handle batching for large numbers of KPI IDs
            all_kpis = []
            for i in range(0, len(kpi_ids), self.max_batch_size):
                batch = kpi_ids[i:i + self.max_batch_size]
                ids_param = ','.join(batch)
                batch_kpis = self._paginate_request(f'kpi/{ids_param}')
                all_kpis.extend(batch_kpis)
            kpis = all_kpis
        
        if as_dataframe:
            return pd.DataFrame(kpis)
            
        return kpis
    
    def get_kpi_groups(self,
                       query: str = None,
                       as_dataframe: bool = False) -> Union[List[Dict], pd.DataFrame]:
        """
        Get KPI groups.
        
        Args:
            query: Optional search term to filter groups by title
            as_dataframe: If True, return results as a Pandas DataFrame
            
        Returns:
            List of KPI group dictionaries or a Pandas DataFrame
        """
        params = {}
        if query:
            params['title'] = query
            
        groups = self._paginate_request('kpi_groups', params)
        
        if as_dataframe:
            return pd.DataFrame(groups)
            
        return groups
    
    def get_kpi_group(self, group_id: str) -> Dict:
        """
        Get a specific KPI group by ID.
        
        Args:
            group_id: The ID of the KPI group to retrieve
            
        Returns:
            KPI group dictionary
        """
        response = self._make_request(f'kpi_groups/{group_id}')
        groups = response.get('values', [])
        
        if not groups:
            raise KoladaAPIError(f"KPI group with ID {group_id} not found")
            
        return groups[0]
    
    #
    # Municipality Methods
    #
    
    def get_municipalities(self, 
                          query: str = None, 
                          municipality_type: str = None,
                          as_dataframe: bool = False) -> Union[List[Dict], pd.DataFrame]:
        """
        Get municipalities.
        
        Args:
            query: Optional search term to filter municipalities by name
            municipality_type: Filter by municipality type (e.g., 'K' for kommun, 'L' for landsting)
            as_dataframe: If True, return results as a Pandas DataFrame
            
        Returns:
            List of municipality dictionaries or a Pandas DataFrame
        """
        params = {}
        
        if query:
            params['title'] = query
            
        if municipality_type:
            params['type'] = municipality_type
            
        municipalities = self._paginate_request('municipality', params)
        
        if as_dataframe:
            return pd.DataFrame(municipalities)
            
        return municipalities
    
    def get_municipality(self, municipality_id: str) -> Dict:
        """
        Get a specific municipality by ID.
        
        Args:
            municipality_id: The ID of the municipality to retrieve
            
        Returns:
            Municipality dictionary
        """
        response = self._make_request(f'municipality/{municipality_id}')
        municipalities = response.get('values', [])
        
        if not municipalities:
            raise KoladaAPIError(f"Municipality with ID {municipality_id} not found")
            
        return municipalities[0]
    
    def get_municipality_groups(self,
                                query: str = None,
                                as_dataframe: bool = False) -> Union[List[Dict], pd.DataFrame]:
        """
        Get municipality groups.
        
        Args:
            query: Optional search term to filter groups by name
            as_dataframe: If True, return results as a Pandas DataFrame
            
        Returns:
            List of municipality group dictionaries or a Pandas DataFrame
        """
        params = {}
        if query:
            params['title'] = query
            
        groups = self._paginate_request('municipality_groups', params)
        
        if as_dataframe:
            return pd.DataFrame(groups)
            
        return groups
    
    def get_municipality_group(self, group_id: str) -> Dict:
        """
        Get a specific municipality group by ID.
        
        Args:
            group_id: The ID of the municipality group to retrieve
            
        Returns:
            Municipality group dictionary
        """
        response = self._make_request(f'municipality_groups/{group_id}')
        groups = response.get('values', [])
        
        if not groups:
            raise KoladaAPIError(f"Municipality group with ID {group_id} not found")
            
        return groups[0]
    
    #
    # Organizational Unit Methods
    #
    
    def get_organizational_units(self, 
                          query: str = None, 
                          municipality: str = None,
                          ou_type: str = None,
                          as_dataframe: bool = False) -> Union[List[Dict], pd.DataFrame]:
        """
        Get organizational units.
    
        Args:
            query: Optional search term to filter units by name
            municipality: Filter by municipality ID
            ou_type: Filter by organizational unit type prefix (e.g., 'V11' for preschools)
            as_dataframe: If True, return results as a Pandas DataFrame
        
        Returns:
            List of organizational unit dictionaries or a Pandas DataFrame
        """
        params = {}
    
        if query:
            params['title'] = query
        
        if municipality:
            params['municipality'] = municipality
        
        units = self._paginate_request('ou', params)
    
        # Filter by OU type if specified
        if ou_type:
            units = [u for u in units if u.get('id', '').startswith(ou_type)]
    
        if as_dataframe:
            return pd.DataFrame(units)
        
        return units
    
    def get_organizational_unit(self, ou_id: str) -> Dict:
        """
        Get a specific organizational unit by ID.
        
        Args:
            ou_id: The ID of the organizational unit to retrieve
            
        Returns:
            Organizational unit dictionary
        """
        response = self._make_request(f'ou/{ou_id}')
        units = response.get('values', [])
        
        if not units:
            raise KoladaAPIError(f"Organizational unit with ID {ou_id} not found")
            
        return units[0]
    
    #
    # Data Methods
    #
    
    def get_values(self,
                kpi_id: Optional[Union[str, List[str]]] = None,
                municipality_id: Optional[Union[str, List[str]]] = None,
                years: Optional[Union[int, List[int]]] = None,
                ou_id: Optional[Union[str, List[str]]] = None,
                updated_since: Optional[Union[str, datetime.date]] = None) -> List[Dict]:
        """
        Get data from the Kolada API with automatic batching for large parameter lists.
        
        Args:
            kpi_id: KPI ID(s) to retrieve data for
            municipality_id: Municipality ID(s) to retrieve data for
            years: Year(s) to retrieve data for
            ou_id: Organizational unit ID(s) to retrieve data for
            updated_since: Filter data updated since this date (format: YYYY-MM-DD)
            
        Returns:
            List of data dictionaries
            
        Note:
            At least one of kpi_id, municipality_id, or years must be provided.
            If ou_id is provided, the 'oudata/' endpoint will be used instead of 'data/'.
            Large parameter lists will be automatically batched to prevent API errors.
        """
        # Determine which endpoint to use based on parameters
        if ou_id:
            # Organizational unit data endpoint
            endpoint = 'oudata/'
            params = {}
            
            # Convert single values to lists for consistent handling
            if isinstance(ou_id, str):
                ou_id = [ou_id]
            params['ou_id'] = ou_id
                
        else:
            # Regular data endpoint
            endpoint = 'data/'
            params = {}
        
        # Process parameters
        if kpi_id:
            if isinstance(kpi_id, str):
                kpi_id = [kpi_id]
            params['kpi_id'] = kpi_id
                
        if municipality_id:
            if isinstance(municipality_id, str):
                municipality_id = [municipality_id]
            params['municipality_id'] = municipality_id
                
        if years:
            if isinstance(years, int):
                years = [years]
            params['year'] = years
                
        if updated_since:
            params['from_date'] = parse_date(updated_since)
        
        # Use the batching request method for parameters that might be too large
        batch_params = []
        if 'kpi_id' in params and len(params['kpi_id']) > self.max_batch_size:
            batch_params.append('kpi_id')
            
        if 'municipality_id' in params and len(params['municipality_id']) > self.max_batch_size:
            batch_params.append('municipality_id')
            
        if 'ou_id' in params and len(params['ou_id']) > self.max_batch_size:
            batch_params.append('ou_id')
            
        if 'year' in params and len(params['year']) > self.max_batch_size:
            batch_params.append('year')
        
        # If we need batching, use the batch request method
        if batch_params:
            data = self._batch_request(endpoint, params, batch_params)
        else:
            # Otherwise, use the standard paginated request
            data = self._paginate_request(endpoint, params)
        
        return data
    
    def get_data_as_dataframe(self,
                             kpi_id: Optional[Union[str, List[str]]] = None,
                             municipality_id: Optional[Union[str, List[str]]] = None,
                             years: Optional[Union[int, List[int]]] = None,
                             ou_id: Optional[Union[str, List[str]]] = None,
                             updated_since: Optional[Union[str, datetime.date]] = None,
                             include_metadata: bool = False) -> pd.DataFrame:
        """
        Get data from the Kolada API as a tidy Pandas DataFrame with automatic batching.
        
        Args:
            kpi_id: KPI ID(s) to retrieve data for
            municipality_id: Municipality ID(s) to retrieve data for
            years: Year(s) to retrieve data for
            ou_id: Organizational unit ID(s) to retrieve data for
            updated_since: Filter data updated since this date (format: YYYY-MM-DD)
            include_metadata: If True, include KPI and municipality metadata
            
        Returns:
            Tidy Pandas DataFrame with data
        """
        # Get the raw data with automatic batching
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
            
            # If municipality data is present, add municipality metadata
            if 'municipality' in df.columns:
                unique_municipalities = df['municipality'].unique().tolist()
                
                # Skip if there are too many unique municipalities (to avoid too many API calls)
                if len(unique_municipalities) <= 50:
                    try:
                        muni_metadata = self.get_municipalities(as_dataframe=True)
                        
                        # Rename columns to avoid conflicts
                        muni_metadata = muni_metadata.rename(columns={
                            col: f'municipality_{col}' for col in muni_metadata.columns if col != 'id'
                        })
                        
                        # Merge metadata with the data
                        df = df.merge(muni_metadata, left_on='municipality', right_on='id', how='left')
                    except Exception as e:
                        logger.warning(f"Could not fetch municipality metadata: {str(e)}")
            
            # If OU data is present, add OU metadata
            if 'ou' in df.columns and ou_id:
                unique_ous = df['ou'].unique().tolist()
    
                # Skip if there are too many unique OUs (to avoid too many API calls)
                if len(unique_ous) <= 50:
                    try:
                    # Fetch OU metadata for the unique OUs
                        ou_metadata_list = []
                        for ou in unique_ous:
                            try:
                                ou_data = self.get_organizational_unit(ou)
                                ou_metadata_list.append(ou_data)
                            except Exception as e:
                                logger.warning(f"Could not fetch metadata for OU {ou}: {str(e)}")
            
                        if ou_metadata_list:
                            ou_metadata = pd.DataFrame(ou_metadata_list)
                
                            # Rename columns to avoid conflicts
                            ou_metadata = ou_metadata.rename(columns={
                                col: f'ou_{col}' for col in ou_metadata.columns if col != 'id'
                            })
                
                            # Merge metadata with the data
                            df = df.merge(ou_metadata, left_on='ou', right_on='id', how='left')
                    except Exception as e:
                            logger.warning(f"Could not fetch OU metadata: {str(e)}")
            
        # After all the metadata merging is done
        df = select_and_reorder_columns(df, include_metadata)  
        
        return df
