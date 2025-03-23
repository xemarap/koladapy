# Basic Usage Examples for KoladaPy

This document provides examples of how to use the KoladaPy library to access data from the Kolada API.

## Initialization

```python
from koladapy import KoladaAPI

# Initialize the client
kolada = KoladaAPI()
```

## Searching for KPIs

You can search for KPIs in the Kolada database with the `search_kpis()` function using the following parameters:

- **query**: Search terms to filter KPIs by title
- **publication_date**: Filter KPIs by publication date (format: YYYY-MM-DD)
- **operating_area**: Filter KPIs by operating area (e.g., "Hälso- och sjukvård")

```python
# Search for KPIs related to education, returns raw API response as JSON
kpis = kolada.search_kpis(query= "gymnasieutbildning")
print(f"Found {len(kpis)} KPIs related to 'gymnasieutbildning'")

# Get KPIs as a Pandas DataFrame
kpis_df = kolada.search_kpis(query="gymnasieutbildning", as_dataframe=True)
print(kpis_df.head())

# Search for KPIs with a publication date
kpis = kolada.search_kpis(query="invånare", updated_since="2026-02-21")
print(f"Found {len(kpis)} KPIs with publication date 2026-02-21")

# Search KPIs by operating area
kpis_oa_df = kolada.search_kpis(operating_area="Hälso- och sjukvård", as_dataframe=True)
print(f"Found {len(kpis_oa_df)} KPIs")
print(kpis_oa_df.head())

# Filter the df on perspective and extract ids as a list
kpis_oa_df_perspective = kpis_oa_df[kpis_oa_df['perspective'] == "Volymer"]
print(f"Found {len(kpis_oa_df_perspective)} KPIs")
kpi_ids_gymn = kpis_oa_df_perspective['id'].tolist()
```

The retrieved ids can then be stored in a list and used to get data for the specific KPIs.

Alternatively - you can use `get_kpis()` to get metadata for specific KPIs.

```python
# Get KPI metadata by id as a DataFrame
kpi_metadata = kolada.get_kpis(
    kpi_ids=["N02280", "N02281", "N02282"],
    as_dataframe=True
    )
```

### Get KPI groups

Kolada groups KPIs in thematic areas, use the `get_kpi_groups()` function to search for and get the ids for specific KPI groups. Then use `get_kpi_group()` with the group_id to get and extract the KPI ids for the group. 

```python
# Search for KPI groups
kpi_groups = kolada.get_kpi_groups(query="BRP+")

# Extract member ids from kpi group
brp_plus = kolada.get_kpi_group(group_id="G2KPI150159")
brp_plus_ids = [brp_plus['member_id'] for brp_plus in brp_plus['members']]
```

## Working with Municipalities

The KoladaPy function offers flexible functions to search for and get ids for municipalities

```python
# Search for a specific municipality
municipalities = kolada.get_municipalities(query="Stockholm")
print(municipalities)

# Get municipalities of a specific type
municipalities = kolada.get_municipalities(municipality_type="K")  # K for kommun
print(f"Found {len(municipalities)} kommuner")

# Use get_municipalities() to filter for all regions and extract ids
regions = kolada.get_municipalities(municipality_type="L") # L for region
region_ids = [regions['id'] for regions in regions]
```

### Get Municipality Groups

Kolada groups municipalities in different groups, eg. all municipalities in a county. Use the `get_muncipality_groups()` function to search for and get the ids for specific municipality groups. Then use `get_municipality_group()` with the group_id to get and extract the municipality ids for the group.

```python
# Use get_municipality_groups() to search for groups
municipality_grp = kolada.get_municipality_groups(query="stockholm", as_dataframe=True)
print(f"Found {len(municipality_grp)} Municipality groups")

# Extract municipality ids from group
sthlm_county = kolada.get_municipality_group(group_id="G33612") # Stockholm county
sthlm_county_ids = [sthlm_county['member_id'] for sthlm_county in sthlm_county['members']]
```

## Getting Data

Use the `get_data_as_dataframe()` function to get data for selected KPIs, muncipalities and years. Set the include_metadata parameter to True to get relevant metadata for the KPI and municpality.

```python
# Get data for a specific KPI, municipality, and year
df = kolada.get_data_as_dataframe(
    kpi_id="N01951",  # Population
    municipality_id="0180",  # Stockholm municipality
    years = 2024,
    include_metadata=True
)

# Get data updated since a specific date
df_from = kolada.get_data_as_dataframe(
    kpi_id="N01951",
    municipality_id="0180",
    updated_since="2022-01-01"
)

# Get data for KPI and municipality groups
df_brp_plus = kolada.get_data_as_dataframe(
    kpi_id=brp_plus_ids # Retrieved KPI ids
    municipality_id=sthlm_county_ids, # Retrieved municipality ids
    years=[2022, 2023], # data for specified years
    include_metadata=True
)

# Get data as a DataFrame for regions and all available years
## Exclude the years parameter to get all available years
df_regions = kolada.get_data_as_dataframe(
    kpi_id="N03941",    # Unemployment rate
    municipality_id=region_ids, # Retrieved municipality ids for regions
    include_metadata=True
)
```

## Working with Organizational Units

If has_ou_data=True in the KPI metadata, then data for that KPI can be retrieved for organizational units. Use the `get_organizational_units()` function to search for an get ids for organizational units.

With the ou_type parameter you can filter the search for organizational type. Organizational units are grouped accordingly:

- V11 = Förskola
- V15 = Grundskola F-9
- V17 = Gymnasieskola
- V21 = Hemtjänst, äldre
- V23 = Särskilt boende, äldre
- V25 = LSS boende med särskild service
- V26 = LSS daglig verksamhet
- V29 = Gruppbostad LSS
- V30 = Servicebostad LSS
- V31 = SoL Boendestöd
- V32 = SoL Boende med särskild service
- V34 = SoL Sysselsättning
- V45 = Våld i nära relationer (dessa enheter avser bara stadsdelar i Stockholm och Göteborg)
- V60 = Fastigheter - region

```python
# Get organizational units for a municipality
units = kolada.get_organizational_units(municipality="1883")  # Karlskoga municipality
print(f"Found {len(units)} organizational units in Karlskoga")

# Get organizational units by ou_type and municipality
units_type = kolada.get_organizational_units(municipality="1883",
                                             ou_type="V11") # Förskola (pre-schools)

# Get ids for all pre-schools in Karlskoga
unit_ids = [units_type['id'] for units_type in units_type]

# Get data for all pre-schools in Karlskoga
df_ou_data = kolada.get_data_as_dataframe(
    ou_id=unit_ids,  # All preschools in Karlskoga
    kpi_id="N11732", # Number of children in pre-school
    years=2023,
    include_metadata=True
)
```