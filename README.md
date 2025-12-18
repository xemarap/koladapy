
<picture align="center">
  <source media="(prefers-color-scheme: dark)" srcset="images/koladapy_dark.png">
  <img alt="Logo" src="images/koladapy_light.png">
</picture>

# KoladaPy

A comprehensive Python wrapper for the Kolada API v3, providing access to Swedish municipality data.

[![Python Versions](https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](#)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![PyPI version](https://badge.fury.io/py/koladapy.svg)](https://badge.fury.io/py/koladapy)

**Note:** This is an independent project and is not associated with Kolada.

## About Kolada

Kolada is a database containing key performance indicators (KPIs) for Swedish municipalities and regions. This wrapper provides a simple interface to search for KPIs, download data, and work with the results in Pandas DataFrames.

## Features

- Search for KPIs with flexible filtering options
- Access metadata for KPIs, municipalities, and organizational units
- Download data with customizable parameters
- Convert results to Pandas DataFrames for easy analysis
- Automatic handling of pagination, validation and rate limits
- Support for all API endpoints including data for different regions and organizational units

## Requirements

- Python 3.7 or higher
- Dependencies:
    - requests>=2.25.0
    - pandas>=1.1.0
    - tqdm>=4.50.0
    - backoff>=1.10.0

## Installation

```bash
pip install koladapy
```

## Quick Start

Visit the [tutorials](https://github.com/xemarap/koladapy/tree/main/tutorials) folder for a basic usage guide.

```python
from koladapy import KoladaAPI

# Initialize the client
kolada = KoladaAPI()

# Search for KPIs related to school
kpis = kolada.search_kpis("skola")
print(f"Found {len(kpis)} KPIs related to 'skola'")

# Get a specific KPI by ID
kpi = kolada.get_kpi("N15033")  # Pupils/teacher in primary school
print(f"KPI title: {kpi['title']}")

# Get the same data as a pandas DataFrame
df = kolada.get_data_as_dataframe(
    kpi_id="N15033", 
    municipality_id="0180", # Stockholm municipality
    years=[2020, 2021]
)
print(df.head())
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

- [Submit suggestions and report bugs](https://github.com/xemarap/koladapy/issues)
- [Open a Pull Request](https://github.com/xemarap/koladapy)
- [Star the GitHub page](https://github.com/xemarap/koladapy)

## Resources

- [Kolada API v3 documentation](https://api.kolada.se/v3/docs)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

This project uses the following open source packages:
- [requests](https://github.com/psf/requests)
- [pandas](https://github.com/pandas-dev/pandas)
- [tqdm](https://github.com/tqdm/tqdm)
- [backoff](https://github.com/litl/backoff)

The full license texts are available in the LICENSES directory.