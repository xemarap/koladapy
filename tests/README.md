# KoladaPy Tests

This directory contains the test suite for the KoladaPy wrapper.

## Running the Tests

To run the tests, you need to have Python 3.7+ and pytest installed.

### Setup

1. Install the required packages for testing:

```bash
pip install pytest pytest-cov responses
```

2. Install the package in development mode:

```bash
pip install -e .
```

### Running All Tests

To run all tests:

```bash
pytest
```

### Running with Coverage

To run tests with coverage:

```bash
pytest --cov=koladapy
```

To generate a coverage report:

```bash
pytest --cov=koladapy --cov-report=html
```

This will create a directory called `htmlcov` with an HTML report of the coverage.

### Running Specific Tests

To run a specific test file:

```bash
pytest tests/test_api.py
```

To run a specific test class:

```bash
pytest tests/test_api.py::TestKoladaAPI
```

To run a specific test method:

```bash
pytest tests/test_api.py::TestKoladaAPI::test_search_kpis
```

## Test Organization

The tests are organized as follows:

- `test_api.py`: Tests for the core API functionality
- `test_pagination.py`: Tests for pagination and batching functionality
- `test_network.py`: Tests for network interactions (using mock responses)
- `conftest.py`: Pytest configuration and shared fixtures

## Mock vs. Real API Calls

Most tests use mocked API responses to avoid making real network requests. However, you can also run tests against the real Kolada API by setting the environment variable `KOLADA_REAL_API=1`:

```bash
KOLADA_REAL_API=1 pytest tests/test_real_api.py
```

Note: Running tests against the real API may be rate-limited and should be used sparingly.

## CI/CD Integration

The test suite is designed to be integrated with CI/CD pipelines. See the tox.ini file for configurations that can be used with tools like GitHub Actions, Travis CI, or GitLab CI.