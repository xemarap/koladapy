class KoladaAPIError(Exception):
    """Base exception class for Kolada API errors."""
    pass


class KoladaRateLimitError(KoladaAPIError):
    """Exception raised when API rate limit is exceeded."""
    pass


class KoladaNotFoundError(KoladaAPIError):
    """Exception raised when a requested resource is not found."""
    pass


class KoladaValidationError(KoladaAPIError):
    """Exception raised when API request validation fails."""
    pass


class KoladaDataError(KoladaAPIError):
    """Exception raised when there's an issue with the data structure."""
    pass