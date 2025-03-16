import pytest

from koladapy.exceptions import (
    KoladaAPIError,
    KoladaRateLimitError,
    KoladaNotFoundError,
    KoladaValidationError,
    KoladaDataError
)


class TestExceptions:
    """Test the exception classes."""

    def test_exception_hierarchy(self):
        """Test the exception hierarchy."""
        # All exceptions should be subclasses of KoladaAPIError
        assert issubclass(KoladaRateLimitError, KoladaAPIError)
        assert issubclass(KoladaNotFoundError, KoladaAPIError)
        assert issubclass(KoladaValidationError, KoladaAPIError)
        assert issubclass(KoladaDataError, KoladaAPIError)

    def test_api_error(self):
        """Test the KoladaAPIError class."""
        message = "General API error"
        error = KoladaAPIError(message)
        
        assert str(error) == message
        assert isinstance(error, Exception)

    def test_rate_limit_error(self):
        """Test the KoladaRateLimitError class."""
        message = "Rate limit exceeded"
        error = KoladaRateLimitError(message)
        
        assert str(error) == message
        assert isinstance(error, KoladaAPIError)

    def test_not_found_error(self):
        """Test the KoladaNotFoundError class."""
        message = "Resource not found"
        error = KoladaNotFoundError(message)
        
        assert str(error) == message
        assert isinstance(error, KoladaAPIError)

    def test_validation_error(self):
        """Test the KoladaValidationError class."""
        message = "Invalid parameter"
        error = KoladaValidationError(message)
        
        assert str(error) == message
        assert isinstance(error, KoladaAPIError)

    def test_data_error(self):
        """Test the KoladaDataError class."""
        message = "Invalid data structure"
        error = KoladaDataError(message)
        
        assert str(error) == message
        assert isinstance(error, KoladaAPIError)

    def test_catching_specific_exceptions(self):
        """Test catching specific exception types."""
        try:
            raise KoladaRateLimitError("Rate limit")
        except KoladaAPIError as e:
            assert isinstance(e, KoladaRateLimitError)
            assert str(e) == "Rate limit"
        
        try:
            raise KoladaNotFoundError("Not found")
        except KoladaAPIError as e:
            assert isinstance(e, KoladaNotFoundError)
            assert str(e) == "Not found"
            
        # Ensure specific exceptions don't catch each other
        with pytest.raises(KoladaNotFoundError):
            try:
                raise KoladaNotFoundError("Not found")
            except KoladaRateLimitError:
                pytest.fail("Should not catch NotFoundError as RateLimitError")

    def test_exception_with_extra_info(self):
        """Test creating exceptions with additional context."""
        # Test with additional context in the message
        response = {"error": "Invalid request", "code": 400}
        error_message = f"Bad request: {response}"
        error = KoladaAPIError(error_message)
        
        assert str(error) == error_message
        assert "Invalid request" in str(error)
        assert "400" in str(error)