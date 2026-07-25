"""
Domain Exceptions File.
Declares standard custom logic exceptions for domain constraint violations
and rule validation errors, isolating presentation controllers from raw DB errors.
"""

class DomainException(Exception):
    """Base domain exception class from which all logic exceptions inherit."""
    pass

class NotFoundError(DomainException):
    """
    Raised when an requested database or cache resource does not exist.
    """
    def __init__(self, message: str = "Resource not found"):
        self.message = message
        super().__init__(self.message)

class OwnershipError(DomainException):
    """
    Raised when a user attempts to retrieve or mutate a resource they do not own.
    """
    def __init__(self, message: str = "Access forbidden to this resource"):
        self.message = message
        super().__init__(self.message)

class ValidationError(DomainException):
    """
    Raised when domain validation assertions or constraints fail.
    """
    def __init__(self, message: str = "Validation failed"):
        self.message = message
        super().__init__(self.message)
