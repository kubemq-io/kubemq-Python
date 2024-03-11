class BaseError(Exception):
    """Base class for exceptions in this module."""
    pass


class ValidationError(BaseError):
    """Exception raised for errors in the input.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str) -> None:
        self.message = f"Validation Error: {message}"
        super().__init__(self.message)


class ConnectionError(BaseError):
    """Exception raised for errors in the input.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str) -> None:
        self.message = f"Connection Error: {message}"
        super().__init__(self.message)

class SendEventError(BaseError):
    """Exception raised for errors in the input.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str) -> None:
        self.message = f"Send Event Error: {message}"
        super().__init__(self.message)

class DeleteChannelError(BaseError):
    def __init__(self, message: str) -> None:
        self.message = f"Delete Channel Error: {message}"
        super().__init__(self.message)
class CreateChannelError(BaseError):
    def __init__(self, message: str) -> None:
        self.message = f"Create Channel Error: {message}"
        super().__init__(self.message)
class GRPCError(Exception):
    def __init__(self, exc):
        # Initialize the base exception message in case neither status nor details are found
        self.message = str(exc)

        # Check if the exception has 'code' (status) and 'details' methods
        if hasattr(exc, 'code') and callable(exc.code) and hasattr(exc, 'details') and callable(exc.details):
            status = exc.code()
            details = exc.details()

            # Ensure that status and details are not None
            if status is not None and details is not None:
                self.message = f"KubeMQ Connection Error - Status: {status} Details: {details}"

        super().__init__(self.message)