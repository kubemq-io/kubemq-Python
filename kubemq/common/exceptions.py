class BaseError(Exception):
    """

    The `BaseError` class is a custom exception class that serves as the base class for all other custom exception classes in this software application.

    Attributes:
        None

    Methods:
        None

    Usage:
        The `BaseError` class should be inherited by any custom exception class that needs to be raised within the application.

    Example:

    ```
    class CustomError(BaseError):
        def __init__(self, message):
            self.message = message

    try:
        raise CustomError("This is a custom error.")
    except CustomError as e:
        print(e.message)
    ```

    """

    pass


class ValidationError(BaseError):
    """
    ValidationError Class

    Subclass of BaseError used for representing validation errors.

    Attributes:
        message (str): The error message associated with the validation error.

    Methods:
        __init__(self, message: str) -> None: Constructor method that initializes the ValidationError instance with the provided error message.

    """

    def __init__(self, message: str) -> None:
        self.message = f"Validation Error: {message}"
        super().__init__(self.message)


class ConnectionError(BaseError):
    """
    Initializes a new instance of the ConnectionError class with the specified error message.

    Args:
        message (str): The error message.

    Returns:
        None
    """

    def __init__(self, message: str) -> None:
        self.message = f"Connection Error: {message}"
        super().__init__(self.message)


class SendEventError(BaseError):
    """
    The `SendEventError` class is a subclass of the `BaseError` class. It represents an error that occurs when sending an event.

    Attributes:
        message (str): The error message.

    Methods:
        __init__(self, message: str) -> None: Initializes a new instance of the `SendEventError` class with the specified message.

    Example usage:
        error = SendEventError("Failed to send event")
    """

    def __init__(self, message: str) -> None:
        self.message = f"Send Event Error: {message}"
        super().__init__(self.message)


class DeleteChannelError(BaseError):
    """

    DeleteChannelError

    This class represents an error that occurs when attempting to delete a channel.

    Attributes:
        message (str): An error message describing the specific cause of the error.

    Methods:
        __init__(self, message: str) -> None:
            Initializes a new instance of the DeleteChannelError class.

            Parameters:
                message (str): An error message describing the specific cause of the error.

            Returns:
                None

    """

    def __init__(self, message: str) -> None:
        self.message = f"Delete Channel Error: {message}"
        super().__init__(self.message)


class CreateChannelError(BaseError):
    """

    This class represents an error that occurs when attempting to create a channel.

    """

    def __init__(self, message: str) -> None:
        self.message = f"Create Channel Error: {message}"
        super().__init__(self.message)


class ListChannelsError(BaseError):
    """ """

    def __init__(self, message: str) -> None:
        self.message = f"List Channels Error: {message}"
        super().__init__(self.message)


class GRPCError(Exception):
    """
    A custom exception class for handling GRPC errors.

    Args:
        exc (Exception): The exception object that occurred.

    Attributes:
        message (str): The exception message.

    Raises:
        None

    """

    def __init__(self, exc):
        # Initialize the base exception message in case neither status nor details are found
        self.message = str(exc)

        # Check if the exception has 'code' (status) and 'details' methods
        if (
            hasattr(exc, "code")
            and callable(exc.code)
            and hasattr(exc, "details")
            and callable(exc.details)
        ):
            status = exc.code()
            details = exc.details()

            # Ensure that status and details are not None
            if status is not None and details is not None:
                self.message = (
                    f"KubeMQ Connection Error - Status: {status} Details: {details}"
                )

        super().__init__(self.message)
