import grpc
def decode_grpc_error(exc) -> str:
    message = str(exc)

    # Check if the exception has 'code' (status) and 'details' methods
    if hasattr(exc, 'code') and callable(exc.code) and hasattr(exc, 'details') and callable(exc.details):
        status = exc.code()
        details = exc.details()
        if status == grpc.StatusCode.CANCELLED:
            return "KubeMQ Connection Closed"
        # Ensure that status and details are not None
        if status is not None and details is not None:
            message = f"KubeMQ Connection Error - Status: {status} Details: {details}"

    return message