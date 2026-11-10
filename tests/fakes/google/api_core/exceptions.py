class GoogleAPICallError(Exception):
    pass


class PreconditionFailed(GoogleAPICallError):
    pass


class ServiceUnavailable(GoogleAPICallError):
    pass
