class QueryExecutionTimeoutException(Exception):
    pass


class InvalidS3PathException(Exception):
    pass


class QueryExecutionFailedException(Exception):
    pass


class QueryUnknownStatusException(Exception):
    pass


class QueryStillRunningException(Exception):
    pass


class NoRegionFoundError(Exception):
    pass


class DatabaseNotFound(Exception):
    pass


class QueryNotSupported(Exception):
    pass
