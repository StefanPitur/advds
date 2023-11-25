class DatabaseCreationException(Exception):
    def __init__(self, message):
        super().__init__(message)


class DatabaseConnectionException(Exception):
    def __init__(self, message):
        super().__init__(message)
