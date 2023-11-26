import pymysql
from fynesse.exception import DatabaseCreationException, DatabaseConnectionException


class PropertyPricesDbConnector:
    _conn = None

    def __init__(self, host, port, username, password):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = "property_prices"

        try:
            self._create_database()
            self._create_connection_to_database()
        except DatabaseCreationException as database_creation_exception:
            raise Exception(database_creation_exception)
        except DatabaseConnectionException as database_connection_exception:
            raise Exception(database_connection_exception)

    def _create_database(self):
        try:
            conn = pymysql.connect(
                host=self._host,
                port=self._port,
                user=self._username,
                password=self._password
            )
            conn.cursor().execute("CREATE DATABASE {}".format(self.database))
        except Exception:
            raise DatabaseCreationException("Could not create a new database at the give server!")

    def _create_connection_to_database(self):
        """
            Create connection to the MariaDB instance on AWS by using the host url, port, credentials and database name.
        """
        try:
            self._conn = pymysql.connect(
                host=self._host,
                port=self._port,
                user=self._username,
                password=self._password,
                database=self._database,
                local_infile=1
            )
        except Exception:
            raise DatabaseConnectionException("Could not establish connection to the database server")

    def get_conn(self):
        return self._conn
