from .config import *
import pymysql
from exception import DatabaseCreationException, DatabaseConnectionException

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import tables
import mongodb
import sqlite"""

# This file accesses the data

"""
    Place commands in this file to access the data electronically. 
Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, 
both intellectual property and personal data privacy rights. Beyond the legal side also think 
about the ethical issues around this data. 
"""


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


def create_and_populate_pp_data_table(property_prices_db: PropertyPricesDbConnector):
    create_pp_data_table(property_prices_db)
    conn = property_prices_db.get_conn()

    return "Calling method to generate pp_data table"


def create_pp_data_table(property_prices_db: PropertyPricesDbConnector):
    conn = property_prices_db.get_conn()

def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError

