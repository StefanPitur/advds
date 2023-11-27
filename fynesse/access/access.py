from fynesse.config import *
import pymysql

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


def create_property_prices_database(host, port, username, password):
    conn = pymysql.connect(
        host=host,
        port=port,
        user=username,
        password=password
    )
    conn.cursor().execute("CREATE DATABASE IF NOT EXISTS `property_prices`")


def create_property_prices_db_connection(host, port, username, password):
    conn = pymysql.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        database="property_prices",
        local_infile=1
    )
    return conn


def create_and_populate_pp_data_table(conn):
    create_pp_data_table(conn)
    populate_pp_data_table(conn)


def create_pp_data_table(conn):
    conn.cursor().execute("""
        -- Table structure for table `pp_data`
        DROP TABLE IF EXISTS `pp_data`;
    """)
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS `pp_data` (
          `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
          `price` int(10) unsigned NOT NULL,
          `date_of_transfer` date NOT NULL,
          `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
          `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
          `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
          `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
          `street` tinytext COLLATE utf8_bin NOT NULL,
          `locality` tinytext COLLATE utf8_bin NOT NULL,
          `town_city` tinytext COLLATE utf8_bin NOT NULL,
          `district` tinytext COLLATE utf8_bin NOT NULL,
          `county` tinytext COLLATE utf8_bin NOT NULL,
          `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
          `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
          `db_id` bigint(20) unsigned NOT NULL
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;
    """)
    conn.cursor().execute("""
        -- Primary key for table `pp_data` 
        ALTER TABLE `pp_data`
        ADD PRIMARY KEY (`db_id`);
    """)
    conn.cursor().execute("""
        ALTER TABLE `pp_data`
        MODIFY db_id bigint(20) unsigned NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1;
    """)


def populate_pp_data_table(conn):
    conn.cursor().execute("""
        LOAD DATA LOCAL INFILE 'pp-complete.csv' INTO TABLE pp_data
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '"'
        LINES STARTING BY '' TERMINATED BY '\n';
    """)


def create_column_index_on_pp_data_table(conn, column_name):
    index_column_name = "pp." + column_name

    conn.cursor().execute(f"""
        DROP INDEX IF EXISTS `{index_column_name}` ON `pp_data`
    """)
    conn.cursor().execute(f"""
        CREATE INDEX IF NOT EXISTS `{index_column_name}` USING HASH
        ON `pp_data` ({column_name})
    """)
