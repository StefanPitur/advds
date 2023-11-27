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

# Create property_prices database connection on remote SQL server


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


# pp_data code


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


# postcode_data code


def create_and_populate_postcode_data_table(conn):
    create_postcode_data_table(conn)
    populate_postcode_data_table(conn)


def create_postcode_data_table(conn):
    conn.cursor().execute("""
        -- Table structure for table `postcode_data`
        DROP TABLE IF EXISTS `postcode_data`;
    """)
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS `postcode_data` (
          `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
          `status` enum('live','terminated') NOT NULL,
          `usertype` enum('small', 'large') NOT NULL,
          `easting` int unsigned,
          `northing` int unsigned,
          `positional_quality_indicator` int NOT NULL,
          `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
          `latitude` decimal(11,8) NOT NULL,
          `longitude` decimal(10,8) NOT NULL,
          `postcode_no_space` tinytext COLLATE utf8_bin NOT NULL,
          `postcode_fixed_width_seven` varchar(7) COLLATE utf8_bin NOT NULL,
          `postcode_fixed_width_eight` varchar(8) COLLATE utf8_bin NOT NULL,
          `postcode_area` varchar(2) COLLATE utf8_bin NOT NULL,
          `postcode_district` varchar(4) COLLATE utf8_bin NOT NULL,
          `postcode_sector` varchar(6) COLLATE utf8_bin NOT NULL,
          `outcode` varchar(4) COLLATE utf8_bin NOT NULL,
          `incode` varchar(3)  COLLATE utf8_bin NOT NULL,
          `db_id` bigint(20) unsigned NOT NULL
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
    """)
    conn.cursor().execute("""
        ALTER TABLE `postcode_data`
        ADD PRIMARY KEY (`db_id`);
    """)
    conn.cursor().execute("""
        ALTER TABLE `postcode_data`
        MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
    """)


def populate_postcode_data_table(conn):
    conn.cursor().execute("""
        LOAD DATA LOCAL INFILE 'open_postcode_geo.csv' INTO TABLE `postcode_data`
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '"'
        LINES STARTING BY '' TERMINATED BY '\n';
    """)


def create_column_index_on_postcode_data_table(conn, column_name):
    index_column_name = "pd." + column_name

    conn.cursor().execute(f"""
        DROP INDEX IF EXISTS `{index_column_name}` ON `postcode_data`
    """)
    conn.cursor().execute(f"""
        CREATE INDEX IF NOT EXISTS `{index_column_name}` USING HASH
        ON `postcode_data` ({column_name})
    """)
