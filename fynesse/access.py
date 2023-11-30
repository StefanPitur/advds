import pymysql
import osmnx as ox
from .config import *
import requests
import zipfile
import os

# This file accesses the data

"""
    Place commands in this file to access the data electronically. 
Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, 
both intellectual property and personal data privacy rights. Beyond the legal side also think 
about the ethical issues around this data. 
"""

# Create property_prices database connection on remote SQL server


def create_database(
        username,
        password,
        database,
        host=config["default_host_url"],
        port=config["default_port"],
):
    conn = pymysql.connect(
        host=host,
        port=port,
        user=username,
        password=password
    )
    conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS `{database}`")


def create_db_connection(
        username,
        password,
        database,
        host=config["default_host_url"],
        port=config["default_port"],
):
    conn = pymysql.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        database=database,
        local_infile=1
    )
    return conn


# pp_data code


def create_and_populate_pp_data_table(conn):
    create_pp_data_table(conn)
    pp_data_csv_path = download_pp_data()
    populate_pp_data_table(conn, pp_data_csv_path)


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
    conn.commit()


def download_pp_data():
    complete_pp_data_url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
    pp_data_csv_path = "tmp/pp-complete.csv"
    return download_file(complete_pp_data_url, pp_data_csv_path)


def populate_pp_data_table(conn, pp_data_csv_path):
    conn.cursor().execute(f"""
        LOAD DATA LOCAL INFILE '{pp_data_csv_path}' INTO TABLE pp_data
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '"'
        LINES STARTING BY '' TERMINATED BY '\n';
    """)
    conn.commit()


def create_column_index_on_pp_data_table(conn, column_name):
    index_column_name = "pp." + column_name

    conn.cursor().execute(f"""
        DROP INDEX IF EXISTS `{index_column_name}` ON `pp_data`
    """)
    conn.cursor().execute(f"""
        CREATE INDEX IF NOT EXISTS `{index_column_name}` USING HASH
        ON `pp_data` ({column_name})
    """)
    conn.commit()


# postcode_data code


def create_and_populate_postcode_data_table(conn):
    create_postcode_data_table(conn)
    postcode_data_csv_path = download_postcode_data()
    populate_postcode_data_table(conn, postcode_data_csv_path)


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
    conn.commit()


def download_postcode_data():
    postcode_data_url = "https://www.getthedata.com/downloads/open_postcode_geo.csv.zip"
    postcode_data_zip_path = "tmp/open_postcode_geo.csv.zip"
    download_file(postcode_data_url, postcode_data_zip_path)
    return unzip_file(postcode_data_zip_path)


def populate_postcode_data_table(conn, postcode_data_csv_path):
    conn.cursor().execute(f"""
        LOAD DATA LOCAL INFILE '{postcode_data_csv_path}' INTO TABLE `postcode_data`
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '"'
        LINES STARTING BY '' TERMINATED BY '\n';
    """)
    conn.commit()


def create_column_index_on_postcode_data_table(conn, column_name):
    index_column_name = "pd." + column_name

    conn.cursor().execute(f"""
        DROP INDEX IF EXISTS `{index_column_name}` ON `postcode_data`
    """)
    conn.cursor().execute(f"""
        CREATE INDEX IF NOT EXISTS `{index_column_name}` USING HASH
        ON `postcode_data` ({column_name})
    """)
    conn.commit()


# prices_coordinates_data code

def create_and_populate_prices_coordinates_data_table(conn):
    create_prices_coordinates_data_table(conn)
    populate_prices_coordinates_data_table(conn)


def create_prices_coordinates_data_table(conn):
    conn.cursor().execute("""
        DROP TABLE IF EXISTS `prices_coordinates_data`;
    """)
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS `prices_coordinates_data` (
          `price` int(10) unsigned NOT NULL,
          `date_of_transfer` date NOT NULL,
          `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
          `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
          `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `locality` tinytext COLLATE utf8_bin NOT NULL,
          `town_city` tinytext COLLATE utf8_bin NOT NULL,
          `district` tinytext COLLATE utf8_bin NOT NULL,
          `county` tinytext COLLATE utf8_bin NOT NULL,
          `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
          `latitude` decimal(11,8) NOT NULL,
          `longitude` decimal(10,8) NOT NULL,
          `db_id` bigint(20) unsigned NOT NULL
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;
    """)
    conn.cursor().execute("""
        ALTER TABLE `prices_coordinates_data`
        ADD PRIMARY KEY (`db_id`);
    """)
    conn.cursor().execute("""
        ALTER TABLE `prices_coordinates_data`
        MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
    """)
    conn.commit()


def populate_prices_coordinates_data_table(conn):
    conn.cursor().execute("""
        INSERT INTO `prices_coordinates_data` (
            `price`,
            `date_of_transfer`,
            `postcode`,
            `property_type`,
            `new_build_flag`,
            `tenure_type`,
            `locality`,
            `town_city`,
            `district`,
            `county`,
            `country`,
            `latitude`,
            `longitude`
        )
        SELECT pp.price, pp.date_of_transfer, pp.postcode, pp.property_type, pp.new_build_flag, pp.tenure_type, pp.locality, pp.town_city, pp.district, pp.county, pd.country, pd.latitude, pd.longitude
        FROM
            (
                SELECT price, date_of_transfer, postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county
                FROM pp_data 
            ) pp
        INNER JOIN
            (
                SELECT postcode, country, latitude, longitude
                FROM postcode_data
            ) pd
        ON pp.postcode = pd.postcode
    """)
    conn.commit()


def create_column_index_on_prices_coordinates_data_table(conn, column_name):
    index_column_name = "pcd." + column_name
    conn.cursor().execute(f"""
        DROP INDEX IF EXISTS `{index_column_name}` ON `prices_coordinates_data`
    """)
    conn.cursor().execute(f"""
        CREATE INDEX IF NOT EXISTS `{index_column_name}` USING HASH
        ON `prices_coordinates_data` ({column_name})
    """)
    conn.commit()


def number_of_rows_prices_coordinates_data_table(conn):
    cur = conn.cursor()
    cur.execute("""
           SELECT COUNT(*) AS row_count
           FROM `prices_coordinates_data`
        """)
    return cur.fetchall()


def get_prices_coordinates_for_coords_and_timedelta(conn, bounding_box, min_date, max_date, property_type):
    north, south, west, east = bounding_box
    cur = conn.cursor()
    cur.execute(f"""
        SELECT pp.price, pp.date_of_transfer, pp.postcode, pp.property_type, pp.new_build_flag, pp.tenure_type, pp.locality, pp.town_city, pp.district, pp.county, pd.country, pd.latitude, pd.longitude
        FROM
            (
                SELECT price, date_of_transfer, postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county 
                FROM pp_data
                WHERE (date_of_transfer BETWEEN '{min_date}' AND '{max_date}') AND property_type = '{property_type}' 
            ) pp
        INNER JOIN
            (
                SELECT postcode, country, latitude, longitude
                FROM postcode_data
                WHERE (latitude BETWEEN {south} AND {north}) AND (longitude BETWEEN {west} AND {east})
            ) pd
        ON pp.postcode = pd.postcode
    """)
    return cur.fetchall()


# OpenStreetMap
def retrieve_pois_from_bbox_given_tags(bounding_box, tags=config["default_tags"]):
    north, south, west, east = bounding_box
    return ox.features_from_bbox(north, south, east, west, tags)


# Util functions
def download_file(url_source, output_file_path):
    with open(output_file_path, 'wb') as f:
        with requests.get(url_source, stream=True) as r:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    return output_file_path


def get_unzipped_file_name(zipped_file_name: str):
    return ".".join(zipped_file_name.split('.')[:-1])


def unzip_file(zipped_file_path):
    file_path = os.path.dirname(zipped_file_path)
    zipped_file_name = os.path.basename(zipped_file_path)
    unzipped_file_name = get_unzipped_file_name(zipped_file_name)

    zipfile.ZipFile(zipped_file_name, 'r').extractall(file_path)
    return file_path + "/" + unzipped_file_name
