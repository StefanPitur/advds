from fynesse.access.PropertyPricesDbConnector import PropertyPricesDbConnector


class PostcodeDataTable:
    _conn = None

    def __init__(self, property_prices_db: PropertyPricesDbConnector):
        self._conn = property_prices_db.get_conn()
        self._create_table()
        self._populate_table()

    def _create_table(self):
        try:
            self._conn.cursor().execute("""
                -- Table structure for table `postcode_data`
                DROP TABLE IF EXISTS `postcode_data`;
            """)
            self._conn.cursor().execute("""
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
            self._conn.cursor().execute("""
                ALTER TABLE `postcode_data`
                ADD PRIMARY KEY (`db_id`);
            """)
            self._conn.cursor().execute("""
                ALTER TABLE `postcode_data`
                MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
            """)
        except Exception as e:
            print("Could not create `postcode_data` table on the database server - {}".format(e))

    def _populate_table(self):
        try:
            self._conn.cursor().execute("""
                LOAD DATA LOCAL INFILE 'open_postcode_geo.csv' INTO TABLE `postcode_data`
                FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '"'
                LINES STARTING BY '' TERMINATED BY '\n';
            """)
        except Exception as e:
            print("Could not upload data to `postcode_data` table from local file - {}".format(e))

    def get_number_of_rows(self):
        cur = self._conn.cursor()
        cur.execute("""
            SELECT COUNT(*) as row_count
            FROM postcode_data
        """)
        return cur.fetchall()

    def create_index_on_column(self, column_name):
        index_column_name = "pd." + column_name
        try:
            self._conn.cursor().execute(f"""
                CREATE INDEX `{index_column_name}` USING HASH
                ON `postcode_data` ({column_name})
            """)
        except Exception as e:
            print("Could not create index from column {} on table postcode_data - {}".format(column_name, e))
