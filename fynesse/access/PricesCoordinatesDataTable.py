from fynesse.access import PpDataTable, PostcodeDataTable, PropertyPricesDbConnector


class PricesCoordinatesDataTable:
    _conn = None

    def __init__(self, property_prices_db: PropertyPricesDbConnector):
        self._conn = property_prices_db.get_conn()
        self._create_table()
        self._populate_table()

    def _create_table(self):
        try:
            self._conn.cursor().execute("""
                DROP TABLE IF EXISTS `prices_coordinates_data`;
            """)
            self._conn.cursor().execute("""
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
            self._conn.cursor().execute("""
                ALTER TABLE `prices_coordinates_data`
                ADD PRIMARY KEY (`db_id`);
            """)
            self._conn.cursor().execute("""
                ALTER TABLE `prices_coordinates_data`
                MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
            """)
        except Exception as e:
            print("Could not create `prices_coordinates_data` table on the database server - {}".format(e))

    def _populate_table(self):
        try:
            self._conn.cursor().execute("""
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
                SELECT pd.price, pd.data_of_transfer, pd.postcode, pd.property_type, pd.new_build_flag, pd.tenure_type, pd.locality, pd.town_city, pd.district, pd.country, pcd.country, pcd.latitude, pcd.longitude
                FROM
                    (
                        SELECT price, data_of_transfer, postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, country
                        FROM pp_data 
                    ) pd
                INNER JOIN
                    (
                        SELECT postcode, country, latitude, longitude
                        FROM postcode_data
                    ) pcd
                ON pd.postcode = pcd.postcode
            """)
        except Exception as e:
            print("Could not populate `prices_coordinates_data` table from joining pp_data and postcode_data - {}".format(e))

    def get_number_of_rows(self):
        cur = self._conn.cursor()
        cur.execute("""
            SELECT COUNT(*) as row_count
            FROM prices_coordinates_data
        """)
        return cur.fetchall()

    def create_index_on_column(self, column_name):
        index_column_name = "pcd." + column_name
        try:
            self._conn.cursor().execute(f"""
                CREATE INDEX `{index_column_name}` USING HASH
                ON `prices_coordinates_data` ({column_name})
            """)
        except Exception as e:
            print("Could not create index from column {} on table prices_coordinates_data - {}".format(column_name, e))
