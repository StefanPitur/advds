from fynesse.access.access import PropertyPricesDbConnector


class PpDataTable:
    _conn = None

    def __init__(self, property_prices_db: PropertyPricesDbConnector):
        self._conn = property_prices_db.get_conn()
        self._create_table()
        self._populate_table()

    def _create_table(self):
        try:
            self._conn.cursor().execute("""
                -- Table structure for table `pp_data`
                DROP TABLE IF EXISTS `pp_data`;
            """)
            self._conn.cursor().execute("""
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
            self._conn.cursor().execute("""
                -- Primary key for table `pp_data` 
                ALTER TABLE `pp_data`
                ADD PRIMARY KEY (`db_id`);
            """)
            self._conn.cursor().execute("""
                ALTER TABLE `pp_data`
                MODIFY db_id bigint(20) unsigned NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1;
            """)
        except Exception as e:
            print("Could not create `pp_data` table on the database server - {}".format(e))

    def _populate_table(self):
        try:
            self._conn.cursor().execute("""
                LOAD DATA LOCAL INFILE 'pp-complete.csv' INTO TABLE pp_data
                FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '"'
                LINES STARTING BY '' TERMINATED BY '\n';
            """)
        except Exception as e:
            print("Could not upload data to `pp_data` table from local file - {}".format(e))

    def get_number_of_rows(self):
        cur = self._conn.cursor()
        cur.execute("""
            SELECT COUNT(*) AS row_count
            FROM pp_data
        """)
        return cur.fetchall()

    def create_index_on_column(self, column_name):
        index_column_name = "pp." + column_name
        try:
            self._conn.cursor().execute(f"""
                CREATE INDEX {index_column_name} USING HASH
                ON `pp_data` ({column_name})
            """)
        except Exception as e:
            print("Could not create index from column {} on table pp_data - {}".format(column_name, e))
