from fynesse.access.access import PropertyPricesDbConnector


class PpDataTable:
    _conn = None

    def __init__(self, property_prices_db: PropertyPricesDbConnector):
        self._conn = property_prices_db.get_conn()
        self.create_table()

    def create_table(self):
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
                ALTER TABLE `pp_data`\n
                ADD PRIMARY KEY (`db_id`);
            """)
            self._conn.cursor().execute("""
                ALTER TABLE `pp_data`
                MODIFY db_id bigint(20) unsigned NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1;
            """)
        except Exception as e:
            print("Could not create `pp_data` table on the database server - {}".format(e))

    def populate_table(self):
        pass
