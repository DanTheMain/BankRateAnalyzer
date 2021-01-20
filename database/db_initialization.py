from psycopg2_wrapper.database import Database
from config import DB_ACCESS_CONFIG, BANKS_TABLE_COLUMNS, RECORDS_TABLE_COLUMNS
import logging


class DbInit:

    def __init__(self, **kwargs):
        self._logger = logging.getLogger('db_init_logger')
        self._logger.setLevel(logging.INFO)
        self._db_name = kwargs['db_name']
        self._db_owner = kwargs['db_user']
        self._banks_table_name = kwargs['banks_table_name']
        self._docs_table_name = kwargs['docs_table_name']
        self._bank_id, self._bank_name = kwargs['bank_id'], kwargs['bank_name']
        self._rating_guid, self._rating_text, self._rating, self._rating_dt = \
            kwargs['rating_guid'], kwargs['message_text'], kwargs['rating'], kwargs['date_time']
        self._db_encoding, self._db_connection_limit = 'UTF8', -1
        self._db = None
        self._init_db(**kwargs)

    def _run_query(self, query: str):
        res = self._db.execute_query(query)
        return res

    def _create_banks_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {self._banks_table_name} (" \
                f"{self._bank_id} TEXT NOT NULL, " \
                f"{self._bank_name} TEXT NOT NULL, " \
                f"PRIMARY KEY ({self._bank_id}))"
        return self._run_query(query)

    def _create_records_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {self._docs_table_name} (" \
                f"{self._rating_guid} TEXT NOT NULL, " \
                f"{self._rating_text} TEXT NOT NULL, " \
                f"{self._rating} TEXT NOT NULL, " \
                f"{self._rating_dt} TEXT NOT NULL, " \
                f"{self._bank_id} TEXT NOT NULL, " \
                f"PRIMARY KEY ({self._rating_guid}))"
        return self._run_query(query)

    def _create_tables_if_not_existing(self):
        try:
            self._create_banks_table()
            self._create_records_table()
            return True
        except Exception as e:
            self._logger.error(f"Failed to successfully create targeted tables; details: {e}")
            return False

    def _init_db(self, **kwargs):
        self._db = Database(host=kwargs['host_ip'],
                            port=kwargs['host_port'],
                            database=kwargs['db_name'],
                            user=kwargs['db_user'],
                            password=kwargs['db_password'])
        self._create_tables_if_not_existing()  # TODO: figure out db check for tables

    def get_db(self) -> Database:
        return self._db


if __name__ == "__main__":
    dbinit = DbInit(**DB_ACCESS_CONFIG, **BANKS_TABLE_COLUMNS, **RECORDS_TABLE_COLUMNS)
