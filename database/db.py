from psycopg2_wrapper.database import Database
import json


class DBParser:

    def __init__(self, **kwargs):
        self._db = Database(host=kwargs['host_name'],
                            database=kwargs['db_name'],
                            user=kwargs['db_user'],
                            password=kwargs['db_password'])
        if kwargs.get('export_filepath'):
            self._export_db_file = kwargs['export_filepath']
        self._bank_documents_table_name = kwargs.get('bank_documents_table') or 'banks_documents'
        self._bank_name_to_id_table_name = kwargs.get('bank_ids_table') or 'bank_ids'

    def get_bank_documents(self, bank_id: int):
        result = self._db.execute_query(f'select * from {self._bank_documents_table_name} where id={bank_id}')
        return dict(result)

    def _get_bank_id(self, bank_name: str) -> int:
        result = self._db.execute_query(f'select ID from {self._bank_name_to_id_table_name} where name={bank_name}')
        return result if result else -1

    def add_bank(self, bank_name: str) -> int:
        pass  # TODO - insert into banks

    def check_bank(self, bank_name: str) -> bool:
        pass  # TODO: check for record id
