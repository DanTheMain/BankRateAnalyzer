from psycopg2_wrapper.database import Database
import itertools
import uuid
from config import DB_ACCESS_CONFIG, BANKS_TABLE_COLUMNS, RECORDS_TABLE_COLUMNS
from database.db_initialization import DbInit


class DBParser:

    def __init__(self, **kwargs):
        self._docs = kwargs.get('docs_table_name')
        self._banks = kwargs.get('banks_table_name')
        self._bank_id, self._bank_name = 'bank_id', 'bank_name'
        self._rating_guid, self._rating_text, self._rating, self._rating_dt = \
            'rating_guid', 'message_text', 'rating', 'date_time'
        self._db = self._get_db(**kwargs)

    @staticmethod
    def _get_db(**kwargs) -> Database:
        db_init = DbInit(**kwargs)
        return db_init.get_db()

    def _is_bank_existent(self, bank_name: str) -> bool:
        query = f"select {self._bank_id} from {self._banks} where {self._bank_name}='{bank_name}'"
        result = self._db.execute_query(query)
        if result and len(result) and len(result[0]):
            return True
        return False

    def _get_bank_id(self, bank_name: str) -> str:
        assert self._is_bank_existent(bank_name), f"Error - no id exists for name '{bank_name}'!"
        query = f"select {self._bank_id} from {self._banks} where {self._bank_name}='{bank_name}'"
        result = self._db.execute_query(query)
        result = result[0]
        assert len(result[0]) > 1, f"Error - multiple bank ids found for bank name '{bank_name}'!"
        return str(result[0])

    def get_banks_data(self) -> dict:
        query = f"select {self._bank_name}, {self._bank_id} from {self._banks}"
        return dict(self._db.execute_query(query))

    def _get_banks_ids(self) -> list:
        query = f"select {self._bank_id} from {self._banks}"
        return list(itertools.chain(*self._db.execute_query(query)))

    def _generate_new_bank_id(self) -> str:
        bid = str(uuid.uuid4())
        if bid in self._get_banks_ids():
            return self._generate_new_bank_id()
        return bid

    def _get_docs_ids(self) -> list:
        query = f"select {self._rating_guid} from {self._docs}"
        return list(itertools.chain(*self._db.execute_query(query)))

    def _generate_new_doc_id(self) -> str:
        did = str(uuid.uuid4())
        if did in self._get_docs_ids():
            return self._generate_new_doc_id()
        return did

    def add_bank(self, bank_name: str) -> str:
        assert bank_name, "Invalid or empty bank name value supplied!"
        assert not self._is_bank_existent(bank_name), f"Unable to add bank by name '{bank_name}' - it already exists"
        new_bank_id = self._generate_new_bank_id()
        query = f"insert into {self._banks}({self._bank_name}, {self._bank_id}) " \
                f"values('{bank_name}', '{new_bank_id}') returning {self._bank_id}"
        return str(self._db.execute_query(query)[0][0])

    def check_bank(self, bank_name: str) -> bool:
        return self._is_bank_existent(bank_name)

    def add_bank_document(self, bank_name: str, doc_data: dict):
        doc_data[self._bank_id] = self._get_bank_id(bank_name) \
            if self._is_bank_existent(bank_name) else self.add_bank(bank_name)
        doc_data[self._rating_guid] = self._generate_new_doc_id()
        doc_data[self._rating_text] = str(doc_data[self._rating_text]).\
            replace(",", "--?--").replace("'", "--??--").replace('"', "--???--")
        query = f"insert into {self._docs}(" \
                f"{self._rating_guid}, {self._bank_id}, {self._rating_text}, {self._rating}, {self._rating_dt}) "\
                f" values('{doc_data[self._rating_guid]}', '{doc_data[self._bank_id]}', " \
                f" '{doc_data[self._rating_text]}', '{doc_data[self._rating]}', '{doc_data[self._rating_dt]}') " \
                f"returning {self._rating_guid}"
        res = self._db.execute_query(query)
        self._restore_rating_text("--?--", ",")
        self._restore_rating_text("--??--", "'")
        self._restore_rating_text("--???--", '"')
        return res

    def _restore_rating_text(self, find_str: str, replace_with_str: str):
        query = f"""UPDATE {self._docs} SET {self._rating_text}=REPLACE({self._rating_text},'$:::$',',')"""
        res = self._db.execute_query(query)
        return res

    def get_bank_documents(self, bank_name: str) -> list:
        assert self._is_bank_existent(bank_name), f"Unable fetch documents for '{bank_name}' - no such bank found"
        bank_id = self._get_bank_id(bank_name)
        query = f"""select {self._rating_text}, {self._rating}, {self._rating_dt} from {self._docs} """ \
                f"""where {self._bank_id}='{bank_id}'"""
        docs = self._db.execute_query(query)
        return [dict(zip((self._rating_text, self._rating, self._rating_dt), doc)) for doc in docs]


if __name__ == "__main__":
    dbp = DBParser(**DB_ACCESS_CONFIG, **BANKS_TABLE_COLUMNS, **RECORDS_TABLE_COLUMNS)
    doc_data = {'message_text': "красавы либо мрази for a bank!", 'rating': '3', 'date_time': '12.12.12 12:12'}
    print(dbp.add_bank_document("sample bank 1234", doc_data))
    print(dbp.get_banks_data())
    print(dbp.get_bank_documents('sample bank 1234'))

