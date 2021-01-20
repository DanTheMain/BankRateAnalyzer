BANKIRU_URL = 'https://www.banki.ru/services/responses/bank/'
BANK_NAME = 'tcs'
#CSV_FILE='bank_data.csv'

DB_ACCESS_CONFIG = {
    'host_ip': '127.0.0.1',
    'host_port': 8123,
    'db_name': '',
    'db_user': '',
    'db_password': '',
    'banks_table_name': 'bank_ids_to_names_data',
    'docs_table_name': 'banks_ratings_data'
}

BANKS_TABLE_COLUMNS = {
    'bank_id': 'bank_id',
    'bank_name': 'bank_name'
}

RECORDS_TABLE_COLUMNS = {
    'rating_guid': 'rating_guid',
    'message_text': 'message_text',
    'rating': 'rating',
    'date_time': 'date_time',
}
