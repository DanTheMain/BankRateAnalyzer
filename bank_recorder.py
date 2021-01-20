import csv
import logging

from parsers import parser_bankiru
from database import db
import config

logger = logging.getLogger('db_init_logger')
logger.setLevel(logging.INFO)


def get_bank_records(bank_name, records_page_limit):
    dbp = db.DBParser(**config.DB_ACCESS_CONFIG, **config.BANKS_TABLE_COLUMNS, **config.RECORDS_TABLE_COLUMNS)
    records = None
    log_hdr, log_result = None, None
    if dbp.check_bank(bank_name):
        log_hdr = f"Bank {bank_name} found"
        print(f"Bank {bank_name} found")
        records = dbp.get_bank_documents(bank_name)
    else:
        bp = parser_bankiru.BankRatingParser(bank_name, records_page_limit)
        records = bp.get_rating_data()
        if not records:
            logger.error(f"No records could be obtained for bank {bank_name}!")
        dbp.add_bank(bank_name)
        [dbp.add_bank_document(bank_name, record) for record in records]
        log_hdr = f"Bank {bank_name} added"
    assert len(records) == len(dbp.get_bank_documents(bank_name))

    if records is not None:
        logger.info(f"bank '{bank_name}' has {len(records)} records")
    return records


def create_bank_records_file(bank_name, records) -> bool:
    if not bank_name or not records:
        return False
    with open(str(bank_name + '.csv'), "w", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    return True
