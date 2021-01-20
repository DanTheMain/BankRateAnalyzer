#! /usr/bin/python3
import argparse
from bank_recorder import get_bank_records, create_bank_records_file


DEFAULT_PAGE_NUM_LIMIT = 10
DEFAULT_RECORD_AS_FILE_FLAG = False

parser = argparse.ArgumentParser()
parser.add_argument('--bank_name', '-bank_name', help="short name of the targeted bank", type=str)
parser.add_argument('--page_limit', '-page_limit', help="optional limit of pages of records to parse through",
                    type=int, default=DEFAULT_PAGE_NUM_LIMIT)
parser.add_argument('--record_in_file', '-record_in_file', help="optional flag to record parse results in a file",
                    type=bool, default=DEFAULT_RECORD_AS_FILE_FLAG)

args = parser.parse_args()
bank_name, page_limit, record_as_file = args.bank_name, args.page_limit, args.record_in_file

if bank_name:
    bank_records = get_bank_records(bank_name=bank_name, records_page_limit=page_limit)
    print(f"bank '{bank_name}' has {len(bank_records)} records")
    if bank_records and record_as_file:
        if create_bank_records_file(bank_name, bank_records):
            print("file with bank records created under current directory")

