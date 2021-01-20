import requests
from bs4 import BeautifulSoup
from time import sleep
from typing import Optional
import datetime
import csv
import config
import logging


class PageParser:

    def __init__(self):
        self._logger = logging.getLogger('PageParserLogger')
        self._logger.setLevel(logging.INFO)

    def _parse_item(self, item) -> dict:
        message_text = item.find('div',
                                 {'class': 'responses__item__message markup-inside-small markup-inside-small--bullet'})
        rating = item.find('span',
                           {'data-test': 'responses-rating-grade'})
        date_time_str = item.find('time',
                                  {'data-test': 'responses-datetime'})
        date_time_str = date_time_str.text.strip() or date_time_str
        date_time = None
        try:
            date_time = datetime.datetime.strptime(date_time_str, '%d.%m.%Y %H:%M')
        except ValueError as e:
            self._logger.error(f"Failed to get date time from {date_time_str}! Details: {e}")
        date_time_str = date_time.date().strftime('%d.%m.%Y') if date_time else date_time_str
        res = {'message_text': message_text.text.strip() if message_text else 'no message',
               'rating': int(rating.text.strip()) if rating else -1,
               'date_time': date_time_str}
        return res

    def find_item_match_in_source(self, item_data, source) -> list:
        soup = BeautifulSoup(source.text, features="html.parser")
        return [self._parse_item(item) for item in
                soup.find_all(item_data['item_tag'], item_data['search_match_tags'])]


class PageSourceRetriever:

    def __init__(self, header_data: Optional[dict] = None):
        self._logger = logging.getLogger('PageSourceRetrieverLogger')
        self._logger.setLevel(logging.INFO)
        self._session = requests.Session()
        if header_data:
            self._session.headers.update(header_data)
        self._default_retry_wait_sec = 1
    
    def get_source(self, url: str) -> requests.Response:
        try:
            return self._session.get(url)
        except Exception as e:
            self._logger.info(f'error occurred while fetching data from url {url}: {e} - trying again ...')
            sleep(self._default_retry_wait_sec)
            try:
                return self._session.get(url)
            except Exception as e:
                sleep(self._default_retry_wait_sec)
                self._logger.warning(f'error occurred again while fetching data from url {url}: {e}" - trying again ...')
                return self._session.get(url)


class BankRatingParser:

    def __init__(self, name: str, page_num_limit: int):
        self._bank_name = name
        self._page_range = range(1, page_num_limit + 1)
        self._rate_source_url = config.BANKIRU_URL
        self._header_data = {'user-agent': 'Mozilla/5.1', 'accept': '*/*'}
        self._rating_item_search_data = {'item_tag': 'article', 'search_match_tags': {'class': 'responses__item'}}
        
    def get_rating_data(self, page_parse_wait_sec: int = 0.5) -> list:
        p_parser = PageParser()
        sp_retriever = PageSourceRetriever(header_data=self._header_data)

        result = list()
        for i in self._page_range:
            res = p_parser.find_item_match_in_source(
                source=sp_retriever.get_source(self._rate_source_url + self._bank_name + f'?page={i}'),
                item_data=self._rating_item_search_data)
            sleep(page_parse_wait_sec)
            result += res

        return result


if __name__ == "__main__":
    brp = BankRatingParser(config.BANK_NAME, 50)
    ratings_records = brp.get_rating_data()
    if hasattr(config, 'CSV_FILE') and ratings_records:
        with open(str(config.CSV_FILE), "w", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ratings_records[0].keys())
            writer.writeheader()
            writer.writerows(ratings_records)
    print(len(ratings_records))
    [print(record) for record in list(ratings_records)]
