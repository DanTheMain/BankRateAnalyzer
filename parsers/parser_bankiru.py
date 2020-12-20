import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep
from typing import Optional
import config


config.BANK_NAME


class PageParser:

    def __init__(self):
        pass

    def _parse_table(self, item):
        res = pd.DataFrame()
        message_text = item.find('div',{'class': 'responses__item__message markup-inside-small markup-inside-small--bullet'})
        rating = item.find('span',{'data-test': 'responses-rating-grade'})
        date_time = item.find('time', {'data-test': 'responses-datetime'})
        res=res.append(pd.DataFrame([[message_text.text if message_text else 'no message', rating.text if rating else 'no rating', date_time.text]], columns = ['message_text','rating', 'date_time']), ignore_index=True)
        return res

    def find_item_match_in_source(self, item_data, source):
        result = pd.DataFrame()
        soup = BeautifulSoup(source.text, features="html.parser") 
        tables=soup.find_all(item_data['item_tag'], item_data['search_match_tags'])
        for item in tables:
            res=self._parse_table(item)
            result = result.append(res)
        return result


class PageSourceRetriever:

    def __init__(self, header_data: Optional[dict] = None):
        self._session = requests.Session()
        if header_data:
            self._session.headers.update(header_data)
        self._default_retry_wait_sec = 1
    
    def get_source(self, url):
        try:
            return self._session.get(url)
        except Exception as e:
            print(f'error occurred while fetching data from url {url}: {e} - trying again ...')
            sleep(self._default_retry_wait_sec)
            return self._session.get(url)


class BankRatingParser:

    def __init__(self, name: str, page_num_limit: int, results_file: Optional[str] = None):
        self._bank_name = name
        self._page_rage = range(1, page_num_limit + 1)
        if results_file:
            self._results_file = results_file
        self._rate_source_url = config.BANKIRU_URL
        self._header_data = {'user-agent': 'Mozilla/5.1', 'accept': '*/*'}
        self._rating_item_search_data = {'item_tag': 'article', 'search_match_tags': {'class': 'responses__item'}}
        
    def get_rating_data(self, page_parse_wait_sec: int = 0.1):
            result = pd.DataFrame()

            p_parser = PageParser()
            sp_retriever = PageSourceRetriever(header_data = self._header_data)

            for i in self._page_rage:
                res = p_parser.find_item_match_in_source(
                    source=sp_retriever.get_source(self._rate_source_url + self._bank_name + f'?page={i}'),
                    item_data=self._rating_item_search_data)
                sleep(page_parse_wait_sec)
                result=result.append(res, ignore_index=True)

            if self._results_file:
                result.to_excel(self._results_file)
            
            return result
        
        

if __name__ == "__main__":
    brp = BankRatingParser(config.bank_name, 1, 'results.xlsx')
    ratings_records = brp.get_rating_data()
