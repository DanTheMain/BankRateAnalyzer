import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep

bank_name = 'promsvyazbank'


def parse_table(item):
    res = pd.DataFrame()
    message_text = item.find('div',{'class': 'responses__item__message markup-inside-small markup-inside-small--bullet'})
    rating = item.find('span',{'data-test': 'responses-rating-grade'})
    date_time = item.find('time', {'data-test': 'responses-datetime'})
    res=res.append(pd.DataFrame([[message_text.text if message_text else 'no message', rating.text if rating else 'no rating', date_time.text]], columns = ['message_text','rating', 'date_time']), ignore_index=True)
    return res

def parse_page(s, page_url):
    result = pd.DataFrame()
    print(page_url)
    try:
        r = s.get(page_url)
    except Exception as e:
        print(f'error occurred while fetching data from url {page_url}: {e} - trying again ...')
        sleep(1)
        r = s.get(page_url)
    sleep(0.1)
    soup = BeautifulSoup(r.text, features="html.parser") 
    tables=soup.find_all('article', {'class': 'responses__item'})
    for item in tables:
        res=parse_table(item)
        result=result.append(res, ignore_index=True)
    return result

result = pd.DataFrame()
s = requests.Session()
s.headers.update({'user-agent': 'Mozilla/5.1', 'accept': '*/*'})
s.get(f'https://www.banki.ru/services/responses/bank/{bank_name}')
for i in range(1, 50):
    res = parse_page(s, f'https://www.banki.ru/services/responses/bank/{bank_name}?page={i}')
    result=result.append(res, ignore_index=True)

result.to_excel('result.xlsx')