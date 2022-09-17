import pdb              # python debugger
import datetime as dt
import requests
import json
import boto3

from bs4 import BeautifulSoup
from urllib.parse import urlparse
from dateutil.relativedelta import relativedelta

def fetch_news_list(datestr, page):
    print(f'Fetching page {page}...', end='', flush=True)

    url = 'https://news.naver.com/main/list.naver?mode=LSD&mid=sec&sid1=101&date={}&page={}'.format(datestr, page)
    
    # print(url)

    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }


    # HTTP Method - GET, PUT, POST, DELETE, etc.
    
    resp = requests.get(url, headers=headers)
    
    # print(resp.status_code)
    # print(resp.text)

    soup = BeautifulSoup(resp.text, 'html.parser')

    # print(soup.title)

    list_body = soup.find("div", {"class": "list_body"})

    buffer = []
    
    for item in list_body.find_all("li"):
        link = item.find_all("a")[-1]

        title = link.text.strip()
        href = link['href']

        parsed_url = urlparse(href)

        msg_id = parsed_url.path.split('/')
        msg_id = 'nn-'+'-'.join(msg_id[-2:])

        # print(title)
        # print(href)
        # print(msg_id)

        body = {
            'msg_id' : msg_id,
            'title' : title,
            'url' : href
        }
        
        entry = {
            'Id' : msg_id,
            'MessageBody': json.dumps(body)
        }


        buffer.append(entry)   
        
        
    return buffer

def fetch_news_list_for_date(date):
    datestr = date.strftime('%Y%m%d')

    print('[{}] Fetching news list on {}...'.format(
            dt.datetime.now(), datestr
    ))

    last_id = None

    for page in range(1, 1000):
        buffer = fetch_news_list(datestr, page)

        if last_id == buffer[-1]['Id']:
            break
        else:
            last_id = buffer[-1]['Id']

        push_to_aws_queue(buffer)

        if len(buffer) < 20:
            break


def push_to_aws_queue(buffer):
    print('Pushing to AWS SQS')


    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='naver-news')

 
    # temp = {}
    # for x in buffer:
    #     temp[x['Id]] = x
    # buffer = list(temp.values())

    # remove duplicates
    temp = {x['Id']: x for x in buffer}
    buffer = list(temp.values())

    for idx in range(0, len(buffer), 10):
        chunk = buffer[idx:idx+10]
        queue.send_messages(Entries=chunk)


if __name__=='__main__': 
    base_date = dt.datetime(2022, 8, 1)

    for d in range(31):
        date = base_date + relativedelta(days=d)
        
        fetch_news_list_for_date(date)

