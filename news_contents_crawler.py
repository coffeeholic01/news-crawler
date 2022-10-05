import pdb
import time
import json
import boto3
import requests
import datetime as dt
import warnings
import re

from bs4 import BeautifulSoup
from config import *

warnings.simplefilter(action='ignore', category=FutureWarning)

def fetch_news_contents(msg):
    # print(msg)
    # print(msg.message_id)
    # print(msg.body)

    item = json.loads(msg.body)
    # print(item)

    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }

    resp = requests.get(item['url'], headers=headers)

    # assert resp.status_code == 200
    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, 'html.parser')

    node = soup.find("meta", {"property": "og:article:author"})
    if node:
        content = node['content']

        if '네이버 스포츠' in content:
            return None
        if 'TV연예' in content:
            return None

        tokens = content.split('|')
        publisher = tokens[0].strip()

    else:
        pdb.set_trace()

    # print(publisher)

    datestr_list, source_url = parse_media_info(soup)

    # print(datestr_list)
    # print(source_url)

    if len(datestr_list) == 1:
        created_at = parse_datestr(datestr_list[0])
        updated_at = created_at

    elif len(datestr_list) == 2:
        created_at = parse_datestr(datestr_list[0])
        updated_at = parse_datestr(datestr_list[1])

    else:
        print(item)
        pdb.set_trace()

    # print(created_at)
    # print(updated_at)

    body = soup.find("div", {"id": "newsct_article"})

    assert body is not None

    body_text = body.text.strip()
    # print(body_text)

    images = body.find_all("img")
    try:
        image_urls = [x['data-src'] for x in images]
        image_urls = [re.sub(r'\?[\w=]+', '', x) for x in image_urls]   # x 안에서 정규식모양의 것을 ''로 바꿔라
        image_urls = list(set(image_urls))  # 중복 제거
    except:
        image_urls = []

    # print(image_urls)

    byline = soup.find("span", {"class": "byline_s"})
    reporter_name, reporter_email = extract_reporter(byline)

    # print(reporter_name)
    # print(reporter_email)

    entry = {
        'id': item['msg_id'],
        'title': item['title'],
        'section': 'economy',
        'naver_url': item['url'],
        'source_url': source_url,
        'image_urls': image_urls,
        'publisher': publisher,
        'created_at': created_at.isoformat(),
        'updated_at': updated_at.isoformat(),
        'reporter_name': reporter_name,
        'reporter_email': reporter_email,
        'body': body_text,
    }

    # print(entry)

    return entry

def parse_media_info(soup):
    media_info = soup.find("div", {"class": "media_end_head_info_datestamp"})

    if media_info:
        datestr_list = media_info.find_all("span", {"class": "media_end_head_info_datestamp_time"})

        link = media_info.find("a", {"class": "media_end_head_origin_link"})
        source_url = link['href'] if link else ''

        return datestr_list, source_url
    pass
    
def parse_datestr(span):
    if span.has_attr('data-date-time'):
        datestr = span['data-date-time']
    elif span.has_attr('data-modify-date-time'):
        datestr = span['data-modify-date-time']
    else:
        return None

    date = dt.datetime.fromisoformat(datestr)

    return date

def extract_reporter(byline):
    if byline is None:
        return '', ''

    byline = byline.text

    m = re.match(r'([\wㄱ-ㅎ가-힣]+)\s*(기자)?\s*\(?([\w\.]+@[\w\.]+)\)?', byline)
    if m:
        return m[1], m[3]

    m = re.match(r'([\wㄱ-ㅎ가-힣]+)\s*(기자)?', byline)
    if m:
        return m[1], ''

    print(byline)

    return '', ''

def upload_to_elastic_search(buffer):
    if len(buffer) == 0:
        return

    data = ''

    for x in buffer:
        index = {
            'index': {
                '_id': x['id']
            }
        }

        data += json.dumps(index) + '\n'
        data += json.dumps(x) + '\n'

    headers = {
        'Content-Type': 'application/json'
    }

    resp = requests.post(
        f'{ELASTICSEARCH_URL}/news/_bulk?pretty&refresh',
        headers=headers,
        data=data,
        auth=ELASTICSEARCH_AUTH
    )

    # print(resp.status_code)

    assert resp.status_code == 200


if __name__ == '__main__':
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='naver-news')

    while True:
        print('[{}] Fetching news'.format(dt.datetime.now()), end='', flush=True)

        messages = queue.receive_messages(
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )

        # print(messages)

        if len(messages) == 0:
            print('- Queue is empty. Wait for a while.')
            time.sleep(60)
            continue

        for msg in messages:
            msg.delete()

        buffer = []

        for msg in messages:
            print('.', end='', flush=True)

            entry = fetch_news_contents(msg)

            if entry:
                buffer.append(entry)

        upload_to_elastic_search(buffer)

        print('!!')

        # time.sleep(2)

