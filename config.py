import os

from dotenv import load_dotenv

load_dotenv()

ELASTICSEARCH_URL = 'https://search-coffeeholic001-eqkl4ejj2gewl7xmy2vkhcti4u.ap-northeast-2.es.amazonaws.com'
ELASTICSEARCH_ID = os.getenv('ELASTICSEARCH_ID')
ELASTICSEARCH_PW = os.getenv('ELASTICSEARCH_PW')
ELASTICSEARCH_AUTH = (ELASTICSEARCH_ID, ELASTICSEARCH_PW)