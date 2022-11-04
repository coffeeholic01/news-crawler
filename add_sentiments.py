import pdb
import json
import requests
import pandas as pd

from config import *
from transformers import pipeline

def fetch_missing_sentiments():
    body = {
        "query" : {
            "bool" : {
                "must_not": [
                    {
                        "exists": {
                            "field": "sentiment"
                        }
                    }   
                ]
            }
        } 
    }

    body = json.dumps(body)

    headers = {
        'Content-Type': 'application/json'
    }

    resp = requests.get(
        f'{ELASTICSEARCH_URL}/news/_search',
        headers=headers,
        data=body,
        auth=ELASTICSEARCH_AUTH

    )

    assert resp.status_code == 200

    results = resp.json()

    hits = results['hits']['hits']
    docs = [x['_source'] for x in hits]

    df = pd.DataFrame(docs)
    df = df[['id', 'title']]

    return df

def upload_to_server(df):
    for _, row in df.iterrows():
        body = {
            "doc": {
                "sentiment":row.label
            }
        }
        body = json.dumps(body)

        headers = {
            'Content-Type': 'application/json'
        }

        resp = requests.post(
            f"{ELASTICSEARCH_URL}/news/_update/{row['id']}",
            headers=headers,
            data=body,
            auth=ELASTICSEARCH_AUTH
        )

        assert resp.status_code == 200

if __name__ == '__main__':
    classifier = pipeline('sentiment-analysis', model='snunlp/KR-FinBert-SC')

    while True:
        df = fetch_missing_sentiments()
        if df.empty:
            break

        titles = df['title'].tolist()
        sentiments = classifier(titles)

        df_sents = pd.DataFrame(sentiments)
        df_sents = df_sents[['label']]

        df= df.join(df_sents)
        print(df)

        upload_to_server(df)    
        
    

