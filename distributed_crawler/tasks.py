from celery import Celery
import time
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import hashlib

# Celery setup (on 10.128.0.2)
app = Celery(
    'tasks',
    broker='redis://10.128.0.2:6379/0',
    backend='redis://10.128.0.2:6379/1',
)

# MongoDB Atlas connection
mongo_uri = (
    "mongodb+srv://omaralaa927:S3zvCY046ZHU1yyr"
    "@cluster0.e6mv0ek.mongodb.net/?retryWrites=true"
    "&w=majority&appName=Cluster0"
)
mongo = MongoClient(mongo_uri, tls=True, tlsAllowInvalidCertificates=True)
db = mongo['Crawler']  # use Crawler database

# Elasticsearch on indexer_node (10.128.0.5)
es = Elasticsearch([{
    'host': '10.128.0.5',
    'port': 9200,
    'scheme': 'http'   # <— you must include 'scheme'
}])

@app.task
def crawl_url(url, depth, politeness):
    """
    Recursively crawl `url` to `depth`, waiting `politeness` seconds
    between requests, store in Mongo and index in Elasticsearch.
    """
    def walk(u, d):
        if d < 0:
            return

        time.sleep(politeness)
        resp = requests.get(u, timeout=10)
        soup = BeautifulSoup(resp.text, 'html.parser')
        text = soup.get_text(separator='\n')

        # 1) Save to MongoDB
        db.crawled_pages.insert_one({
            'url': u,
            'text': text,
            'depth': depth - d,
            'timestamp': time.time()
        })

        # 2) Index in Elasticsearch, using a SHA‑1 hash of the URL as the doc ID
        doc_id = hashlib.sha1(u.encode('utf-8')).hexdigest()
        es.index(
            index='web_pages',
            id=doc_id,
            body={'url': u, 'text': text}
        )

        # 3) Recurse on any <a href="...">
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('http'):
                walk(href, d - 1)

    walk(url, depth)

