# tasks.py
from celery import Celery
import time
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import hashlib
import tldextract
from urllib.parse import urlparse, urljoin, urlunparse
from robotexclusionrulesparser import RobotExclusionRulesParser
import boto3
import os

# Celery setup
app = Celery(
    'tasks',
    broker='redis://10.128.0.2:6379/0',
    backend='redis://10.128.0.2:6379/1',
)
app.conf.update(task_track_started=True)

# MongoDB URI
mongo_uri = (
    "mongodb://10.128.0.4:27017/?retryWrites=true&w=majority"
)

# Elasticsearch
es = Elasticsearch([{'host': '10.128.0.5', 'port': 9200, 'scheme': 'http'}])

# S3 client
s3 = boto3.client('s3')
bucket = os.getenv('CRAWL_BUCKET', 'my-crawl-bucket')

@app.task(bind=True)
def crawl_url(self, seed_url, depth, politeness):
    mongo = MongoClient(mongo_uri)
    db = mongo['Crawler']
    visited = set()
    robots_cache = {}
    seed_domain = tldextract.extract(seed_url).registered_domain

    try:
        db.task_status.update_one(
            {'task_id': self.request.id},
            {'$set': {'status': 'started', 'started_at': time.time()}},
            upsert=True
        )

        def process_url(u, current_depth):
            nonlocal visited, robots_cache
            if current_depth < 0:
                return
            u = normalize_url(u)
            parsed = urlparse(u)
            if not parsed.scheme or not parsed.netloc:
                return
            if tldextract.extract(u).registered_domain != seed_domain:
                return
            domain_key = f"{parsed.scheme}://{parsed.netloc}"
            if domain_key not in robots_cache:
                rerp = RobotExclusionRulesParser()
                try:
                    rerp.fetch(f"{domain_key}/robots.txt")
                except:
                    robots_cache[domain_key] = None
                else:
                    robots_cache[domain_key] = rerp
            rerp = robots_cache.get(domain_key)
            if not rerp or not rerp.is_allowed("MyCrawlerBot", u):
                return
            if u in visited:
                return
            visited.add(u)
            delay = (rerp.get_crawl_delay("MyCrawlerBot") or politeness)
            time.sleep(delay)

            try:
                resp = requests.get(u, timeout=10, verify=False)
                resp.raise_for_status()
            except Exception:
                return

            soup = BeautifulSoup(resp.text, 'html.parser')
            text = soup.get_text(separator='\n', strip=True)
            doc_id = hashlib.sha1(u.encode()).hexdigest()

            # store raw HTML + text
            db.crawled_pages.update_one(
                {'url': u},
                {'$set': {
                    'html': resp.text,
                    'text': text,
                    'depth': depth - current_depth,
                    'timestamp': time.time()
                }},
                upsert=True
            )

            # upload raw HTML to S3
            s3.put_object(Bucket=bucket, Key=f"{doc_id}.html", Body=resp.text)

            # index in Elasticsearch
            es.index(
                index='web_pages',
                id=doc_id,
                body={'url': u, 'text': text}
            )

            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                if not href or href.startswith('javascript:'):
                    continue
                absolute_url = urljoin(u, href)
                process_url(absolute_url, current_depth - 1)

        process_url(seed_url, depth)
        db.task_status.update_one(
            {'task_id': self.request.id},
            {'$set': {'status': 'completed', 'finished_at': time.time()}}
        )
    except Exception as e:
        db.task_status.update_one(
            {'task_id': self.request.id},
            {'$set': {'status': 'failed', 'error': str(e)}}
        )
        raise
    finally:
        mongo.close()