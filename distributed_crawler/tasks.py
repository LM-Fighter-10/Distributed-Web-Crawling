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
from google.cloud import storage

# -------------------
# Celery setup
# -------------------
app = Celery(
    'tasks',
    broker='redis://10.128.0.2:6379/0',
    backend='redis://10.128.0.2:6379/1',
)
app.conf.update(task_track_started=True)

# -------------------
# MongoDB setup
# -------------------
MONGO_URI = "mongodb+srv://omaralaa927:S3zvCY046ZHU1yyr@cluster0.e6mv0ek.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

def get_mongo_client():
    # Each worker process gets its own client
    return MongoClient(MONGO_URI, tls=True, tlsAllowInvalidCertificates=True)

# -------------------
# Elasticsearch setup
# -------------------
es = Elasticsearch([
    {'host': '10.128.0.5', 'port': 9200, 'scheme': 'http'}
])

# -------------------
# URL normalization helper
# -------------------
def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.rstrip('/') or '/'
    return urlunparse((
        parsed.scheme,
        parsed.netloc.lower(),
        path,
        parsed.params,
        parsed.query,
        parsed.fragment
    ))

# -------------------
# Crawl task
# -------------------
@app.task(bind=True)
def crawl_url(self, seed_url: str, depth: int, politeness: float):
    """Crawl a site to the given depth, index pages, store raw HTML in GCS."""
    mongo = get_mongo_client()
    db = mongo['Crawler']
    visited = set()
    robots_cache = {}
    seed_domain = tldextract.extract(seed_url).registered_domain

    try:
        # Mark task queued→started
        db.task_status.update_one(
            {'task_id': self.request.id},
            {'$set': {'status': 'started', 'started_at': time.time()}},
            upsert=True
        )

        def process_url(u: str, current_depth: int):
            nonlocal visited, robots_cache

            if current_depth < 0:
                return

            # Normalize & validate
            u = normalize_url(u)
            parsed = urlparse(u)
            if not parsed.scheme or not parsed.netloc:
                return

            # Stay in same domain
            if tldextract.extract(u).registered_domain != seed_domain:
                return

            # Fetch & cache robots.txt
            domain_key = f"{parsed.scheme}://{parsed.netloc}"
            if domain_key not in robots_cache:
                rerp = RobotExclusionRulesParser()
                try:
                    rerp.fetch(f"{domain_key}/robots.txt")
                    robots_cache[domain_key] = rerp
                except Exception:
                    robots_cache[domain_key] = None
            rerp = robots_cache[domain_key]
            if not rerp or not rerp.is_allowed("MyCrawlerBot", u):
                return

            # Dedupe
            if u in visited:
                return
            visited.add(u)

            # Respect crawl-delay
            delay = (rerp.get_crawl_delay("MyCrawlerBot") or politeness)
            time.sleep(delay)

            # Fetch page
            try:
                resp = requests.get(u, timeout=10, verify=False)
                resp.raise_for_status()
            except Exception:
                return

            # Parse text
            soup = BeautifulSoup(resp.text, 'html.parser')
            text = soup.get_text(separator='\n', strip=True)

            # Persist text in Mongo
            db.crawled_pages.update_one(
                {'url': u},
                {'$set': {
                    'text': text,
                    'depth': depth - current_depth,
                    'timestamp': time.time()
                }},
                upsert=True
            )

            # Index in Elasticsearch
            doc_id = hashlib.sha1(u.encode('utf-8')).hexdigest()
            es.index(
                index='web_pages',
                id=doc_id,
                body={'url': u, 'text': text}
            )

            # **Upload raw HTML to GCS**
            gcs = storage.Client()  # uses VM's service account
            bucket = gcs.bucket('distributed-crawler')
            blob = bucket.blob(f"{doc_id}.html")
            blob.upload_from_string(resp.text, content_type='text/html')

            # Discover & recurse
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                if not href or href.startswith('javascript:'):
                    continue
                absolute = urljoin(u, href)
                process_url(absolute, current_depth - 1)

        # Kick off crawl recursion
        process_url(seed_url, depth)

        # Mark task completed
        db.task_status.update_one(
            {'task_id': self.request.id},
            {'$set': {'status': 'completed', 'finished_at': time.time()}}
        )

    except Exception as e:
        # Mark failure
        db.task_status.update_one(
            {'task_id': self.request.id},
            {'$set': {'status': 'failed', 'error': str(e)}}
        )
        raise
    finally:
        mongo.close()

