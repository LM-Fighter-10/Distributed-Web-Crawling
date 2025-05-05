#!/usr/bin/env python3
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
MONGO_URI = (
    "mongodb+srv://omaralaa927:S3zvCY046ZHU1yyr"
    "@cluster0.e6mv0ek.mongodb.net/?retryWrites=true"
    "&w=majority&appName=Cluster0"
)
def get_mongo_client():
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
    """Canonicalize URLs (strip trailing slash, lowercase host)."""
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
# Fault-tolerant indexing task
# -------------------
@app.task(bind=True, max_retries=5, default_retry_delay=60)
def index_document(self, doc_id: str, body: dict):
    """
    Index a document into Elasticsearch with retry on failure.
    Logs persistent failures to MongoDB.index_failures.
    """
    try:
        es.index(index='web_pages', id=doc_id, body=body)
    except Exception as exc:
        mongo = get_mongo_client()
        db = mongo['Crawler']
        db.index_failures.insert_one({
            'doc_id': doc_id,
            'body': body,
            'error': str(exc),
            'retry_count': self.request.retries,
            'timestamp': time.time()
        })
        mongo.close()
        raise self.retry(exc=exc)

# -------------------
# Recursive crawl helper
# -------------------
def process_url(u: str,
                current_depth: int,
                seed_domain: str,
                politeness: float,
                visited: set,
                robots_cache: dict):
    """
    Fetch, parse, store, and index a single URL, then recurse to links.
    """
    if current_depth < 0:
        return

    # Normalize & validate
    u = normalize_url(u)
    parsed = urlparse(u)
    if not parsed.scheme or not parsed.netloc:
        return

    # Stay on seed domain
    if tldextract.extract(u).registered_domain != seed_domain:
        return

    # Robots.txt handling
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

    # Deduplicate
    if u in visited:
        return
    visited.add(u)

    # Politeness delay
    delay = rerp.get_crawl_delay("MyCrawlerBot") or politeness
    time.sleep(delay)

    # Fetch page
    try:
        resp = requests.get(u, timeout=10, verify=False)
        resp.raise_for_status()
    except Exception:
        return

    # Extract text
    soup = BeautifulSoup(resp.text, 'html.parser')
    text = soup.get_text(separator='\n', strip=True)

    # Persist to MongoDB
    mongo = get_mongo_client()
    db = mongo['Crawler']
    db.crawled_pages.update_one(
        {'url': u},
        {'$set': {
            'text': text,
            'depth': current_depth,
            'timestamp': time.time()
        }},
        upsert=True
    )

    # Generate doc_id and enqueue indexing
    doc_id = hashlib.sha1(u.encode('utf-8')).hexdigest()
    index_document.delay(doc_id, {'url': u, 'text': text})

    # Upload raw HTML to GCS
    try:
        gcs = storage.Client()
        bucket = gcs.bucket('distributed-crawler')
        blob = bucket.blob(f"{doc_id}.html")
        blob.upload_from_string(resp.text, content_type='text/html')
    except Exception as exc:
        db.index_failures.insert_one({
            'doc_id': doc_id,
            'error': f"GCS upload failed: {exc}",
            'timestamp': time.time()
        })

    mongo.close()

    # Recurse on links
    for link in soup.find_all('a', href=True):
        href = link['href'].strip()
        if not href or href.startswith('javascript:'):
            continue
        absolute = urljoin(u, href)
        process_url(absolute,
                    current_depth - 1,
                    seed_domain,
                    politeness,
                    visited,
                    robots_cache)

# -------------------
# Crawl task entrypoint
# -------------------
@app.task(bind=True)
def crawl_url(self, seed_url: str, depth: int, politeness: float):
    """
    Crawl a website from seed_url to given depth.
    Stores text in MongoDB, raw HTML in GCS, and indexes via Elasticsearch.
    """
    mongo = get_mongo_client()
    db = mongo['Crawler']

    # Mark as started
    db.task_status.update_one(
        {'task_id': self.request.id},
        {'$set': {'status': 'started', 'started_at': time.time()}},
        upsert=True
    )

    visited = set()
    robots_cache = {}
    seed_domain = tldextract.extract(seed_url).registered_domain

    # Begin recursive crawl
    process_url(seed_url, depth, seed_domain, politeness, visited, robots_cache)

    # Mark as completed
    db.task_status.update_one(
        {'task_id': self.request.id},
        {'$set': {'status': 'completed', 'finished_at': time.time()}},
        upsert=True
    )

    mongo.close()
