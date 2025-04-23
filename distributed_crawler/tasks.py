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

# Celery setup
app = Celery(
    'tasks',
    broker='redis://10.128.0.2:6379/0',
    backend='redis://10.128.0.2:6379/1',
)
app.conf.update(task_track_started=True)

# MongoDB connection (process-safe)
mongo_uri = (
    "mongodb+srv://omaralaa927:S3zvCY046ZHU1yyr"
    "@cluster0.e6mv0ek.mongodb.net/?retryWrites=true"
    "&w=majority&appName=Cluster0"
)

def get_mongo_client():
    """Create new MongoDB connection per process"""
    return MongoClient(mongo_uri, tls=True, tlsAllowInvalidCertificates=True)

# Elasticsearch connection
es = Elasticsearch([{'host': '10.128.0.5', 'port': 9200, 'scheme': 'http'}])

def normalize_url(url):
    """Standardize URL format"""
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

@app.task(bind=True)
def crawl_url(self, seed_url, depth, politeness):
    """Crawl task with full link discovery"""
    mongo = get_mongo_client()
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

            # Normalize and validate URL
            u = normalize_url(u)
            parsed = urlparse(u)
            if not parsed.scheme or not parsed.netloc:
                return

            # Check domain scope
            if tldextract.extract(u).registered_domain != seed_domain:
                return

            # Check robots.txt
            domain_key = f"{parsed.scheme}://{parsed.netloc}"
            if domain_key not in robots_cache:
                rerp = RobotExclusionRulesParser()
                try:
                    rerp.fetch(f"{domain_key}/robots.txt")
                    robots_cache[domain_key] = rerp
                except Exception as e:
                    print(f"[!] Robots.txt fetch failed for {domain_key}: {str(e)}")
                    robots_cache[domain_key] = None
                    return

            rerp = robots_cache[domain_key]
            if not rerp or not rerp.is_allowed("MyCrawlerBot", u):
                print(f"[!] Skipping {u} (disallowed)")
                return

            # Avoid duplicates
            if u in visited:
                return
            visited.add(u)

            # Respect crawl delay
            delay = (rerp.get_crawl_delay("MyCrawlerBot") or politeness)
            time.sleep(delay)

            # Fetch page
            try:
                resp = requests.get(u, timeout=10, verify=False)
                resp.raise_for_status()
            except Exception as e:
                print(f"[!] Failed to fetch {u}: {str(e)}")
                return

            # Parse content
            soup = BeautifulSoup(resp.text, 'html.parser')
            text = soup.get_text(separator='\n', strip=True)

            # Store data
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

            # Discover links
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                if not href or href.startswith('javascript:'):
                    continue

                # Convert to absolute URL
                absolute_url = urljoin(u, href)
                normalized_url = normalize_url(absolute_url)
                
                # Process recursively
                process_url(normalized_url, current_depth - 1)

        # Start crawling
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