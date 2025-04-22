#!/usr/bin/env python3
import argparse
import time
from celery import Celery
from pymongo import MongoClient
from elasticsearch import Elasticsearch, NotFoundError

# Celery config: Redis broker/backend on your Redis VM (10.128.0.2)
app = Celery('master', broker='redis://10.128.0.2:6379/0', backend='redis://10.128.0.2:6379/1')

# MongoDB Atlas (no local Mongo here)
mongo = MongoClient(
    "mongodb+srv://omaralaa927:S3zvCY046ZHU1yyr@cluster0.e6mv0ek.mongodb.net/"
)
db = mongo['Crawler']

# Elasticsearch on indexer-node
es = Elasticsearch([
    {'host': '10.128.0.5', 'port': 9200, 'scheme': 'http'}
])

def enqueue_crawl(url, depth, politeness):
    from tasks import crawl_url
    result = crawl_url.delay(url, depth, politeness)
    db.task_status.insert_one({
        'task_id': result.id,
        'url': url,
        'depth': depth,
        'politeness': politeness,
        'status': 'queued',
        'timestamp': time.time()
    })
    print(f"[✔] Task queued: {url} (id={result.id})")

def do_search(keywords):
    q = {"query": {"match": {"text": keywords}}}
    try:
        resp = es.search(index="web_pages", body=q)
    except NotFoundError:
        print("Index not found. Have you run any crawls yet?")
        return
    hits = resp['hits']['hits']
    # record search in Mongo
    db.search_history.insert_one({
        'keywords': keywords,
        'results': [h['_source']['url'] for h in hits],
        'timestamp': time.time()
    })
    print(f"Found {len(hits)} results for '{keywords}':")
    for h in hits:
        print(" •", h['_source']['url'])

if __name__ == '__main__':
    p = argparse.ArgumentParser(prog='master_node.py')
    sub = p.add_subparsers(dest='cmd', required=True)

    c1 = sub.add_parser('crawl', help='Enqueue a crawl')
    c1.add_argument('-u','--url', required=True, help='Seed URL')
    c1.add_argument('-d','--depth', type=int, default=1, help='Crawl depth')
    c1.add_argument('-p','--politeness', type=float, default=1.0,
                    help='Delay between requests (seconds)')

    c2 = sub.add_parser('search', help='Keyword search')
    c2.add_argument('-k','--keywords', required=True, help='Search keywords')

    args = p.parse_args()
    if args.cmd == 'crawl':
        enqueue_crawl(args.url, args.depth, args.politeness)
    elif args.cmd == 'search':
        do_search(args.keywords)

