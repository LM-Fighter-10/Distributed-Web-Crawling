#!/usr/bin/env python3
import argparse
import time
import threading
from celery import Celery
from pymongo import MongoClient
from elasticsearch import Elasticsearch, NotFoundError

# Celery config
app = Celery('master', broker='redis://10.128.0.2:6379/0', backend='redis://10.128.0.2:6379/1')
app.conf.update(task_track_started=True)

# MongoDB connection
mongo = MongoClient(
    "mongodb+srv://omaralaa927:S3zvCY046ZHU1yyr@cluster0.e6mv0ek.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
db = mongo['Crawler']

# Elasticsearch connection
es = Elasticsearch([{'host': '10.128.0.5', 'port': 9200, 'scheme': 'http'}])

# Known crawler node IDs (as reported by Celery)
KNOWN_NODES = ['celery@crawler-node']

# -- Heartbeat Monitor --
def heartbeat_monitor(interval=60):
    while True:
        try:
            pong = app.control.ping(timeout=5.0)
            alive = [list(d.keys())[0] for d in pong]
            # update node status
            for node in KNOWN_NODES:
                db.node_status.update_one(
                    {'node': node},
                    {'$set': {
                        'active': node in alive,
                        'last_seen': time.time()
                    }},
                    upsert=True
                )
        except Exception:
            pass
        time.sleep(interval)

# -- Task Timeout & Re-queue Monitor --
def monitor_tasks(interval=300):
    from tasks import crawl_url
    while True:
        stale = list(db.task_status.find({
            'status': {'$in': ['queued', 'started']},
            'created_at': {'$lt': time.time() - 3600}
        }))
        for task in stale:
            # mark timeout
            db.task_status.update_one(
                {'_id': task['_id']},
                {'$set': {'status': 'timeout', 'finished_at': time.time()}}
            )
            # re-enqueue
            new = crawl_url.delay(task['url'], task['depth'], task['politeness'])
            db.task_status.insert_one({
                'task_id': new.id,
                'url': task['url'],
                'depth': task['depth'],
                'politeness': task['politeness'],
                'status': 'requeued',
                'created_at': time.time(),
                'origin': task['task_id']
            })
        time.sleep(interval)

# -- CLI commands --
def enqueue_crawl(url, depth, politeness):
    from tasks import crawl_url
    result = crawl_url.delay(url, depth, politeness)
    db.task_status.insert_one({
        'task_id': result.id,
        'url': url,
        'depth': depth,
        'politeness': politeness,
        'status': 'queued',
        'created_at': time.time(),
        'started_at': None,
        'finished_at': None,
        'error': None
    })
    print(f"[✔] Task queued: {url} (id={result.id})")


def do_search(keywords, mode):
    q = {"query": {"match": {"text": keywords}}}
    if mode == 'phrase':
        q = {"query": {"match_phrase": {"text": keywords}}}
    elif mode == 'boolean':
        q = {"query": {"query_string": {"default_field": "text", "query": keywords}}}
    try:
        resp = es.search(index="web_pages", body=q)
    except NotFoundError:
        print("Index not found. Have you run any crawls yet?")
        return
    hits = resp['hits']['hits']
    db.search_history.insert_one({
        'keywords': keywords,
        'results': [h['_source']['url'] for h in hits],
        'timestamp': time.time()
    })
    print(f"Found {len(hits)} results for '{keywords}':")
    for h in hits:
        print(" •", h['_source']['url'])


def show_status():
    crawled = db.crawled_pages.count_documents({})
    indexed = es.count(index='web_pages')['count']
    total_tasks = db.task_status.count_documents({})
    active_nodes = len([s for s in db.node_status.find({'active': True})])
    print("--- System Status ---")
    print(f"Pages crawled: {crawled}")
    print(f"Pages indexed: {indexed}")
    print(f"Total tasks: {total_tasks}")
    print(f"Active crawlers: {active_nodes}")


if __name__ == '__main__':
    p = argparse.ArgumentParser(prog='master_node.py')
    sub = p.add_subparsers(dest='cmd', required=True)

    c1 = sub.add_parser('crawl', help='Enqueue a crawl')
    c1.add_argument('-u','--url', required=True)
    c1.add_argument('-d','--depth', type=int, default=1)
    c1.add_argument('-p','--politeness', type=float, default=1.0)

    c2 = sub.add_parser('search', help='Keyword search')
    c2.add_argument('-k','--keywords', required=True)
    c2.add_argument('-m','--mode',
                    choices=['match','phrase','boolean'],
                    default='match',
                    help='Search mode: simple match, exact phrase, or boolean')

    c3 = sub.add_parser('status', help='Show system status')

    c4 = sub.add_parser('monitor', help='Start monitors')

    args = p.parse_args()

    if args.cmd == 'crawl':
        enqueue_crawl(args.url, args.depth, args.politeness)
    elif args.cmd == 'search':
        do_search(args.keywords, args.mode)
    elif args.cmd == 'status':
        show_status()
    elif args.cmd == 'monitor':
        # start heartbeat & timeout monitors
        t1 = threading.Thread(target=monitor_tasks, daemon=True)
        t2 = threading.Thread(target=heartbeat_monitor, daemon=True)
        t1.start(); t2.start(); t1.join()