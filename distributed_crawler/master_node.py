#!/usr/bin/env python3

import argparse
import time
import threading
from celery import Celery
from pymongo import MongoClient
from elasticsearch import Elasticsearch, NotFoundError

# -------------------
# Celery config
# -------------------
app = Celery(
    'master',
    broker='redis://10.128.0.2:6379/0',
    backend='redis://10.128.0.2:6379/1'
)
app.conf.update(task_track_started=True)

# -------------------
# MongoDB (Atlas) connection
# -------------------
MONGO_URI = (
    "mongodb+srv://omaralaa927:S3zvCY046ZHU1yyr"
    "@cluster0.e6mv0ek.mongodb.net/?retryWrites=true"
    "&w=majority&appName=Cluster0"
)
mongo = MongoClient(MONGO_URI, tls=True, tlsAllowInvalidCertificates=True)
db = mongo['Crawler']

# -------------------
# Elasticsearch (Indexer node)
# -------------------
es = Elasticsearch([
    {'host': '10.128.0.5', 'port': 9200, 'scheme': 'http'}
])

# -------------------
# Heartbeat Monitor
# -------------------
def heartbeat_monitor(interval=10):
    while True:
        now = time.time()
        # 1. Crawler worker heartbeats via Celery ping
        try:
            pong = app.control.ping(timeout=5.0)
            alive_workers = {list(d.keys())[0] for d in pong}
            # Mark alive crawler nodes
            for node in alive_workers:
                db.node_status.update_one(
                    {'node': node},
                    {'$set': {'active': True, 'last_seen': now}},
                    upsert=True
                )
            # Mark any other nodes inactive
            db.node_status.update_many(
                {'node': {'$nin': list(alive_workers)}},
                {'$set': {'active': False, 'last_seen': now}}
            )
        except Exception:
            pass

        # 2. Indexer node heartbeat via Elasticsearch ping
        try:
            alive_indexer = es.ping()
            db.node_status.update_one(
                {'node': 'elasticsearch-node'},
                {'$set': {'active': alive_indexer, 'last_seen': now}},
                upsert=True
            )
        except Exception:
            db.node_status.update_one(
                {'node': 'elasticsearch-node'},
                {'$set': {'active': False, 'last_seen': now}},
                upsert=True
            )

        time.sleep(interval)

# -------------------
# Task Timeout & Re-queue Monitor
# -------------------
def monitor_tasks(interval=300):
    from tasks import crawl_url

    while True:
        now = time.time()
        stale = list(db.task_status.find({
            'status': {'$in': ['queued', 'started']},
            'created_at': {'$lt': now - 3600}
        }))
        for task in stale:
            # mark timeout
            db.task_status.update_one(
                {'_id': task['_id']},
                {'$set': {'status': 'timeout', 'finished_at': now}}
            )
            # re-enqueue
            new = crawl_url.delay(task['url'], task['depth'], task['politeness'])
            db.task_status.insert_one({
                'task_id': new.id,
                'url': task['url'],
                'depth': task['depth'],
                'politeness': task['politeness'],
                'status': 'requeued',
                'created_at': now,
                'origin': task['task_id']
            })
            print(f"[!] Task {task['task_id']} timed out → requeued as {new.id}")
        time.sleep(interval)

# -------------------
# CLI: Enqueue Crawl
# -------------------
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

# -------------------
# CLI: Search
# -------------------
def do_search(keywords, mode, size):
    # build query
    if mode == 'phrase':
        q = {"query": {"match_phrase": {"text": keywords}}}
    elif mode == 'boolean':
        q = {
            "query": {
                "query_string": {
                    "default_field": "text",
                    "query": keywords
                }
            }
        }
    else:  # 'match'
        q = {"query": {"match": {"text": keywords}}}

    try:
        resp = es.search(index="web_pages", body=q, size=size)
    except NotFoundError:
        print("Index not found. Have you run any crawls yet?")
        return

    hits = resp['hits']['hits']
    # record search history
    db.search_history.insert_one({
        'keywords': keywords,
        'mode': mode,
        'size': size,
        'results': [h['_source']['url'] for h in hits],
        'timestamp': time.time()
    })

    print(f"Found {len(hits)} results for '{keywords}' (mode={mode}, size={size}):")
    for h in hits:
        print(" •", h['_source']['url'])

# -------------------
# CLI: Status
# -------------------
def show_status():
    crawled = db.crawled_pages.count_documents({})
    try:
        indexed = es.count(index='web_pages')['count']
    except Exception:
        indexed = 0
    total_tasks = db.task_status.count_documents({})
    # Count active crawler nodes (exclude indexer-node)
    active_crawlers = db.node_status.count_documents({
        'node': {'$ne': 'elasticsearch-node'},
        'active': True
    })
    indexer_status = db.node_status.find_one({'node': 'elasticsearch-node'})

    print("--- System Status ---")
    print(f"Pages crawled: {crawled}")
    print(f"Pages indexed: {indexed}")
    print(f"Total tasks: {total_tasks}")
    print(f"Active crawlers: {active_crawlers}")
    if indexer_status:
        state = "active" if indexer_status['active'] else "inactive"
        last = time.ctime(indexer_status['last_seen'])
        print(f"Indexer node is {state} (last seen {last})")
    else:
        print("Indexer node status: unknown")

# -------------------
# Main CLI handler
# -------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='master_node.py')
    subs = parser.add_subparsers(dest='cmd', required=True)

    # crawl
    p1 = subs.add_parser('crawl', help='Enqueue a crawl')
    p1.add_argument('-u', '--url', required=True)
    p1.add_argument('-d', '--depth', type=int, default=1)
    p1.add_argument('-p', '--politeness', type=float, default=1.0)

    # search
    p2 = subs.add_parser('search', help='Keyword search')
    p2.add_argument('-k', '--keywords', required=True)
    p2.add_argument(
        '-m', '--mode',
        choices=['match', 'phrase', 'boolean'],
        default='match',
        help='Search mode: simple match, exact phrase, or boolean'
    )
    p2.add_argument(
        '-n', '--size', type=int, default=10,
        help='How many results to return (default=10)'
    )

    # status
    subs.add_parser('status', help='Show system status')

    # monitor
    subs.add_parser('monitor', help='Start heartbeat & timeout monitors')

    args = parser.parse_args()

    if args.cmd == 'crawl':
        enqueue_crawl(args.url, args.depth, args.politeness)
    elif args.cmd == 'search':
        do_search(args.keywords, args.mode, args.size)
    elif args.cmd == 'status':
        show_status()
    elif args.cmd == 'monitor':
        t1 = threading.Thread(target=monitor_tasks, daemon=True)
        t2 = threading.Thread(target=heartbeat_monitor, daemon=True)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
