# tasks.py
from celery import Celery
from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
from elasticsearch import Elasticsearch

app = Celery('tasks', broker='redis://localhost:6379/0')
app.config_from_object('celeryconfig')

mongo = MongoClient("mongodb://localhost:27017")
db = mongo['crawler_system']
es = Elasticsearch("http://localhost:9200")

@app.task(name='tasks.crawl_url')
def crawl_url(task_id, url, depth=1):
    print(f"Crawling: {url}")
    try:
        response = requests.get(url, timeout=10, headers={"User-Agent": "ASU-CSE354-Crawler/1.0"})
        if response.status_code != 200:
            raise Exception(f"Failed with status: {response.status_code}")
        
        soup = BeautifulSoup(response.text, "html.parser")
        text = " ".join([p.get_text() for p in soup.find_all("p")])
        links = [urljoin(url, a['href']) for a in soup.find_all("a", href=True)]

        db.crawled_pages.insert_one({
            "url": url,
            "text": text,
            "html": response.text,
            "metadata": {
                "status_code": response.status_code,
                "timestamp": time.time()
            },
            "extracted_links": links
        })

        # Forward to indexer
        es.index(index="web_pages", id=url, body={"url": url, "text": text})

        db.task_status.update_one({"task_id": task_id}, {"$set": {"status": "completed"}})
    except Exception as e:
        db.task_status.update_one({"task_id": task_id}, {"$set": {"status": "failed", "error": str(e)}})
        print(f"[Error] {url} -> {e}")
