# master_node.py
from celery import Celery
from pymongo import MongoClient
from datetime import datetime
import uuid

app = Celery('master', broker='redis://localhost:6379/0')
app.config_from_object('celeryconfig')

MONGO_URI = "mongodb+srv://omaralaa927:S3zvCY046ZHU1yyr@cluster0.e6mv0ek.mongodb.net/Crawler?retryWrites=true&w=majority&appName=Cluster0"
mongo = MongoClient(MONGO_URI)
db = mongo['Crawler']
task_col = db['task_status']

def add_crawl_task(url, depth=1):
    task_id = str(uuid.uuid4())
    app.send_task('tasks.crawl_url', args=[task_id, url, depth])
    task_col.insert_one({
        'task_id': task_id,
        'url': url,
        'status': 'queued',
        'created_at': datetime.utcnow()
    })
    print(f"[✔] Task queued: {url}")

# Example usage
if __name__ == '__main__':
    seed_urls = [
        "https://www.example.com",
        "https://en.wikipedia.org/wiki/Web_crawler"
    ]
    for url in seed_urls:
        add_crawl_task(url, depth=2)
