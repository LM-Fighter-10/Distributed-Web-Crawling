from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
from celery import Celery

app = Celery("tasks", broker="redis://10.128.0.2:6379/0", backend="redis://10.128.0.2:6379/1")

@app.task
def crawl_url(url):
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text()
        return {'url': url, 'text': text}
    except Exception as e:
        return {'url': url, 'error': str(e)}
