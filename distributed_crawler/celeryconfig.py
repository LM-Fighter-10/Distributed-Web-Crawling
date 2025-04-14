# celeryconfig.py
from kombu import Exchange, Queue

broker_url = 'redis://10.128.0.2:6379/0'
result_backend = 'redis://10.128.0.2:6379/1'

task_queues = (
    Queue('crawl_tasks', Exchange('crawl'), routing_key='crawl.url'),
)

task_routes = {
    'tasks.crawl_url': {'queue': 'crawl_tasks', 'routing_key': 'crawl.url'},
}
