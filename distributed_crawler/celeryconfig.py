# celeryconfig.py

from kombu import Exchange, Queue

# Redis broker/results on your crawler VM
broker_url = 'redis://10.128.0.2:6379/0'
result_backend = 'redis://10.128.0.2:6379/1'

# single queue for crawling jobs
crawl_exchange = Exchange('crawl', type='direct')
task_queues = (
    Queue('crawl_tasks', crawl_exchange, routing_key='crawl.url'),
)

# route our crawl_url task into that queue
task_routes = {
    'tasks.crawl_url': {'queue': 'crawl_tasks', 'routing_key': 'crawl.url'},
}
