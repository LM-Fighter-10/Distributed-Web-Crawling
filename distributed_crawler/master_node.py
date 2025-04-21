from tasks import crawl_url
from elasticsearch import Elasticsearch

# Assuming results are returned synchronously for simplicity
result = crawl_url.delay("https://www.amazon.eg")
output = result.get(timeout=30)

if 'text' in output:
    es = Elasticsearch("http://10.128.0.5:9200")
    es.index(index="web_pages", id=output['url'], body={
        "url": output['url'],
        "text": output['text']
    })
else:
    print(f"Error crawling {output['url']}: {output.get('error')}")
