# indexer_api.py
from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch, NotFoundError

app = Flask(__name__)
es = Elasticsearch([{'host': '10.128.0.5', 'port': 9200, 'scheme': 'http'}])

# ensure index with stemming analyzer
settings = {
    'settings': {
        'analysis': {
            'analyzer': {
                'default': {
                    'type': 'standard',
                    'stopwords': '_english_',
                    'filter': ['lowercase', 'porter_stem']
                }
            }
        }
    },
    'mappings': {
        'properties': {
            'text': {'type': 'text'}
        }
    }
}
es.indices.create(index='web_pages', body=settings, ignore=400)

@app.route('/api/search')
def search():
    q = request.args.get('query')
    mode = request.args.get('mode', 'match')
    body = {'query': {'match': {'text': q}}}
    if mode == 'phrase':
        body = {'query': {'match_phrase': {'text': q}}}
    try:
        res = es.search(index='web_pages', body=body)
    except NotFoundError:
        return jsonify([]), 404
    return jsonify([hit['_source'] for hit in res['hits']['hits']])

@app.route('/api/metrics')
def metrics():
    total = es.count(index='web_pages')['count']
    return jsonify({'indexed_pages': total})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)