# indexer_api.py
from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch

app = Flask(__name__)
es = Elasticsearch("http://localhost:9200")

@app.route('/api/search')
def search():
    q = request.args.get('query')
    res = es.search(index="web_pages", query={"match": {"text": q}})
    return jsonify([hit["_source"] for hit in res["hits"]["hits"]])

if __name__ == '__main__':
    app.run(port=8000)
