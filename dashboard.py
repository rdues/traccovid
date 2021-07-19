# -*- coding: utf-8 -*-

import sys
from elasticsearch import Elasticsearch
import tracdash

# ElasticSearch
es_ips = ['127.0.0.1']
es_index = 'tracdb'
es_cache = './escache'

# Plotly dashboard
ip = '127.0.0.1'
port = 8080
path = "/tracdash/"


es = Elasticsearch(es_ips, timeout=(60*60))

app = tracdash.prepare_app(es, es_index, cache_path=es_cache, path=path)

# for gunicorn
server = app.server


if __name__ == "__main__":
	# run plotly simple server if not using gunicorn
    app.run_server(
		debug=True, 
		dev_tools_hot_reload=False,
		host=ip,
		port=port
	)

