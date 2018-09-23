from elasticsearch import Elasticsearch
from flask import Flask, json, request, redirect, make_response, jsonify
from utility import db_operations
import time
import os, base64, re, logging
import certifi
import config

try:
    from urllib.parse import urlparse  # Python 3
except ImportError:
    from urlparse import urlparse  # Python 2

logging.basicConfig(level=logging.INFO)

# Connect to elasticsearch over SSL using auth for best security:
es_header = [{
  'host': config.BONSAI_URL,
  'port': config.ES_PORT,
  'use_ssl': True,
  'ca_certs':certifi.where(),
  'http_auth': config.BONSAI_KEYS
}]

#Creating instance of Flask and Elasticsearch DB operations
app = Flask(__name__)

db_operations = db_operations.DBOperations(es_header,config.ES_INDEX,config.DOC_TYPE)
pattern = '^\w+$'

#Basic instructions on how to use url-shortening service
@app.route('/')
def index():
	return "There are two functionalities: \n 1. To get a shorten url there is a post request to this page in which there are three parameters (in form format): url,custom_hash(optional) and ttl(in ms)(optional) \n 2.Get request for shortened url."

#In POST method checking basic validation of parameters and displaying proper errors. And returning shortened url as a result after inserting the record in elasticsearch
@app.route('/', methods=['POST'])
def shorten_url():
	original_url = request.form.get('url',"")
	if(original_url == ""):
		return("Url can't be empty")
	if urlparse(original_url).scheme == '':
		url = 'http://' + original_url
	else:
		url = original_url
	custom_hash = request.form.get('custom_hash',"")
	if(re.match(pattern,custom_hash) == False):
		return("Custom url should contain alphanumeric characters")
	url_hash = db_operations.shorten(url,custom_hash)
	if(url_hash is None):
		return("This url already exists")
	ttl = request.form.get('ttl',None)
	if(ttl is None):
		ttl = config.DEFAULT_TTL
	current_milli_time = lambda: int(round(time.time() * 1000))
	db_operations.insert(url_hash,{
	'long_url': url,
	'url_hash': url_hash,
	'timestamp': current_milli_time(),
	'ttl': int(ttl)
	})
	return ("Your URL is shortened to %s%s"% (config.HOST_URL,str(url_hash)))

#In GET method we get the original url from elasticsearch by searching with hash key
@app.route('/<urlHash>', methods=['GET'])
def get_link(urlHash):
	long_url = db_operations.search(urlHash)
	if(long_url is None):
		return("This url doesn't exists or has expired.")
	else:
		return redirect(long_url, code = 302)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

