import random,string
from elasticsearch import Elasticsearch
import time

#This class includes basic elasticsearch queries and creation of hash for shortUrls
class DBOperations:
	def __init__(self,es_header,index,doc_type):
		self.es = Elasticsearch(es_header)
		self.es_index = index
		self.es_doc_type = doc_type

	def insert(self,url_hash,data):
		self.es.create(index = self.es_index, doc_type = self.es_doc_type, id = url_hash, body = data)

	#In this operation we check if ttl expires we delete the document and show proper error(We can retain it and mark as expired if reporting is needed, but it's not been implemented here)
	def search(self,url_hash):
		if(self.exists(url_hash)):
			data = self.es.get(index = self.es_index, doc_type = self.es_doc_type, id = url_hash)
			current_milli_time = lambda: int(round(time.time() * 1000))
			if( current_milli_time() > data['_source']['timestamp'] + data['_source']['ttl']):
				self.es.delete(index = self.es_index, doc_type = self.es_doc_type, id = url_hash)
				return None
			return data['_source']['long_url']
		else:
			return None

	def exists(self,url_hash):
		return self.es.exists(index = self.es_index, doc_type = self.es_doc_type, id = url_hash)

	#This function checks the availability of hash and returns hash wherever applicable
	def shorten(self,long_url,custom_hash):
		if(custom_hash != ""):
			if(self.exists(custom_hash)):
				return None
			else:
				return custom_hash
		else:
			letters = string.ascii_letters + string.digits
			url_hash = ''.join(random.SystemRandom().choice(letters) for n in range(7))
			if(self.exists(url_hash)):
				shorten(long_url,"")
			else:
				return url_hash


