import boto3
import re
import simplejson as json
import botocore
import numpy as np
# TODO LIST:
# Implement auto indexingwith only primary keys

class Search():
	"""
	The Search class is our wrapper around boto3's cloud search object. We will
	use it to add more robust and safe queries in the future. 

	Parameters
	----------
	iam_role : dict
		A dictionary containing the iam role for the corresponding 
		search_url. Should look as follows:
			e.g. {
				"key": iam_access_key,
				"secret": iam_secret_key 
			}
	search_url : str
		A string representing the cloudsearch search endpoint given from aws.
			e.g. "https://<cloudsearch_name>-<hash>.<region>.cloudsearch.amazonaws.com"

	Attributes
	----------
	iam_role : dict
		This is where we store iam_role
	cloudsearch : boto3.client object
		Boto3 client object for cloudsearchdomain. Uses iam_role and the search_url
		to connect to cloudsearch from aws. We use this object for all interactions
		with CloudSearch.
	"""
	def __init__(self, iam_role, search_url, domain_name):
		self.iam_role = iam_role
		self.cloudsearch = boto3.client('cloudsearchdomain',
				endpoint_url=search_url,
				aws_access_key_id=self.iam_role['key'],
				aws_secret_access_key=self.iam_role['secret'],
				region_name='us-east-1'
		)
		self.client = boto3.client('cloudsearch',
			aws_access_key_id = self.iam_role['key'],
			aws_secret_access_key = self.iam_role['secret'],
			region_name = 'us-east-1'
		)
		self.cache = []
		self.__domain_name = domain_name


	def clear(self):
		"""
		Completely deletes all items out of this cloudsearch resource
		"""
		data = self.cloudsearch.search(
			query='matchall',
			queryParser='structured',
			size=10000,
			returnFields='_all_fields'
		)
		items = data['hits']['hit']
		deletion_list = []
		if len(items) == 0:
			print('CloudSearch already empty')
			return
		for i in range(len(items)):
			# Cloudsearch returns data as a dict {id1: {data}, id2: {data}, ...}
			item_id = items[i].pop('id')
			deletion_list.append({
				'type': 'delete',
				'id': item_id
			})
		file_obj = json.dumps(deletion_list, use_decimal=True)
		self.cloudsearch.upload_documents(
			documents=file_obj,
			contentType='application/json'
		)
		print('CloudSearch for umpires emptied')

	def flush(self, primary_key = 'name', sort_key = 'data_year'):
		"""
		Updates cloudsearch with all recently uploaded corresponding dynamodb items.
		More specifically, we're effectively flushing this cloudsearch's cache and uploading
		the contents of its cache to cloudsearch

		Parameters
		----------
       primary_key : str
            string representing the primary key for this dynamodb table
        sort_key : str
            string representing the sort key for this dynamodb table. If 
            no sort key exists leave as None
		"""
		local_cache = []
		for item in self.cache:
			item = {re.sub('[/().\s-]', '_', key): item[key] for key in item}
			primary = re.sub('[/().\s-]', '_', item[primary_key])
			new_item = {
				'type': 'add',
				'id': '{0}_{1}'.format(primary, item[sort_key]),
				'fields': {
					primary_key.lower(): item[primary_key], 
					sort_key.lower(): item[sort_key]
				}
			}
			local_cache.append(new_item)
		data = json.dumps(local_cache, use_decimal=True)
		try:
			self.cloudsearch.upload_documents(
				documents=data,
				contentType='application/json'
			)
		except botocore.exceptions.ClientError as e:
			errcode = e.response['Error']['Code']
			if errcode == 'DocumentServiceException':
				resp = self.client.describe_index_fields(DomainName = self.__domain_name)['IndexFields']
				existing_fields = [field['Options']['IndexFieldName'] for field in resp]
				my_fields = [field_name for field_name in local_cache[0]['fields']]
				print('Expected keys: {0}\nFound keys: {1}'.format(existing_fields, my_fields))
				exit(1)
		self.cache = []

		print('CloudSearch for Umpires refreshed')

	def get(self, query):
		"""
		Get method takes some query string and returns a json response of relevant search
		results 

		Parameters
		----------
		query : str
				the search statement which will be indexed against dynamodb. 
						e.g. "jordan baker" 

		Returns
		----------
		list of dictionaries
				List where left most indices contain more relevant searches. If no such 
				elements exists returns empty array. One dictionary represents an 
				entire item within dynamodb. For example, if this dataset is connected 
				to the Umpires table, one dictionary would give all the statistics 
				about a single Umpire.
						e.g. [{
										"attr1": "val",
										"attr2": "val2",
										... 
										"_score": "0.7653421"}
						}, ...]

		"""
		data = self.cloudsearch.search(query=query,
									queryParser='simple',
									size=10,
									returnFields="_score,_all_fields")
		arr = [x['fields'] for x in data['hits']['hit']]
		return arr

	def put(self, item):
		"""
		Adds some data item to this cloudsearch's cache and then immediately
		flushes the cache.

		Parameters
		----------
		item : dict
			some dict object who's key schema resembles that as specified
			by this cloudsearch domain
		"""
		self.cache.append(item)
		self.flush()
