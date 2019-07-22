import boto3
import re
import simplejson as json
import botocore
class CloudSearchPP():
	def __init__(self, config, iam):
		self.cloudsearch = boto3.client('cloudsearchdomain', 
			endpoint_url = config['cloud-search']['umpires-url'],
			aws_access_key_id = iam['key'], 
			aws_secret_access_key = iam['secret'],
			region_name='us-east-1'
		)
		# Cache originally empty. Cache will only retrieve stuff from corresponding
		# dynamodb table.
		self.cache = []

	def emptyCloudSearch(self):
		"""Empties all items out of this cloud search
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
			documents = file_obj,
			contentType = 'application/json'
		)
		print('CloudSearch for umpires emptied')

	def refreshCloudSearch(self):
		"""Updates cloudsearch with all recently uploaded dynamodb items
		"""
		local_cache = []
		# Once again cache is updated from 
		for item in self.cache:
			item = {re.sub('[/().\s-]', '_', key): item[key] for key in item}
			ump = re.sub('[/().\s-]', '_', item['ump'])
			new_item = {
				'type': 'add', 
				'id': '{0}_{1}'.format(ump, item['number']), 
				'fields': {key.lower(): item[key] for key in item}
			}
			local_cache.append(new_item)
		data = json.dumps(local_cache, use_decimal=True)
		self.cloudsearch.upload_documents(
			documents = data,
			contentType = 'application/json'
		)
		self.cache = []


		print('CloudSearch for Umpires refreshed')
