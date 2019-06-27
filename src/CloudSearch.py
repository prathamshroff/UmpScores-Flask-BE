import boto3
import numpy as np
class Search:
	'''
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
		to connect to cloudsearch from aws
	'''
	def __init__(self, iam_role, search_url):
		self.iam_role = iam_role
		self.cloudsearch = boto3.client('cloudsearchdomain', 
			endpoint_url=search_url,
			aws_access_key_id = self.iam_role['key'], 
			aws_secret_access_key = self.iam_role['secret'],
			region_name='us-east-1'
		)

	def get(self, query):
		'''
		get method takes some query string and returns a json response of relevant search
		results 

		Parameters
		----------
		query : str
			the search statement which will be indexed against dynamodb. 
				e.g. "jordan baker"

		Returns
		----------
		list where left most indices contain more relevant searches
			e.g. [{
					"attr1": "val",
					"attr2": "val2",
					... 
					"_score": "0.7653421"}
			}, ...]
		
		'''
		data = self.cloudsearch.search(query=query,
			queryParser='simple',
			size=10, 
			returnFields="_score,_all_fields")
		arr = [x['fields'] for x in data['hits']['hit']]
		return arr

