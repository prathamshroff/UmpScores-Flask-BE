import boto3
class Search:
	def __init__(self, iam_role, search_url):
		self.iam_role = iam_role
		self.cloudsearch = boto3.client('cloudsearchdomain', 
			endpoint_url=search_url,
			aws_access_key_id = self.iam_role['key'], 
			aws_secret_access_key = self.iam_role['secret'],
			region_name='us-east-1'
		)

	def get(self, query):
		"""Index query

		:param query: string to index on
		:returns: dict of document results
		"""
		return self.cloudsearch.search(query=query,
			queryParser='simple',
			size=10)
