import boto3
class Search:
	class __impl:
		"""Singleton Instance"""
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

	__instance = None

	def __init__(self, iam_role, search_url):
		""" Create singleton instance """
		if Search.__instance is None:
			Search.__instance = Search.__impl(iam_role, search_url)

		# Store instance reference as the only member in the handle
		self.__dict__['_Singleton__instance'] = Search.__instance

	def __getattr__(self, attr):
		return getattr(self.__instance, attr)

	def __setattr__(self, attr, value):
		return setattr(self.__instance, attr, value)
