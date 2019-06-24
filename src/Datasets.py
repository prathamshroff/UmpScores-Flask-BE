import boto3
class Dataset:
	class __impl:
		"""Singleton Instance"""
		def __init__(self, iam_role, table):
			self.iam_role = iam_role
			self.dynamodb = boto3.resource('dynamodb',
				aws_access_key_id = self.iam_role['key'], 
				aws_secret_access_key = self.iam_role['secret'],
				region_name='us-east-1'
			).Table(table)

		def get(self, key):
			"""Execute query given some dictionary

			:param key: DICT of key value pairs to query based on
			:returns: dict of document results
			"""
			return self.dynamodb.get_item(Key=key)['Item']

		def all(self):
			return self.dynamodb.scan()

	__instance = None

	def __init__(self, iam_role, table):
		""" Create singleton instance """
		if Dataset.__instance is None:
			Dataset.__instance = Dataset.__impl(iam_role, table)

		# Store instance reference as the only member in the handle
		self.__dict__['_Singleton__instance'] = Dataset.__instance

	def __getattr__(self, attr):
		return getattr(self.__instance, attr)

	def __setattr__(self, attr, value):
		return setattr(self.__instance, attr, value)
