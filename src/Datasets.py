import boto3
class Dataset:
	def __init__(self, iam_role, table):
		self.iam_role = iam_role
		self.dynamodb = boto3.resource('dynamodb',
			aws_access_key_id = self.iam_role['key'], 
			aws_secret_access_key = self.iam_role['secret'],
			region_name='us-east-1'
		).Table(table)

	def get(self, **kwargs):
		"""Execute query given some dictionary

		:returns: dict of document results
		"""
		return self.dynamodb.get_item(**kwargs)['Items']

	def scan(self, **kwargs):
		"""Execute query given some dictionary

		:returns: Scan through database and get all results
		"""
		return self.dynamodb.scan(**kwargs)['Items']
