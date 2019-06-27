import boto3
class Dataset:
	'''
	The Dataset class is our wrapper around boto3's dynamodb object. We will
	use it to add more robust and safe queries in the future. 

	Parameters
	----------
	iam_role : dict
		A dictionary containing the iam role for the corresponding 
		dynamodb table. Should look as follows:
			e.g. {
				"key": iam_access_key,
				"secret": iam_secret_key 
			}
	table : str
		A string representing the dynamodb table to use.
			e.g. "Players"

	Attributes
	----------
	iam_role : dict
		This is where we store iam_role
	dynamodb : boto3.resource object
		Boto3 resoure object for dynamodb. Uses iam_role and the table parameter
		to connect to dynamodb from aws
	'''
	def __init__(self, iam_role, table):
		self.iam_role = iam_role
		self.dynamodb = boto3.resource('dynamodb',
			aws_access_key_id = self.iam_role['key'], 
			aws_secret_access_key = self.iam_role['secret'],
			region_name='us-east-1'
		).Table(table)

	def get(self, query_map, filter_expressions=None):
		'''
		get method takes some query map and some filter options
		then returns a json response of some data item

		Parameters
		----------
		queryMap : str
			query the dynamodb table for the following key value pairs.
				e.g. {"Umpires": "jordan baker", ...}
		FilterExpressions : boto3.dynamodb.conditions.(Attr/Key)
			Filter the results given some attribute/key filter option. If None,
			FilterExpressions will not be sent.
				e.g. Attr('timeStamp').between(start, end)

		Returns
		----------
		dict
			matched item element within the database. If no such element exists returns
			empty dict
				e.g. {
					"attr1": "val1",
					"attr2": "val2",
					...
				}
		
		'''
		if not filter_expressions:
			data = self.dynamodb.get_item(Key=query_map)
		else:
			data = self.dynamodb.get_item(Key=query_map, FilterExpressions=filter_expressions)
		if 'Item' not in data:
			return {}
		return data['Item']

	def scan(self, query=None, filter_expressions=None):
		'''
		scan method takes some query keyword and some filter options
		then returns a json response of relevant search
		results 

		Parameters
		----------
		query : str
			query the dynamodb table for this following string.
				e.g. "jordan baker"
		FilterExpressions : boto3.dynamodb.conditions.(Attr/Key)
			Filter the results given some attribute/key filter option. If None,
			FilterExpressions will not be sent.
				e.g. Attr('timeStamp').between(start, end)

		Returns
		----------
		list of dictionaries
			matched item elements within the database. If no such elements exists returns
			empty dict
				e.g. [{
					"attr1": "val1",
					"attr2": "val2",
					...
				}, ...]
		
		'''
		if not filter_expressions and not query:
			data = self.dynamodb.scan()
		elif not query and filter_expressions:
			data = self.dynamodb.scan(FilterExpressions=filter_expressions)
		else:
			data = self.dynamodb.scan(query=query, FilterExpressions=filter_expressions)
		if 'Items' not in data:
			return {}
		return data['Items']
