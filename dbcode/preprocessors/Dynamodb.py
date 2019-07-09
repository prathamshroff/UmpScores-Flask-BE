from preprocessors.CloudSearch import *
import boto3
import botocore
import pandas as pd
from decimal import Decimal
import simplejson as json
#TODO use simplejson for decimal if possible
class DynamodbPP():
	def __init__(self, config, iam, table_name, cloudsearchpp):
		self.__table_name = table_name
		self.table = boto3.resource('dynamodb',
			aws_access_key_id = iam['key'], 
			aws_secret_access_key = iam['secret'],
			region_name='us-east-1'
		).Table(self.__table_name)
		self.cloudsearchpp = cloudsearchpp

	def uploadUmpires(self, refined_filepath):
		"""Uploads every item within some filepath to the dynamodb table
		"""
		df = pd.read_csv(refined_filepath, keep_default_na=False)
		data = df.to_dict()
		keys = list(data.keys())
		
		with self.table.batch_writer() as batch:
			for item_id in range(len(data[keys[0]])):
				item = {key: data[key][item_id] for key in keys}
				for key in keys:
					if type(item[key]) == float or type(item[key]) == int:
						item[key] = Decimal(str(item[key]))
				try:
					self.cloudsearchpp.cache.append(item)
					batch.put_item(
						Item=item
					)
				except botocore.exceptions.ClientError as e:
					print("Error couldn't upload the following row: \n", item)
					exit(0)
				except botocore.exceptions.ParamValidationError as e:
					print("Wrong Item type: {0}".format(item))
					exit(0)

		print('Dynamodb table for {0} refreshed'.format(self.__table_name))

	def clearTable(self, primary_key, sort_key = None):
		"""Deletes every item from this dynamodb table
		"""
		scan = self.table.scan()
		with self.table.batch_writer() as batch:
			if sort_key == None:
				for each in scan['Items']:
					batch.delete_item(
						Key = {
							primary_key: each[primary_key]
					})
			else:
				for each in scan['Items']:
					batch.delete_item(
						Key = {
							primary_key: each[primary_key],
							sort_key: each[sort_key]
					})
		print('Dynamodb table for {0} emtpied'.format(self.__table_name))
