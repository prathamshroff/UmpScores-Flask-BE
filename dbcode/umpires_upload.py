import boto3
import pandas as pd
import sys
import math
from decimal import Decimal
import botocore
import simplejson as json
import re
#TODO refresh cloudsearch by pulling all 
raw_filepath = 'datasets/raw/umpire2019.xlsx'
refined_filepath = 'datasets/refined/refined_umpire2019.xlsx'

config = eval(open('../.config.json').read())
iam = config["iam-user"]

table = boto3.resource('dynamodb',
	aws_access_key_id = iam['key'], 
	aws_secret_access_key = iam['secret'],
	region_name='us-east-1'
).Table('refrating-umpires-v1')

cloudsearch = boto3.client('cloudsearchdomain', 
	endpoint_url = config['cloud-search']['umpires-url'],
	aws_access_key_id = iam['key'], 
	aws_secret_access_key = iam['secret'],
	region_name='us-east-1'
)

cache = []

def parseRawUmpire2019():
	def fixEmptyString(item):
		if type(item) == float:
			if math.isnan(item):
				return "n/a"
		return item

	def fixEmptyNumber(item):
		if math.isnan(item):
			return -1
		return item

	string_fields = [
		'Data source',
		'ump',

	]

	number_fields = [
		'number',
		'serve year',
		'birth year',
		'All-star game',
		'Division Series',
		'Division Series',
		'League Championship Series',
		'World Series',
		'Wild Card Game',
		'M.S (Y/N)',
		'B.S (Y/N)',
		'Children',
		'Marital Status (Y/N)',
		'Umpire School/Camp Student (Y/N)',
		'Umpire School/Camp Instructor (Y/N)',
		'Other Leagues'
	]

	dataset = pd.read_excel(raw_filepath, index=False)
	for field in number_fields:
		dataset[field] = dataset[field].apply(fixEmptyNumber)

	for field in string_fields:
		dataset[field] = dataset[field].apply(fixEmptyString)

	dataset.to_excel(refined_filepath)

def uploadUmpires():
	df = pd.read_excel(refined_filepath, keep_default_na=False, index=False)
	data = df.to_dict()
	keys = [key for key in data]
	with table.batch_writer() as batch:
		for item_id in range(len(data[keys[0]])):
			item = {key: data[key][item_id] for key in keys}
			for key in keys:
				if type(item[key]) == float:
					item[key] = Decimal(item[key])
			try:
				if 'Unnamed: 0' in item:
					item.pop('Unnamed: 0')
				cache.append(item)
				batch.put_item(
					Item=item
				)
			except botocore.exceptions.ClientError as e:
				print("Error couldn't upload the following row: \n", item)
				exit(0)
	print('Dynamodb table for umpires refreshed')

def clearTable():
	scan = table.scan()
	with table.batch_writer() as batch:
		for each in scan['Items']:
			batch.delete_item(
				Key={
					'ump': each['ump'],
					'number': each['number']
				}
			)
	print('Dynamodb table for umpires emtpied')

def emptyCloudSearch():
	data = cloudsearch.search(
		query='matchall',
		queryParser='structured',
		size=10000,
		returnFields='_all_fields'
	)
	items = data['hits']['hit']
	deletion_list = []
	if len(items) == 0:
		print('CloudSearch currently empty')
		return
	for i in range(len(items)):
		item_id = items[i].pop('id')
		deletion_list.append({
			'type': 'delete',
			'id': item_id
		})
	file_obj = json.dumps(deletion_list, use_decimal=True)
	print(deletion_list)
	cloudsearch.upload_documents(
		documents = file_obj,
		contentType = 'application/json'
	)
	print('CloudSearch for umpires emptied')

def refreshCloudSearch():
	local_cache = []
	for item in cache:
		item = {re.sub('[/().\s-]', '_', key): item[key] for key in item}
		ump = re.sub('[/().\s-]', '_', item['ump'])
		new_item = {
			'type': 'add', 
			'id': '{0}_{1}'.format(ump, item['number']), 
			'fields': {key.lower(): item[key] for key in item}
		}
		local_cache.append(new_item)
	data = json.dumps(local_cache, use_decimal=True)
	cloudsearch.upload_documents(
		documents = data,
		contentType = 'application/json'
	)
	print('CloudSearch for Umpires refreshed')

if __name__ == '__main__':
	parseRawUmpire2019()
	clearTable()
	uploadUmpires()
	emptyCloudSearch()
	refreshCloudSearch()

