import boto3
import pandas as pd
import sys
import math
from decimal import Decimal
import botocore

raw_filepath = 'datasets/raw/umpire2019.xlsx'
refined_filepath = 'datasets/refined/refined_umpire2019.xlsx'

config = eval(open('../.config.json').read())
iam = config["iam-user"]

table = boto3.resource('dynamodb',
	aws_access_key_id = iam['key'], 
	aws_secret_access_key = iam['secret'],
	region_name='us-east-1'
).Table('refrating-umpires-v1')

def parse_raw_umpire2019():
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

	dataset = pd.read_excel(raw_filepath)
	for field in number_fields:
		dataset[field] = dataset[field].apply(fixEmptyNumber)

	for field in string_fields:
		dataset[field] = dataset[field].apply(fixEmptyString)

	dataset.to_excel(refined_filepath)

def upload_umpires():
	df = pd.read_excel(refined_filepath, keep_default_na=False)
	data = df.to_dict()
	keys = [key for key in data]
	with table.batch_writer() as batch:
		for item_id in range(len(data[keys[0]])):
			item = {key: data[key][item_id] for key in keys}
			for key in keys:
				if type(item[key]) == float:
					item[key] = Decimal(item[key])
			try:
				batch.put_item(
					Item=item
				)
			except botocore.exceptions.ClientError as e:
				print("Error couldn't upload the following row: \n", item)
				exit(0)

def clear_table():
	scan = table.scan()
	with table.batch_writer() as batch:
		for each in scan['Items']:
			batch.delete_item(
				Key={
					'ump': each['ump'],
					'number': each['number']
				}
			)

if __name__ == '__main__':
	parse_raw_umpire2019()
	clear_table()
	upload_umpires()

