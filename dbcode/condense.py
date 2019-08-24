import pandas as pd
import sys, os
import math
import simplejson as json
sys.path.append('../src')
from CloudSearch import Search
from Datasets import Table
import base64
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
configs = eval(open('../.config.json').read())
iam = configs["iam-user"]
umpires_cloudsearch = Search(iam, configs['cloudsearch']['umpires']['url'], 
	configs['cloudsearch']['umpires']['name'])
team_stats_table = Table(iam, 'refrating-team-stats-v1', umpires_cloudsearch)

game_stats_table = Table(iam, 'refrating-game-stats-v1')
s3_client = boto3.client('s3', aws_access_key_id = iam['key'],
	aws_secret_access_key = iam['secret'])

umpire_id_lookup = Table(iam, 'refrating-umps-lookup')

def media_refresh():
	media_folder = 'ref_images'
	objects = s3_client.list_objects(Bucket=configs['media_bucket'])
	if 'Contents' in objects:
		for resp in objects['Contents']:
			s3_client.delete_object(Bucket=configs['media_bucket'], Key=resp['Key'])

	filenames = os.listdir('ref_images')
	for file in filenames:
		num = Decimal(int(file.split('.')[0]))
		keyname = umpire_id_lookup.scan(FilterExpression = Attr('id').eq(num))
		assert(len(keyname) == 1)
		lookup = keyname[0]
		with open(os.path.join('ref_images', file), 'rb') as img:
			args = {
				'ContentType': 'image/jpeg'
			}
			s3_client.upload_fileobj(img, configs['media_bucket'], 
				Key = os.path.join('umpires', lookup['name']),
				ExtraArgs = args)

def umpire_id_lookup_reset():
	umpire_id_lookup.clearTable('name', sort_key='id')
	df = pd.read_csv('name_id.csv')
	if 'ump' in df:
		df = df.rename(columns={'ump':'name'})
		df.to_csv('name_id.csv')
	umpire_id_lookup.uploadFilepath('name_id.csv')

def dataPrep():
	filepaths = [
		'output-data/Team-Stats',
		'output-data/Game-Stats'
		# 'output-data/Pitcher-Stats'
	]

	for path in filepaths:
		data = loadYear(path, ['name', 'ump', 'team', 'blindspot_pitch', 'favorite_ump',
			'most_hated_ump', 'favorite_pitcher', 'most_hated_pitcher'])

		for year in data:
			panda_files = list(data[year].keys())

			if path == 'output-data/Team-Stats':
				on = ['name', 'data_year']

			elif path == 'output-data/Game-Stats':
				on = ['game']

			merge = data[year][panda_files[0]]
			for i in range(1, len(panda_files)):
				merge = pd.merge(data[year][panda_files[i]], merge, left_on=on, 
					right_on=on)
			merge.to_csv(os.path.join(os.path.join(path, year), 'merged.csv'))

#TODO ADD NUMBER SORTKEY TO DATA
def dataUpload():
	# filepaths = 'output-data/Team-Stats'
	filepaths = [
		'output-data/Team-Stats',
		'output-data/Game-Stats'
		# 'output-data/Pitcher-Stats'
	]
	for path in filepaths:
		folders = [os.path.join(path, folder) for folder in os.listdir(path)]
		refined_files = [os.path.join(folder, 'merged.csv') for folder in folders]

		if path == 'output-data/Team-Stats':
			team_stats_table.clearTable('name', sort_key = 'data_year')
			table = team_stats_table

		elif path == 'output-data/Game-Stats':
			game_stats_table.clearTable('game')
			table = game_stats_table

		for file in refined_files:
			table.uploadFilepath(file)

def loadYear(parent_folder, string_fields):
	"""Turns all of the files 

	Parameters
	----------
	parent_folder : str
		string representing relative path to an output-data folder such as game-stats, or team-stats
			e.g. 'output-data/Game-Stats' or 'output-data/Pitcher-Stats'

	Returns
	----------
	dict
		returns a dictionary of all the pandas csv files within that folder
			e.g. {
				'2019': {
					'<parent_folder>/2019/bcr_awayteam.csv': dict,
					'<parent_folder>/2019/bcr_hometeam.csv': dict,
					...
				},
				'2018': ...
			}
			
	"""
	filepaths = {year: [os.path.join(os.path.join(parent_folder, year), file) for file in \
		os.listdir(os.path.join(parent_folder, year)) if file != 'merged.csv'] for year in os.listdir(parent_folder)}
	filedata = {year: {path: Table.fillna(pd.read_csv(path), string_fields) for \
		path in filepaths[year]} for year in filepaths}
	delete_files = []

	for year in filedata:
		for path in filedata[year]:
			commonkey = None

			if 'name' in filedata[year][path].columns:
				commonkey = 'name'
			elif 'ump' in filedata[year][path].columns:
				commonkey = 'ump'

			if commonkey == None:
				print('Empty File?', filedata[year][path].columns, path)
				delete_files += [(year, path)]
			else:
				filedata[year][path]['data_year'] = [int(year)] * len(filedata[year][path][commonkey])
				filedata[year][path] = filedata[year][path].drop(columns=['Unnamed: 0'])

	for year, path in delete_files:
		del filedata[year][path]
	return filedata

if __name__ == '__main__':
	# umpire_id_lookup_reset()
	media_refresh()
	# dataPrep()
	# dataUpload()
	# umpires_cloudsearch.emptyCloudSearch()
	# umpires_cloudsearch.refreshCloudSearch()
