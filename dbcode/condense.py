import pandas as pd
import sys, os
import math
import simplejson as json
sys.path.append('../src')
from CloudSearch import Search
from Datasets import Table

configs = eval(open('../.config.json').read())
iam = configs["iam-user"]
umpires_text_search = Search(iam, configs['cloudsearch']['umpires-url'])
umpires_dataset = Table(iam, 'refrating-umpires-v1', umpires_text_search)

def dataPrep():
	filepaths = [
		'output-data/Team-Stats'
		# 'output-data/Game-Stats',
		# 'output-data/Pitcher-Stats'
	]
	for path in filepaths:
		data = loadYear(path, ['name', 'ump', 'team', 'blindspot_pitch', 'favorite_ump',
			'most_hated_ump', 'favorite_pitcher', 'most_hated_pitcher'])
		for year in data:
			panda_files = list(data[year].keys())
			print(panda_files)
			merge = pd.merge(data[year][panda_files[0]], data[year][panda_files[1]], left_on='name', right_on='name')
			for i in range(2, len(panda_files)):
				merge = pd.merge(data[year][panda_files[i]], merge, left_on='name', right_on='name')
			merge.to_csv(os.path.join(os.path.join(path, year), 'merged.csv'))

#TODO ADD NUMBER SORTKEY TO DATA
def dataUpload():
	filepaths = [
		'output-data/Team-Stats'
		# 'output-data/Game-Stats',
		# 'output-data/Pitcher-Stats'
	]
	refined_files = [os.path.join(os.path.join(filepaths, folder), 'merged.csv') for folder in filepaths if \
		folder[0] == '2']
	umpires_dataset.clearTable('name', sort_key='number')
	umpires_dataset.uploadUmpires(refined_files)

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
	for year in filedata:
		for path in filedata[year]:
			filedata[year][path]['data_year'] = year
	return filedata

if __name__ == '__main__':
	dataPrep()
	dataUpload()