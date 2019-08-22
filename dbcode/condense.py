import pandas as pd
import sys, os
import math
import simplejson as json
sys.path.append('../src')
from CloudSearch import Search
from Datasets import Table


configs = eval(open('../.config.json').read())
iam = configs["iam-user"]
umpires_cloudsearch = Search(iam, configs['cloudsearch']['umpires']['url'], 
	configs['cloudsearch']['umpires']['name'])
team_stats_table = Table(iam, 'refrating-team-stats-v1', umpires_cloudsearch)

game_stats_table = Table(iam, 'refrating-game-stats-v1')

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
			table.uploadUmpires(file)

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
	dataPrep()
	dataUpload()
	umpires_cloudsearch.emptyCloudSearch()
	umpires_cloudsearch.refreshCloudSearch()
