import os
import pandas as pd
from DataSections.Tables import *
from DataSections.Util import simple_merge_folder, pickle
from threading import Thread

# def pitcher_upload_simple_folders(pool):
# 	aggregate_exclusion = ['output-data/Pitcher-Stats/walk-strikeout/walkstrikeout_aggregation_{0}.csv' for year in range(2010, 2020)]
# 	aggregate_exclusion.append('merged.csv')
# 	kwargs = [
# 		# {
# 		# 	'table': pitcher_walk_strikeout,
# 		# 	'primary_key': 'name',
# 		# 	'get_season': lambda file: file.split('_')[2].split('.')[0],
# 		# 	'sort_key': 'season',
# 		# 	'exclude_files': aggregate_exclusion,
# 		# 	'root': 'output-data/Pitcher-Stats/walk-strikeout'
# 		# }
# 	]
# 	# pool.starmap(pickle, [(simple_merge_folder, arg) for arg in kwargs])

def upload_strikeout(pool):
	root = 'output-data/Pitcher-Stats/walk-strikeout'
	files = [os.path.join(root, 'walk_strikeout_{0}.csv'.format(year)) for year in range(2010, 2020)]
	pitcher_walk_strikeout.clear('name', sort_key = 'season')
	for file in files:
		df = pd.read_csv(file)
		if 'ump' in df.columns:
			df.rename(columns={'ump':'name'}, inplace=True)
		if 'Unnamed: 0' in df.columns:
			df.drop(columns=['Unnamed: 0'], inplace=True)
		df['name'] = df['name'].apply(lambda row: row.lower())
		df['season'] = len(df) * [file.split('_')[-1].split('.')[0]]
		df.to_csv(file)
	pool.map(pitcher_walk_strikeout.upload, files)


