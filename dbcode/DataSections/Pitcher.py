import os
import pandas as pd
from DataSections.Tables import *
from DataSections.Util import simple_merge_folder, pickle
from threading import Thread
def upload_strikeout(pool):
	"""
	Uploads output-data/Pitcher-Stats/walk-strikeout to dynamodb table 
	umpscores-pitcher-walk-strikeout.
	"""
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


