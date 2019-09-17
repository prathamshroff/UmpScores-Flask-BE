import os
import pandas as pd
from DataSections.Tables import *
from DataSections.Util import simple_merge_folder, pickle
from threading import Thread

def pitcher_upload_simple_folders(pool):
	aggregate_exclusion = ['output-data/Pitcher-Stats/walk-strikeout/walkstrikeout_aggregation_{0}.csv' for year in range(2010, 2020)]
	aggregate_exclusion.append('merged.csv')
	kwargs = [
		# {
		# 	'table': pitcher_walk_strikeout,
		# 	'primary_key': 'name',
		# 	'get_season': lambda file: file.split('_')[2].split('.')[0],
		# 	'sort_key': 'season',
		# 	'exclude_files': aggregate_exclusion,
		# 	'root': 'output-data/Pitcher-Stats/walk-strikeout'
		# }
	]
	# pool.starmap(pickle, [(simple_merge_folder, arg) for arg in kwargs])