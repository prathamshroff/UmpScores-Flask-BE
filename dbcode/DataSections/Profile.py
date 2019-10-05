import os
import pandas as pd
from DataSections.Tables import *
from DataSections.Util import simple_merge_folder, pickle
from threading import Thread
def profile_upload_simple_folders(pool):
	"""
	Uploads simple folders within output-data/Profile. 

	Algorithm
	----------
	We define a simple folder as a folder with no sub folders, all contents are files whose filenames
	have year within that filename such that we may easily extract season column names.
	We then parallelize the upload process for all of these files
	"""
	kwargs = [
		{
			'table': profile_best_worst_park_table, 
			'root': 'output-data/Profile/best-worst park',
			'primary_key': 'name',
			'get_season': lambda file: file.split('.')[0].split('_')[-1], 
			'sort_key':'season'
		},
		{
			'table': profile_best_worst_month_table, 
			'root': 'output-data/Profile/best-worst month',
			'primary_key': 'name',
			'get_season': lambda file: file.split('.')[0].split('_')[-1], 
			'sort_key':'season'
		},
		{
			'table': profile_team_preference_table, 
			'root': 'output-data/Profile/team preference',
			'primary_key': 'name',
			'get_season': lambda file: file.split('.')[0].split('_')[-1], 
			'sort_key':'season'
		}
	]
	pool.starmap(pickle, [(simple_merge_folder, arg) for arg in kwargs])

	
