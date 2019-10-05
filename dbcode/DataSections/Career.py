import os
import pandas as pd
from DataSections.Tables import *
from DataSections.Util import simple_merge_folder, pickle
from threading import Thread

def career_upload_simple_folders(pool):
	"""
	Uploads simple folders within output-data/Career. 

	Algorithm
	----------
	We define a simple folder as a folder with no sub folders, all contents are files whose filenames
	have year within that filename such that we may easily extract season column names.
	We then parallelize the upload process for all of these files
	"""
	kwargs = [
		{
			'table': crew_update_table, 
			'root': 'output-data/Career/crew_update',
			'primary_key': 'name',
			'get_season': lambda file: file.split('_')[3].split('.')[0], 
			'sort_key':'season',
			'exclude_files': ['merged.csv', 'ARCHIVE']
		},
		{
			'table': careers_season,
			'root': 'output-data/Career/BCR By Season/',
			'primary_key': 'name',
			'get_season': lambda file: file.split('_')[2].split('.')[0],
			'sort_key': 'data_year',
			'exclude_files': ['merged.csv', 'BCR_among_all_umps.csv']
		},
		{
			'table': career_crucial_calls_table,
			'root': 'output-data/Career/crucial calls',
			'primary_key': 'name',
			'get_season': lambda file: file.split('_')[-1].split('.')[0],
			'sort_key': 'season'
		},
		{
			'table': umpire_zones,
			'root': 'output-data/Career/pitch+zone',
			'primary_key': 'name',
			'get_season': lambda file: file.split('_')[-1].split('.')[0],
			'sort_key': 'season'
		}
	]
	pool.starmap(pickle, [(simple_merge_folder, arg) for arg in kwargs])
