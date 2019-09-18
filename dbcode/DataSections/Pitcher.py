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

def upload_strikeout():
	root = 'output-data/Pitcher-Stats/walk-strikeout'
	files = [os.path.join(root, 'walk_strikeout_{0}.csv'.format(year)) for year in range(2010, 2020)]
	pitcher_names = []
	merge = {'composite_key': [], 'name': [], 'pitcher': [], 'season': [],
		'strikeout': [], 'ws_ratio': [], 'walk': []}
	for file in files:
		print(file)
		df = pd.read_csv(file)
		pitchers = []
		for col in df.columns:
			if col.startswith('walk_'):
				pitcher_name = col.replace('walk_', '')
				# Turning D..J..Carrasco into D.J. Carrasco
				if '..' not in pitcher_name:
					pitchers.append(' '.join(pitcher_name.split('.')))
				else:
					name = pitcher_name.replace('..', '*.')
					retname = []
					for word in name.split('.'):
						word = word.replace('*', '.')
						if len(word) == 1:
							retname.append(word + '.')
						elif word[-1] == '.':
							retname.append(word + ' ')
						else:
							retname.append(word + ' ')
					retname = ''.join(retname)
					pitchers.append(retname)

		umpire_names = [ump for ump in df['ump']]
		season = file.split('_')[-1].split('.')[0]
		for umpire in umpire_names:
			for pitcher in pitchers:
				row = df.loc[df['ump'] == umpire].to_dict()
				k = '.'.join(pitcher.split())
				merge['composite_key'].append('{0}_{1}_{2}'.format(umpire, pitcher, season))
				merge['name'].append(umpire)
				merge['pitcher'].append(pitcher)
				merge['season'].append(season)

				merge['strikeout'].append(row['strikeout_{0}'.format(k)][list(row['strikeout_{0}'.format(k)].keys())[0]])
				merge['ws_ratio'].append(row['ws_ratio_{0}'.format(k)][list(row['ws_ratio_{0}'.format(k)].keys())[0]])
				merge['walk'].append(row['walk_{0}'.format(k)][list(row['walk_{0}'.format(k)].keys())[0]])
	merge = df.DataFrame(merge)
	merge.to_csv(os.path.join(root, 'merged.csv'))


