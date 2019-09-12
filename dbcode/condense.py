import pandas as pd
import sys, os
import math
import simplejson as json
sys.path.append('../src')
from AWS.CloudSearch import Search
from AWS.Datasets import Table
import base64
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import time
#TODO LIST:
# Don't add non existent s3 images to database

# POTENTIAL VULNERABILITIES
# Duplicate umpire/pitcher names could be problemsome
# If a pitcher switches teams during the same season that would be an issue
if os.path.exists('../.config.json'):
	configs = eval(open('../.config.json').read())
	iam = configs["iam-user"]
else:
	print('Oops, could not find refrating-be/.config.json!')
	exit(1)


# Creating AWS objects
umpires_cloudsearch = Search(iam, configs['cloudsearch']['umpires']['url'], 
	configs['cloudsearch']['umpires']['name'])

team_stats_table = Table(iam, 'refrating-team-stats-v1', umpires_cloudsearch)
game_stats_table = Table(iam, 'refrating-game-stats-v1')
games_date_lookup = Table(iam, 'refrating-games-lookup')
umpire_id_lookup = Table(iam, 'refrating-umps-lookup')
careers_season = Table(iam, 'refrating-careers-season')
careers = Table(iam, 'refrating-careers')
careers_range = Table(iam, 'refrating-career-range')
crews = Table(iam, 'refrating-crews')
careers_range_change = Table(iam, 'refrating_career_range_change')
ump_game_lookup = Table(iam, 'refrating-ump-game-lookup')
s3_client = boto3.client('s3', aws_access_key_id = iam['key'],
	aws_secret_access_key = iam['secret'])
pitcher_stats = Table(iam, 'refrating-pitcher-stats')
umpire_pitchers = Table(iam, 'refrating-umpire-pitchers')
umpire_zones = Table(iam, 'refrating-pitcher-zone')
profile_month_table = Table(iam, 'refrating-profile-month')
average_game_length_table = Table(iam, 'umpscores-career-average-game-length')


def upload_average_game_length_table():
	root = 'output-data/Career/average_game_length.csv'
	df = pd.read_csv(root)
	df.rename(columns = {'ump': 'name'}, inplace=True)
	df = Table.fillna(df, [])
	df = df.drop(columns=['Unnamed: 0'])
	df.to_csv(root)
	average_game_length_table.clear('name')
	average_game_length_table.upload(root)

def upload_profile_best_worst_months():
	root = 'output-data/Profile/best-worst month'
	files = [os.path.join(root, file) for file in os.listdir(root)]
	profile_month_table.clear('name', sort_key = 'season')
	for file in files:
		df = pd.read_csv(file)
		if 'ump' in df.columns:
			df.rename(columns = {'ump': 'name'}, inplace = True)
		if 'season' not in df.columns:
			df['season'] = len(df) * [file.split('.')[0].split('_')[-1]]
		if 'Unnamed: 0' in df.columns:
			df = df.drop(columns = ['Unnamed: 0'])
		df = Table.fillna(df, [])
		df.to_csv(file)
		profile_month_table.upload(file)

def upload_zone_data():
	root = 'output-data/Career/pitch+zone'
	umpire_zones.clear('name', sort_key = 'season')
	for file in os.listdir(root):
		filename = os.path.join(root, file)
		df = pd.read_csv(filename)
		df['season'] = [file[-8:-4]] * len(df) # get year
		df = Table.fillna(df, [])
		df = df.drop(columns = ['Unnamed: 0'])
		df.to_csv(filename)
		umpire_zones.upload(filename)

def upload_umpire_pitchers():
	umpire_pitchers.clear('name', sort_key = 'season')
	parent_folder = 'output-data/Pitcher-Stats'
	for season_folder in os.listdir(parent_folder):
		if season_folder in ['Archive', 'walk-strikeout']:
			continue
		season_filepath = os.path.join(parent_folder, season_folder)
		file = os.path.join(season_filepath, 'ump_pitcher.csv')
		df = Table.fillna(pd.read_csv(file), [])
		df = df.drop(columns = ['Unnamed: 0'])
		df['season'] = [season_folder] * len(df)
		df.rename(columns = {'ump': 'name'}, inplace = True)
		output_filepath = os.path.join(season_filepath, 'ump_pitcher_refined.csv')
		df.to_csv(output_filepath)
		umpire_pitchers.upload(output_filepath)


def upload_pitcher_stats():
	def add_pitcher_name(row):
		row['pitcher_uuid'] = '{0}_{1}_{2}'.format(row['name'], row['team'], row['season'])
		return row
	pitcher_stats.clear('name', sort_key = 'pitcher_uuid')
	parent_folder = 'output-data/Pitcher-Stats'
	for season_folder in os.listdir(parent_folder):
		season_filepath = os.path.join(parent_folder, season_folder)
		if season_folder != 'Archive':
			df = pd.read_csv(os.path.join(season_filepath, 'pitcher_BCR.csv'))
			for filename in os.listdir(season_filepath):
				if filename not in ['pitcher_BCR.csv', 'ump_pitcher.csv', 'merged.csv']:
					df = pd.merge(df, pd.read_csv(os.path.join(season_filepath, filename)),
						left_on = ['name', 'team'], right_on = ['name', 'team'], suffixes = ('', '_y'))
					df = drop_y(df)
			df = Table.fillna(df, [])
			df = df.drop(columns = ['Unnamed: 0'])
			df['season'] = [season_folder] * len(df)
			df = df.apply(add_pitcher_name, axis=1)
			output_filename = os.path.join(season_filepath, 'merged.csv')
			df.to_csv(output_filename)
			pitcher_stats.upload(output_filename)


def ump_game_lookup_refresh():
	root = 'output-data/Game-Stats'
	folders = [os.path.join(root, folder) for folder in os.listdir(root)]
	filenames = [os.path.join(folder, 'game_bcr.csv') for folder in folders]
	output = pd.DataFrame()
	output['ump'] = []
	output['game'] = []

	for file in filenames:
		df = pd.read_csv(file)[['ump', 'game']]
		output = pd.concat((output, df))

	output.rename(columns = {'ump': 'name'}, inplace = True)
	if 'Unnamed: 0' in output.columns:
		output = output.drop(columns = ['Unnamed: 0'])
	output.to_csv('refrating_ump_game_lookup.csv')
	
	ump_game_lookup.clear('name', sort_key = 'game')
	ump_game_lookup.upload('refrating_ump_game_lookup.csv')

def upload_career_change_range_file():
	"""
	Empties and reuploads Change_in_BCR_2010-2019.csv to refrating_career_range_change table

	Requirements
	----------
	Before running this, make sure you have done the following:
	dbcode/output-data/Career exists and if not, download output-data from drive
	"""
	root = 'output-data/Career/Change_in_BCR_2010-2019.csv'
	df = pd.read_csv(root)
	if 'ump' in df.columns:
		df.rename(columns = {'ump': 'name'}, inplace = True)
	if 'Unnamed: 0' in df.columns:
		df = df.drop(columns = ['Unnamed: 0'])
	df.to_csv(root)
	careers_range_change.clear('name')
	careers_range_change.upload(root)


def upload_career_range_file():
	"""
	Empties and reuploads season_bcr_2010-2019.csv to refrating-career-range table

	Requirements
	----------
	Before running this, make sure you have done the following:
	dbcode/output-data/Career exists and if not, download output-data from drive
	"""
	root = 'output-data/Career/BCR By Season/season_bcr_2010-2019.csv'
	df = pd.read_csv(root)
	if 'ump' in df.columns:
		df.rename(columns = {'ump': 'name'}, inplace = True)
	if 'Unnamed: 0' in df.columns:
		df = df.drop(columns = ['Unnamed: 0'])
	df.to_csv(root)
	careers_range.clear('name')
	careers_range.upload(root)


def upload_crew_update():
	"""
	Iterates through output-data/Career/crew_update files, appends data_year onto those files
	and uploads the files to refrating-crews dynamodb table.

	Requirements
	----------
	Before running this, make sure you have done the following:
	dbcode/output-data/Career exists and if not, download output-data from drive
	"""
	root = 'output-data/Career/crew_update'
	filenames = [os.path.join(root, file) for file in os.listdir(root) if file != 'ARCHIVE']
	string_fields = ['status', 'crew']
	crews.clear('name', sort_key = 'data_year')
	for file in filenames:
		df = pd.read_csv(file)
		df = Table.fillna(df, string_fields)
		if 'data_year' not in df.columns:
			df['data_year'] = [int(file.split('.')[0][-4:])] * df['name'].count()
		if 'Unnamed: 0' in df.columns:
			df = df.drop(columns = ['Unnamed: 0'])
		if 'crew_dhief' in df.columns:
			df.rename(columns = {'crew_dhief': 'crew_chief'}, inplace = True)
		if 'crew_field' in df.columns:
			df.rename(columns = {'crew_field': 'crew_chief'}, inplace = True)
		df.to_csv(file)
		crews.upload(file)
	print('Uploaded crew data')


def upload_career_data():
	"""
	Uploads dbcode/output-data/Career/career.csv to refrating-careers table and renames ump
	column to name in that table

	Requirements
	----------
	Before running this, make sure you have done the following:
	dbcode/output-data/Career/career.csv exists and if not, download output-data from drive
	"""
	filename = 'output-data/Career/career.csv'
	df = pd.read_csv(filename)
	if 'ump' in df.columns:
		df.rename(columns = {'ump': 'name'}, inplace = True)
	if 'Unnamed: 0' in df.columns:
		df = df.drop(columns = ['Unnamed: 0'])
	df.to_csv(filename)
	careers.clear('name')
	careers.upload(filename)
	print('Uploaded career.csv')


def create_career_seasonal_data():
	"""
	Iterates through Career/season_bcr_year.csv files, appends data_year onto those files
	and uploads the files to refrating-careers-season dynamodb table.

	Requirements
	----------
	Before running this, make sure you have done the following:
	dbcode/output-data/Career exists and if not, download output-data from drive
	"""
	season_career_bcrs = ['season_bcr_{0}.csv'.format(year) for year in range(2010, 2020)]
	careers_season.clear('name', sort_key = 'data_year')

	for file in season_career_bcrs:
		file = os.path.join('output-data/Career/BCR By Season/', file)
		df = pd.read_csv(file)

		if 'data_year' not in df.columns:
			df['data_year'] = [file.split('_')[2].split('.')[0]] * len(df)
			df.drop(columns = ['Unnamed: 0'], inplace=True)
			df.to_csv(file)
		careers_season.upload(file)
	print('Uploaded career_seasonal_bcr data')


def create_game_date():
	"""
	Creates the game_date.csv file and repopulates the refrating-games-lookup dynamodb
	table

	Requirements
	----------
	Before running this, make sure you have done the following:
	dbcode/output-data/* exists and if not, download output-data from drive
	You've ran dataPrep() against output-data Game-Stats effectively updating 
		all of its merged.csv files. If you have not done this yet, do before
		running this.

	Algorithm
	----------
	Iterates through every merged.csv file in output-date/Game-Stats directory and identifies
	all of the unique game-date rows. Creates a table containing only these two
	indexable rows and writes it to game_date.csv and finishes by emptying then reuploading
	game_date.csv data to the refrating-games-lookup dynamodb table
	"""
	root = 'output-data/Game-Stats'
	folders = [os.path.join(root, folder) for folder in os.listdir(root)]
	files = []
	for folder in folders:
		files.append(os.path.join(folder, 'merged.csv'))
	df = pd.read_csv(files[0])
	df = df[['game', 'date']]
	for i in range(1, len(files)):
		file = pd.read_csv(files[i])[['game', 'date']]
		df = pd.concat((df, file))
	if 'Unnamed: 0' in df.columns:
		df = df.drop(columns=['Unnamed: 0'])
	df.to_csv('game_date.csv')

	games_date_lookup.clear('game', sort_key='date')
	games_date_lookup.upload('game_date.csv')


def media_refresh():
	"""
	Empties and reuploads umpire images and team logos

	Requirements
	----------
	Before running this, make sure you have done the following:
	dbcode/ref_images exists, otherwise download ref_images from drive
	dbcode/team_logos exists, otherwise download team_logos from drive
	refrating-umps-lookup dynamodb Table has most recent information.
		See umpire_id_lookup_reset on how to update this table

	Algorithm
	----------
	First it empties every item from the ref rating bucket. Afterwards,
	it iterates through every image in ref_images folder which you can obtain from 
	google drive or by asking Chris Ackerman. These image names are hashed
	using our data team's ids for that respective umpire. I convert the number hash 
	filename into a human readable filename containing the umpire's name where spaces are
	replaced with plus signs in s3 upon uploading by looking up the id in our
	refrating-umps-lookup table. The url for the ref images will be
	s3_url/umpires/<first_name>+<last_name>.jpg

	After I've uploaded every ref image, I will continue to upload every team logo.
	I do this by iterating through the team_logos folder of which you can find on drive,
	and simply reading then uploading those png's to s3. The url schema for these images
	will be s3_url/logos/<logo_file_name>.png
	"""
	media_folder = 'ref_images'
	objects = s3_client.list_objects(Bucket=configs['media_bucket'])
	if 'Contents' in objects:
		for resp in objects['Contents']:
			print('Deleting {0}'.format(resp['Key']))
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
			bucket_obj_name = 'umpires/{0}.jpg'.format(lookup['name'])
			print('uploading {0}'.format(bucket_obj_name))
			s3_client.upload_fileobj(img, configs['media_bucket'], 
				Key = bucket_obj_name,
				ExtraArgs = args)

	logos = [os.path.join('team_logos', file) for file in os.listdir('team_logos')]

	for logo in logos:
		with open(logo, 'rb') as file:
			team_name = logo.split('/')[1].split('.')[0]
			keyname = 'logos/{0}.png'.format(team_name)
			print('uploading {0}'.format(keyname))
			s3_client.upload_fileobj(file, configs['media_bucket'],
				Key = keyname,
				ExtraArgs = {
					'ContentType': 'image/png'
				})


def umpire_id_lookup_reset():
	"""
	Empties and reuploads the refrating-umps-lookup dynamodb table

	Requirements
	----------
	Before running this, make sure you have done the following:
	dbcode/name_id.csv exists and if not, ask Chris Ackerman to give you this file

	Algorithm
	----------
	Empties the entirety of refrating-umps-lookup and then reuploads it with 
	content from the newly parsed name_id.csv file. name_id.csv is parsed such that
	the ump column is renamed to name, and a url to the umpire profile pic is included
	in the data. 
	"""
	umpire_id_lookup.clear('name', sort_key='id')
	df = pd.read_csv('name_id.csv')
	if 'ump' in df:
		df.rename(columns={'ump':'name'}, inplace=True)
	if 'ump_profile_pic' not in df.columns:
		get_url = lambda name: 'https://{0}.s3.amazonaws.com/umpires/{1}+{2}.jpg'.format(
			configs['media_bucket'],
			*name.split()
		)
		df['ump_profile_pic'] = df['name'].apply(lambda row: get_url(row))
	if 'Unnamed: 0' in df.columns:
		df = df.drop(columns=['Unnamed: 0'])
	df.to_csv('name_id.csv')
	umpire_id_lookup.upload('name_id.csv')


def drop_y(df):
	"""
	Removes duplicate columns which emerged from merging pd.DataFrames

	Parameters
	----------
	df : pd.DataFrame
		some table which has recently been merged with another pd.DataFrame

	Returns
	----------
	pd.DataFrame
		returns a DataFrame which removed all potential duplicate columns
	"""
	to_drop = [x for x in df if x.endswith('_y')]
	df.drop(to_drop, axis=1, inplace=True)
	return df


def dataPrep(filepaths):
	"""
	Generates merged.csv files within every directory inside filepaths

	Requirements
	----------
	Before running this, make sure you have done the following:
	dbcode/output-data exists otherwise download output-data from drive

	Parameters
	----------
	filepaths : list
		filepaths should be a list of which folders directly inside output-data exist.
			e.g. filepaths = ['output-data/Team-Stats', 'output-data/Game-Stats', ...]
	
	Algorithm
	----------
	dataPrep iterates through every year folder within filepaths.
	Once inside of some year-folder, it will compress all of those csv files into
	a singular csv file called 'merged.csv'. This merged.csv file will then be used
	for future operations. dataPrep will also parse csv files according to which filepath
	the files reside under. For example, Team-Stats files require different sanitization
	in comparison to Game-Stats files.
	"""
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
					right_on=on, suffixes=('', '_y'))

			if path == 'output-data/Team-Stats':
				if 'ump_profile_pic' not in merge.columns:
					get_url = lambda name: 'https://{0}.s3.amazonaws.com/umpires/{1}+{2}'.format(configs['media_bucket'],
						*name.split())
					merge['ump_profile_pic'] = merge['name'].apply(lambda row: get_url(row))

			if path == 'output-data/Game-Stats':
				date_format = lambda date: date.replace('/', '-')
				merge['date'] = merge['date'].apply(lambda row: date_format(row))

			merge = drop_y(merge)
			merge.to_csv(os.path.join(os.path.join(path, year), 'merged.csv'))


def dataUpload(filepaths):
	"""
	Empties and reuploads merged.csv files to corresponding dynamodb resources

	Requirements
	----------
	Before running this, make sure you have done the following:
	dbcode/output-data exists otherwise download output-data from drive
	Ensure you have recently ran dataPrep in order to reupdate the merged.csv files

	Parameters
	----------
	filepaths : list
		filepaths should be a list of which folders directly inside output-data exist.
			e.g. filepaths = ['output-data/Team-Stats', 'output-data/Game-Stats', ...]

	Algorithm
	----------
	Iterates through every filepath in filepaths and uploads the merged.csv files
	found within that filepath to their corresponding dynamodb resources. Before
	any uploads happen naturally, we completely empty the table first to ensure
	duplicate and corrupted data is cleared out.
	"""
	for path in filepaths:
		folders = [os.path.join(path, folder) for folder in os.listdir(path)]
		refined_files = [os.path.join(folder, 'merged.csv') for folder in folders]

		if path == 'output-data/Team-Stats':
			team_stats_table.clear('name', sort_key = 'data_year')
			table = team_stats_table

		elif path == 'output-data/Game-Stats':
			game_stats_table.clear('game')
			table = game_stats_table

		for file in refined_files:
			table.upload(file)


def loadYear(parent_folder, string_fields):
	"""
	Turns all of csv files within some parent_folder into a python readable object keyed by
	the files path and year.

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
		path in filepaths[year] if path[-4:] == '.csv'} for year in filepaths}
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


def refresh_all_aws_resources():
	tasks = [
		'output-data/Team-Stats',
		'output-data/Game-Stats'
	]
	stamp = time.time()
	upload_average_game_length_table()
	# upload_profile_best_worst_months()
	# upload_zone_data()
	# upload_umpire_pitchers()
	# upload_pitcher_stats()
	# ump_game_lookup_refresh()
	# upload_career_change_range_file()
	# upload_career_range_file()
	# upload_crew_update()
	# upload_career_data()
	# create_career_seasonal_data()
	# dataPrep(tasks)
	# create_game_date()
	# umpire_id_lookup_reset()
	# media_refresh()
	# dataUpload(tasks)
	# umpires_cloudsearch.clear()
	# umpires_cloudsearch.flush()
	print('Completed all tasks in {0}s'.format(time.time() - stamp))

if __name__ == '__main__':
	refresh_all_aws_resources()
