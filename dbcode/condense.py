import pandas as pd
import sys, os
import math
import simplejson as json
from DataSections.Tables import *
sys.path.append('../src')
from AWS.CloudSearch import Search
from AWS.Datasets import Table
import base64
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import time
from multiprocessing.pool import ThreadPool as Pool
from threading import Thread
import DataSections.Profile as Profile
import DataSections.Career as Career
import DataSections.Pitcher as Pitcher
from DataSections.Util import simple_merge_folder, single_file_upload
#TODO LIST:
# Don't add non existent s3 images to database

# POTENTIAL VULNERABILITIES
# Duplicate umpire/pitcher names could be problemsome
# If a pitcher switches teams during the same season that would be an issue
pool = Pool()
def single_files_scheduler():
	t1 = Thread(target = single_file_upload, kwargs = {
			'table': umpires_2019_table, 
			'filepath': 'output-data/Profile/umpire2019.xlsx', 
			'primary_key': 'name',
			'output_filepath': 'output-data/Profile/umpire2019.csv', 
			'sort_key': None, 
			'reader': pd.read_excel
		}
	)
	t1.start()
	args = [
		[average_game_length_table, 'output-data/Career/average_game_length.csv', 'name'],
		[bcr_best_year_table, 'output-data/Profile/best_year.csv', 'name'],
		[bcr_weather_table, 'output-data/Profile/bcr_weather.csv', 'name'],
		[bcr_start_time_table, 'output-data/Profile/bcr_start_time.csv', 'name'],
		[bcr_std_table, 'output-data/Career/bcr_std.csv', 'name'],
		[ejections_table, 'output-data/Career/ejections.csv', 'name'],
		[careers_range_change, 'output-data/Career/Change_in_BCR_2010-2019.csv', 'name'],
		[careers, 'output-data/Career/career.csv', 'name'],
		[careers_range, 'output-data/Career/BCR By Season/BCR_among_all_umps.csv', 'name']
		
	]
	pool.starmap(single_file_upload, args)
	t1.join()
	print('Finished uploading singular files')

def upload_umpire_pitchers():
	umpire_pitchers.clear('name', sort_key = 'season')
	parent_folder = 'output-data/Pitcher-Stats'
	for season_folder in os.listdir(parent_folder):
		if season_folder in ['Archive', 'walk-strikeout']:
			continue
		season_filepath = os.path.join(parent_folder, season_folder)
		file = os.path.join(season_filepath, 'ump_pitcher.csv')
		df = Table.fillna(pd.read_csv(file), [])
		df['season'] = [season_folder] * len(df)
		df['name'] = df['ump'].apply(lambda ump: ump.lower())
		df.drop(columns = ['ump', 'Unnamed: 0'], inplace = True)
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
		if season_folder not in ['Archive', 'walk-strikeout']:
			df = pd.read_csv(os.path.join(season_filepath, 'pitcher_BCR.csv'))
			for filename in os.listdir(season_filepath):
				if filename not in ['pitcher_BCR.csv', 'ump_pitcher.csv', 'ump_pitcher_refined.csv', 'merged.csv']:
					df = pd.merge(df, pd.read_csv(os.path.join(season_filepath, filename)),
						left_on = ['name', 'team'], right_on = ['name', 'team'], suffixes = ('', '_y'))
					df = Table.drop_y(df)
			df = Table.fillna(df, [])
			df = df.drop(columns = ['Unnamed: 0'])
			df['season'] = [season_folder] * len(df)
			df = df.apply(add_pitcher_name, axis=1)
			df['name'] = df['name'].apply(lambda row: row.lower())
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
	output['name'] = output['name'].apply(lambda row: row.lower())
	output.to_csv('refrating_ump_game_lookup.csv')
	
	ump_game_lookup.clear('name', sort_key = 'game')
	ump_game_lookup.upload('refrating_ump_game_lookup.csv')


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
	df['name'] = df['name'].apply(lambda row: row.lower())
	df.to_csv('name_id.csv')
	umpire_id_lookup.upload('name_id.csv')




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

			merge = Table.drop_y(merge)
			if 'name' in merge.columns:
				merge['name'] = merge['name'].apply(lambda row: row.lower())
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
	# Pitcher.upload_strikeout(pool)
	# Career.career_upload_simple_folders(pool)
	# Profile.profile_upload_simple_folders(pool)
	# single_files_scheduler()
	# upload_umpire_pitchers()
	upload_pitcher_stats()
	ump_game_lookup_refresh()
	dataPrep(tasks)
	create_game_date()
	umpire_id_lookup_reset()
	media_refresh()
	dataUpload(tasks)
	umpires_cloudsearch.clear()
	umpires_cloudsearch.flush()
	print('Completed all tasks in {0}s'.format(time.time() - stamp))

if __name__ == '__main__':
	refresh_all_aws_resources()
