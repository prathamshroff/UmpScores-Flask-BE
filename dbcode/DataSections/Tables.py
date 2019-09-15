import sys
import os
import boto3
import pandas as pd
sys.path.append('../src')
from AWS.CloudSearch import Search
from AWS.Datasets import Table
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
profile_best_worst_month_table = Table(iam, 'refrating-profile-month')
average_game_length_table = Table(iam, 'umpscores-career-average-game-length')
bcr_std_table = Table(iam, 'umpscores-bcr-std')
crew_update_table = Table(iam, 'umpscores-crew-update')
ejections_table = Table(iam, 'umpscores-ejections')
bcr_start_time_table = Table(iam, 'umpscores-bcr-start-time')
bcr_weather_table = Table(iam, 'umpscores-bcr-weather')
bcr_best_year_table = Table(iam, 'umpscores-best-year')
umpires_2019_table = Table(iam, 'umpscores-umpires-2019')
profile_team_preference_table = Table(iam, 'umpscores-profile-team-preferences')
profile_best_worst_park_table = Table(iam, 'umpscores-profile-best-worst-park')
career_crucial_calls_table = Table(iam, 'umpscores-crucial-calls')
pitcher_walk_strikeout = Table(iam, 'umpscores-pitcher-walk-strikeout')
	


