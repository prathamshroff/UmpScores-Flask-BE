import simplejson as json
import sys

with open('.config.json') as f:
    configs = json.load(f)
sys.path.append('../AWS')
from AWS.Datasets import Table
from AWS.CloudSearch import Search

# Connect boto3 resources
iam = configs['iam-user']
umpires_text_search = Search(iam, configs['cloudsearch']['umpires']['url'], 
    configs['cloudsearch']['umpires']['name'])
games_text_search = Search(iam, configs['cloudsearch']['games']['url'],
        configs['cloudsearch']['games']['name'])

team_stats_dataset = Table(iam, 'refrating-team-stats-v1', umpires_text_search)
games_dataset = Table(iam, 'refrating-game-stats-v1', games_text_search)
careers_range_change = Table(iam, 'refrating_career_range_change')
ump_game_lookup = Table(iam, 'refrating-ump-game-lookup')
pitcher_stats = Table(iam, 'refrating-pitcher-stats')

games_date_lookup = Table(iam, 'refrating-games-lookup')
careers_season = Table(iam, 'refrating-careers-season')
umpire_id_lookup = Table(iam, 'refrating-umps-lookup')
careers_range = Table(iam, 'refrating-career-range')
careers = Table(iam, 'refrating-careers')
crews = Table(iam, 'refrating-crews')
umpire_pitchers = Table(iam, 'refrating-umpire-pitchers')
umpire_zones = Table(iam, 'refrating-pitcher-zone')
profile_month_table = Table(iam, 'refrating-profile-month')
average_game_length_table = Table(iam, 'umpscores-career-average-game-length')
bcr_std_table = Table(iam, 'umpscores-bcr-std')
crew_update_table = Table(iam, 'umpscores-crew-update')
ejections_table = Table(iam, 'umpscores-ejections')
bcr_start_time_table = Table(iam, 'umpscores-bcr-start-time')
bcr_weather_table = Table(iam, 'umpscores-bcr-weather')
bcr_best_year_table = Table(iam, 'umpscores-best-year')