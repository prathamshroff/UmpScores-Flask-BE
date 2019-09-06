from StorageSolutions.tables import *
import simplejson as json
from Util.EndpointFunctions import *
from flask import Flask, jsonify, request, Response
import time

now = time.time()

data_year_range = range(2010, 2020)

ALL_UMPIRE_KEYS = umpire_id_lookup.scan()
ALL_UMPIRE_NAMES = [obj['name'] for obj in ALL_UMPIRE_KEYS]

def get_pitcher_names(umpire_name):
	names = set()
	for year in data_year_range:
		pitchers = umpire_pitchers.get(query_map = {'name': umpire_name, 'season': 2019}).keys()
		subnames = [name.replace('total_call_', '').replace('.', ' ') for name in pitchers if name.startswith('total_call_')]
		names = names.union(subnames)
	return names

pitcher_objects = {umpire: [create_pitcher_object(pitcher, pitcher_stats, data_year_range) for pitcher in \
	get_pitcher_names(umpire)] for umpire in ALL_UMPIRE_NAMES}
print('Cached Pitcher Objects: t = {0}s'.format(time.time() - now))

RANKINGS_OBJECT = create_rankings_object(careers_season, team_stats_dataset, ALL_UMPIRE_NAMES, data_year_range)
RANKINGS_OBJECT = json.dumps(RANKINGS_OBJECT, use_decimal=True)
RANKINGS_OBJECT = Response(RANKINGS_OBJECT, status=200, mimetype='application/json')
print('Cached Ranking Objects: t = {0}s'.format(time.time() - now))

team_names = [name.replace('total_call_', '') for name in \
    team_stats_dataset.get(query_map = {'name':'Jordan Baker', 'data_year' : 2019}).keys() if \
    name.startswith('total_call_')]
team_names = [name for name in team_names if '_' not in name]
team_objects = {}
for umpire in ALL_UMPIRE_NAMES:
	team_objects[umpire] = []
	for team in team_names:
		team_objects[umpire] += create_team_object(umpire, team, team_stats_dataset, data_year_range)
print('Cached Team Objects: t = {0}s'.format(time.time() - now))

print('Finished caching in {0}s'.format(time.time() - now))

