from StorageSolutions.tables import *
import simplejson as json
from Util.EndpointFunctions import create_rankings_object, create_umpire_object, get_all_games, cache
from flask import Flask, jsonify, request, Response
from multiprocessing.pool import ThreadPool as Pool
from threading import Thread
import time

now = time.time()

refPool = Pool()
data_year_range = range(2010, 2020)
ALL_UMPIRE_KEYS = umpire_id_lookup.scan()

for umpire in ALL_UMPIRE_KEYS:
	get = crew_update_table.get({'name':umpire['name'], 'season':2019},
		AttributesToGet=['crew chief', 'ump number'])
	umpire['isCrewChief'] = get['crew chief'] if 'crew chief' in get else -1
	umpire['number'] = get['ump number'] if 'ump number' in get else -1

ALL_UMPIRE_NAMES = [obj['name'] for obj in ALL_UMPIRE_KEYS]
for umpire in ALL_UMPIRE_KEYS:
	name = umpire['name']
	parts = name.split()
	umpire['firstName'] = parts[0]
	umpire['lastName'] = parts[-1]


cache['umpires'] = refPool.starmap(create_umpire_object, [(name, data_year_range[-1]) for name in ALL_UMPIRE_NAMES])
cache['umpires'] = {obj['name']: obj for obj in cache['umpires'] if 'name' in obj}
print('Generated Umpires Objects: t = {0}s'.format(time.time() - now))
# def get_pitcher_names(umpire_name):
# 	names = set()
# 	for year in data_year_range:
# 		pitchers = umpire_pitchers.get(query_map = {'name': umpire_name, 'season': 2019}).keys()
# 		subnames = [name.replace('total_call_', '').replace('.', ' ') for name in pitchers if name.startswith('total_call_')]
# 		names = names.union(subnames)
# 	return names

# pitcher_objects = {umpire: [create_pitcher_object(pitcher, pitcher_stats, data_year_range) for pitcher in \
# 	get_pitcher_names(umpire)] for umpire in ALL_UMPIRE_NAMES}
# print('Cached Pitcher Objects: t = {0}s'.format(time.time() - now))

cache['rankings'] = refPool.starmap(create_rankings_object, [(name, data_year_range) for name in ALL_UMPIRE_NAMES])
cache['rankings'] = json.dumps(cache['rankings'], use_decimal=True)
cache['rankings'] = Response(cache['rankings'], status=200, mimetype='application/json')
print('Cached Ranking Objects: t = {0}s'.format(time.time() - now))



cache[cache['use']]['games'] = get_all_games(ALL_UMPIRE_NAMES)
print('Cached Games in: t = {0}s'.format(time.time() - now))
# team_objects = {}
# for umpire in ALL_UMPIRE_NAMES:
# 	team_objects[umpire] = []
# 	for team in team_names:
# 		team_objects[umpire] += create_team_object(umpire, team, team_stats_dataset, data_year_range)
# print('Cached Team Objects: t = {0}s'.format(time.time() - now))

print('Finished caching in {0}s'.format(time.time() - now))

