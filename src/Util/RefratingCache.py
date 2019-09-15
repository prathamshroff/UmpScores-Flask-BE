from StorageSolutions.tables import *
import simplejson as json
from Util.EndpointFunctions import create_rankings_object, create_umpire_object, get_all_games, cache, \
	create_career_object
from flask import Flask, jsonify, request, Response
from multiprocessing.pool import ThreadPool as Pool
from threading import Thread
import time
import queue

que = queue.Queue()

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

CACHE_ARGS = [(name, data_year_range) for name in ALL_UMPIRE_NAMES]
gamesThread = Thread(target = get_all_games, args=(ALL_UMPIRE_NAMES, que))
gamesThread.start()

cache['use'] = 'blue'
cache['blue'] = {}
cache['green'] = {}

cache[cache['use']]['career'] = refPool.starmap(create_career_object, CACHE_ARGS)
cache[cache['use']]['career'] = {arr[0]['name']: arr for arr in cache[cache['use']]['career'] if len(arr) != 0}
print('Cached Career Objects: t = {0}s'.format(time.time() - now))

cache[cache['use']]['umpires'] = refPool.starmap(create_umpire_object, [(name, data_year_range[-1]) for name in ALL_UMPIRE_NAMES])
cache[cache['use']]['umpires'] = {obj['name']: obj for obj in cache[cache['use']]['umpires'] if 'name' in obj}
print('Cached Umpires Objects: t = {0}s'.format(time.time() - now))
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

cache[cache['use']]['rankings'] = refPool.starmap(create_rankings_object, CACHE_ARGS)
cache[cache['use']]['rankings'] = json.dumps(cache[cache['use']]['rankings'], use_decimal=True)
cache[cache['use']]['rankings'] = Response(cache[cache['use']]['rankings'], status=200, mimetype='application/json')
print('Cached Ranking Objects: t = {0}s'.format(time.time() - now))


gamesThread.join()
cache[cache['use']]['games'] = que.get()
print('Cached Games in: t = {0}s'.format(time.time() - now))
# team_objects = {}
# for umpire in ALL_UMPIRE_NAMES:
# 	team_objects[umpire] = []
# 	for team in team_names:
# 		team_objects[umpire] += create_team_object(umpire, team, team_stats_dataset, data_year_range)
# print('Cached Team Objects: t = {0}s'.format(time.time() - now))


print('Finished caching in {0}s'.format(time.time() - now))

