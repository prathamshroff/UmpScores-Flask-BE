from StorageSolutions.tables import *
import simplejson as json
from Util.EndpointFunctions import create_rankings_object, create_umpire_object, get_all_games, \
	create_career_object, create_umpire_game_object
from flask import Flask, jsonify, request, Response
from multiprocessing.pool import ThreadPool as Pool
from threading import Thread
import time
import queue
def recache_everything(cache, mutex, refPool, data_year_range):
	time_stamp = time.time()
	cache_que = queue.Queue()
	mutex.acquire()
	try:
		cache_id = 'green' if cache['use'] == 'blue' else 'blue'
		cache[cache_id]['umpire_keys'] = umpire_id_lookup.scan()

		for umpire in cache[cache_id]['umpire_keys']:
			get = crew_update_table.get({'name':umpire['name'], 'season':2019},
				AttributesToGet=['crew chief', 'ump number'])
			umpire['isCrewChief'] = get['crew chief'] if 'crew chief' in get else -1
			umpire['number'] = get['ump number'] if 'ump number' in get else -1

		cache[cache_id]['umpire_names'] = [obj['name'] for obj in cache[cache_id]['umpire_keys']]
		for umpire in cache[cache_id]['umpire_keys']:
			name = umpire['name']
			parts = name.split()
			umpire['firstName'] = parts[0]
			umpire['lastName'] = parts[-1]



		CACHE_ARGS = [(name, data_year_range) for name in cache[cache_id]['umpire_names']]
		gamesThread = Thread(target = get_all_games, args=(cache[cache_id]['umpire_names'], cache_que))
		gamesThread.start()

		now = time.time()
		cache[cache_id]['career'] = refPool.starmap(create_career_object, CACHE_ARGS)
		cache[cache_id]['career'] = {arr[0]['name']: arr for arr in cache[cache_id]['career'] if len(arr) != 0}
		print('Cached Career Objects: t = {0}s'.format(time.time() - now))

		now = time.time()
		cache[cache_id]['umpires'] = refPool.starmap(create_umpire_object, [(name, data_year_range[-1]) for name in cache[cache_id]['umpire_names']])
		cache[cache_id]['umpires'] = {obj['name']: obj for obj in cache[cache_id]['umpires'] if 'name' in obj}
		print('Cached Umpires Objects: t = {0}s'.format(time.time() - now))

		now = time.time()
		cache[cache_id]['umpire_games'] = refPool.map(create_umpire_game_object, cache[cache_id]['umpire_names'])
		cache[cache_id]['umpire_games'] = {arr[0]['name']: arr for arr in cache[cache_id]['umpire_games'] if len(arr) != 0}
		print('Cached Umpire Game Objects at: t = {0}s'.format(time.time() - now))

		now = time.time()
		cache[cache_id]['rankings'] = refPool.starmap(create_rankings_object, CACHE_ARGS)
		cache[cache_id]['rankings'] = json.dumps(cache[cache_id]['rankings'], use_decimal=True)
		cache[cache_id]['rankings'] = Response(cache[cache_id]['rankings'], status=200, mimetype='application/json')
		print('Cached Ranking Objects: t = {0}s'.format(time.time() - now))

		now = time.time()
		gamesThread.join()
		cache[cache_id]['games'] = cache_que.get()
		cache[cache_id]['games'] = [obj for obj in cache[cache_id]['games'] if obj != {} and type(obj) != KeyError]
		print('Cached Games in: t = {0}s'.format(time.time() - now))
		cache['use'] = cache_id
	finally:
		print('Finished caching in {0}s'.format(time.time() - time_stamp))
		mutex.release()

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

# team_objects = {}
# for umpire in ALL_UMPIRE_NAMES:
# 	team_objects[umpire] = []
# 	for team in team_names:
# 		team_objects[umpire] += create_team_object(umpire, team, team_stats_dataset, data_year_range)
# print('Cached Team Objects: t = {0}s'.format(time.time() - now))



