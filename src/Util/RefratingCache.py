from StorageSolutions.tables import *
import simplejson as json
from Util.EndpointFunctions import create_rankings_object, create_umpire_object, get_all_games, \
	create_career_object, create_umpire_game_object, create_chart_object, create_team_object, \
	get_pitcher_names, create_pitcher_object
from flask import Flask, jsonify, request, Response
from multiprocessing.pool import ThreadPool as Pool
from threading import Thread
import time
import queue
from copy import deepcopy

def recache_everything(cache, mutex, refPool, data_year_range):
	print('Beginning caching process')
	time_stamp = time.time()
	cache_que = queue.Queue()
	mutex.acquire()
	try:
		cache_id = 'green' if cache['use'] == 'blue' else 'blue'
		cache[cache_id]['/umpireList'] = umpire_id_lookup.scan()

		for umpire in cache[cache_id]['/umpireList']:
			get = crew_update_table.get({'name':umpire['name'], 'season':2019},
				AttributesToGet=['crew_chief', 'crew.chief', 'ump.number', 'status', 'crew.number'])
			umpire['isCrewChief'] = get['crew_chief'] if 'crew_chief' in get else -1
			umpire['altIsCrewChief'] = get['crew.chief'] if 'crew.chief' in get else -1
			umpire['number'] = get['ump.number'] if 'ump.number' in get else -1
			umpire['status'] = get['status'] if 'status' in get else -1
			umpire['crewNumber'] = get['crew.number'] if 'crew.number' in get else -1

		cache[cache_id]['umpire_names'] = [obj['name'] for obj in cache[cache_id]['/umpireList']]
		for umpire in cache[cache_id]['/umpireList']:
			name = umpire['name']
			parts = name.split()
			umpire['firstName'] = parts[0]
			umpire['lastName'] = parts[-1]

		CACHE_ARGS = [(name, data_year_range) for name in cache[cache_id]['umpire_names']]
		gamesThread = Thread(target = get_all_games, args=(cache[cache_id]['umpire_names'], cache_que))
		gamesThread.start()

		# NEEDS PITCHER STUFF

		now = time.time()
		print('Starting to cache /get-pitchers')
		cache[cache_id]['/get-pitchers'] = refPool.map(get_pitcher_names, cache[cache_id]['umpire_names'])
		cache[cache_id]['/get-pitchers'] = {arr['name']: arr['data'] for arr in cache[cache_id]['/get-pitchers'] if len(arr) != 0}
		print('Cached /get-pitchers: t = {0}s'.format(time.time() - now))

		now = time.time()
		print('Starting to cache /charts')
		cache[cache_id]['/charts'] = refPool.starmap(create_chart_object, CACHE_ARGS)
		cache[cache_id]['/charts'] = {arr['name']: arr for arr in cache[cache_id]['/charts'] if len(arr) != 0}
		print('Cached /charts: t = {0}s'.format(time.time() - now))

		now = time.time()
		print('Starting to cache /teams')
		cache[cache_id]['/teams'] = refPool.starmap(create_team_object, CACHE_ARGS)
		cache[cache_id]['/teams'] = {arr[0]['name']: arr for arr in cache[cache_id]['/teams'] if len(arr) != 0}
		print('Cached /teams: t = {0}s'.format(time.time() - now))

		now = time.time()
		print('Starting to cache /career')
		cache[cache_id]['/career'] = refPool.starmap(create_career_object, CACHE_ARGS)
		cache[cache_id]['/career'] = {arr[0]['name']: arr for arr in cache[cache_id]['/career'] if len(arr) != 0}
		print('Cached /career: t = {0}s'.format(time.time() - now))

		now = time.time()
		print('Starting to cache/umpire')
		cache[cache_id]['/umpire'] = refPool.starmap(create_umpire_object, [(name, data_year_range[-1]) for name in cache[cache_id]['umpire_names']])
		cache[cache_id]['/umpire'] = {obj['name']: obj for obj in cache[cache_id]['/umpire'] if 'name' in obj}
		print('Cached /umpire: t = {0}s'.format(time.time() - now))

		now = time.time()
		print('Starting to cache /umpireGames')
		cache[cache_id]['/umpireGames'] = refPool.map(create_umpire_game_object, cache[cache_id]['umpire_names'])
		cache[cache_id]['/umpireGames'] = {arr[0]['name']: arr for arr in cache[cache_id]['/umpireGames'] if len(arr) != 0}
		print('Cached /umpireGames: t = {0}s'.format(time.time() - now))

		now = time.time()
		print('Starting to cache /rankings')
		cache[cache_id]['/rankings'] = refPool.starmap(create_rankings_object, CACHE_ARGS)
		cache[cache_id]['/rankings'] = json.dumps(cache[cache_id]['/rankings'], use_decimal=True)
		cache[cache_id]['/rankings'] = Response(cache[cache_id]['/rankings'], status=200, mimetype='application/json')
		print('Cached /rankings: t = {0}s'.format(time.time() - now))

		now = time.time()
		gamesThread.join()
		cache[cache_id]['/games'] = cache_que.get()
		cache[cache_id]['/games'] = [obj for obj in cache[cache_id]['/games'] if obj != {} and type(obj) != KeyError]
		print('Cached /games: t = {0}s'.format(time.time() - now))
		cache['use'] = cache_id
	finally:
		print('Finished caching in {0}s'.format(time.time() - time_stamp))
		mutex.release()


def recache_games(cache, mutex):
	mutex.acquire()
	try:
		time_stamp = time.time()
		q = queue.Queue()
		get_all_games(cache[cache['use']]['umpire_names'], q)
		games = q.get()
		games = [obj for obj in games if obj != {} and type(obj) != KeyError]
		cache[cache['use']]['games'] = games
	finally:
		print('Cached Only Games in {0}s'.format(time.time() - time_stamp))
		mutex.release()


