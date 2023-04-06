from StorageSolutions.tables import *
import simplejson as json
from Util.EndpointFunctions import create_rankings_object, create_umpire_object, get_all_games, \
	create_career_object, create_umpire_game_object, create_chart_object, create_team_object, \
	get_pitcher_names, create_pitcher_object, create_awards_object
from flask import Flask, jsonify, request, Response
from multiprocessing.pool import ThreadPool as Pool
from threading import Thread
import time
import queue
from copy import deepcopy
from threading import Lock


def recache_everything(cache, mutex, refPool, data_year_range):
	"""
	Recaches everything within our cache object and toggles the cache.

	Parameters
	----------
	cache : Dict
		cache dict representing the objects we have cached. Uses green blue deployment
		such that we can update all objects on a separate cache and atomically switch to the
		updated cache version.
			e.g. cache = {
				'green': {...},
				'blue': {...},
				'use': green
			}
	mutex : threading.Lock()
		mutex locks this function to ensure mutual exclusion within the cache
	refPool : multiprocessing.pool.ThreadPool
		multiprocessing threaded pooling object to parallelize execution of updating the cache
	data_year_range : list representing the year range for various information.
		e.g. range(2010,2019)    
	"""
	with mutex:
		try:
			print('Beginning caching process')
			time_stamp = time.time()
			cache_que = queue.Queue()
			cache_id = 'green' if cache.get('use') == 'blue' else 'blue'

			cache[cache_id]['/umpireList'] = umpire_id_lookup.scan()

			for umpire in cache[cache_id]['/umpireList']:
				try:
					get = crew_update_table.get({'name':umpire['name'], 'season':2019},
						AttributesToGet=['crew.chief', 'ump.number', 'status', 'crew.number'])
					umpire['isCrewChief'] = get['crew.chief'] if 'crew.chief' in get else -1
					umpire['number'] = get['ump.number'] if 'ump.number' in get else -1
					umpire['status'] = get['status'] if 'status' in get else -1
					umpire['crewNumber'] = get['crew.number'] if 'crew.number' in get else -1
				except Exception as e:
					print(f"Error processing umpire {umpire['name']}: {e}")

			cache[cache_id]['umpire_names'] = [obj['name'] for obj in cache[cache_id]['/umpireList']]
			for umpire in cache[cache_id]['/umpireList']:
				name = umpire['name']
				parts = name.split()
				umpire['firstName'] = parts[0]
				umpire['lastName'] = parts[-1]

			CACHE_ARGS = [(name, data_year_range) for name in cache[cache_id]['umpire_names']]

			gamesThread = Thread(target=get_all_games, args=(cache[cache_id]['umpire_names'], cache_que))
			gamesThread.start()

			# NEEDS PITCHER STUFF

			now = time.time()
			print("Started to cache awards")
			cache[cache_id]['/awards'] = create_awards_object()
			print("Finished caching /awards in {0}s".format(time.time() - now))

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
			print('Starting to cache /umpire')
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
			print('Starting to cache /games')
			gamesThread.join()
			cache[cache_id]['/games'] = cache_que.get()
			f = lambda x: {key: x[key] if type(x[key]) != KeyError else -1 for key in x}
			cache[cache_id]['/games'] = [f(obj) for obj in cache[cache_id]['/games'] if obj != {} and 'umpireName' in obj and type(obj) != KeyError]
			print('Cached /games: t = {0}s'.format(time.time() - now))

			cache['use'] = cache_id
		except Exception as e:
			print(f'Error during recache_everything: {e}')


def recache_games(cache, mutex):
	"""
	Re-Caches /games endpoint

	Parameters
	----------
	cache : Dict
		cache dict representing the objects we have cached. Uses green blue deployment
		such that we can update all objects on a separate cache and atomically switch to the
		updated cache version.
			e.g. cache = {
				'green': {...},
				'blue': {...},
				'use': green
			}
	mutex : threading.Lock()
		mutex locks this function to ensure mutual exclusion while recaching
	"""
	with mutex:
		try:
			time_stamp = time.time()
			q = queue.Queue()
			get_all_games(cache[cache['use']]['umpire_names'], q)
			games = q.get()
			games = [obj for obj in games if obj != {} and type(obj) != KeyError]
			f = lambda x: {key: x[key] if type(x[key]) != KeyError else -1 for key in x}
			games = [f(obj) for obj in games if obj != {} and 'umpireName' in obj and type(obj) != KeyError]

			cache[cache['use']]['/games'] = games
			print('Cached Only Games in {0}s'.format(time.time() - time_stamp))
		except Exception as e:
			print(f'Error while recaching games: {e}')



