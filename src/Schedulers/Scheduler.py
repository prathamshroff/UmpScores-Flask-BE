from multiprocessing import Process
import time
from requests import get
from typing import Dict, Tuple, Sequence
import os
class CacheScheduler():
	def __init__(self, url: str, secret: str):
		self.url = url
		self.secret = secret
		toMins = lambda ms: ms * 60
		Process(target=self.call_endpoint, args=('/updateGamesCache', {'secret': self.secret}, 
			toMins(15))).start()

	def call_endpoint(self, endpoint: str, params: Dict[str, str], freq: int):
		"""
		Parameters
		----------
		endpoint : str
			string object representing which endpoint to call e.g. /updateGameCache
		params : dict
			dictionary representing get request arguments
		freq : int
			frequency in seconds representing how long in between calling said endpoint
		"""
		if endpoint[0] == '/':
			endpoint = endpoint[1:] # don't want /game we want game

		if not endpoint.startswith(self.url):
			endpoint = os.path.join(self.url, endpoint)

		while True:
			print('CacheScheduler daemon calling {0}'.format(endpoint))
			time.sleep(freq)
			get(endpoint, params=params)


