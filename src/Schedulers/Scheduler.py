from multiprocessing import Process
import time
from requests import get
from typing import Dict, Tuple, Sequence
import os
class CacheScheduler():
	"""
	CacheScheduler class sets up all daemons responsible for calling various cache update endpoints
	for the backend. It creates a soft 'daemon' by executing call_endpoint function on a separate process.
	
	Parameters
	----------
	url : str
		string representing the root url of the server we're going to be calling for preceeded by http:// 
			e.g. http://localhost, http://127.0.0.1:3000, http://ec2-3-228-16-91.compute-1.amazonaws.com/
	secret : str
		secret is a string representing the privileged secret key. This secret key can be found
		in .config.json. Is used to authorize get requests 

	Attributes
	----------
	url : str
		stores given url
	secret : str
		stores given secret
	"""
	def __init__(self, url: str, secret: str):
		self.url = url
		self.secret = secret
		toMins = lambda ms: ms * 60
		Process(target=self.call_endpoint, args=('/updateGamesCache', {'secret': self.secret}, 
			toMins(15))).start()

	def call_endpoint(self, endpoint: str, params: Dict[str, str], freq: int):
		"""
		Periodically sends a get request to this given endpoint. 

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
			time.sleep(freq)
			print('CacheScheduler daemon calling {0}'.format(endpoint))
			get(endpoint, params=params)


