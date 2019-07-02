from flask import Flask, jsonify, request, Response
import simplejson as json
import sys, os
import boto3
import time
from boto3.dynamodb.conditions import Key, Attr
sys.path.append('./src')
from Datasets import Dataset
from CloudSearch import Search
with open('.config.json') as f:
	configs = json.load(f)

app = Flask(__name__)
app.config["DEBUG"] = True

# Custom boto3 wrappers. Will handle data sanitization and malicious queries in the future
umpires_dataset = Dataset(configs['iam-user'], 'refrating-umpires-v1-search')
games_dataset = Dataset(configs['iam-user'], 'Refrating-Games')
umpires_text_search = Search(configs['iam-user'], configs['cloud-search']['umpires-url'])
games_text_search = Search(configs['iam-user'], configs['cloud-search']['games-url'])

# TODO Remove this ('/') endpoint for future versions
@app.route('/', methods=['GET'])
def getUmpire():
	"""
	Test URL to ensure dynamodb connection is stable

	Response
	----------
	If this endpoint is working you should get this exact json object:
	{
		"ump": "Bill Welke",
		...
	}
	""" 
	if request.method == 'GET':
		data = json.dumps(umpires_dataset.get({"ump":"Bill Welke"}), use_decimal=True)
		resp = Response(data, status=200, mimetype='application/json')
		return resp

@app.route('/search', methods=['GET'])
def search():
	"""
	Search URL which takes a string in the form of a get request and
	compares that query against a dynamodb table by interfacing through CloudSearch

	Get
	----------
	search : string
		query string which will be sent to aws cloudsearch to find relevant search
		results.
			e.g. ?search="jordan baker"

	Response
	----------
	{"umpire-search-results": [{...}, {...}, ...], "game-search-results": [{...}, {...}, ...]}
		The "umpires" value and the "games" value are similarly formatted such that
		each array contains several dictionaries representing relevant search results. Left most
		indices contain more relevant search results. Each dictionary would be an entire data entry 
		from their respective table. For example, a dictionary under the umpires key
		would represent all of the statistics about some arbitrary umpire while in 
		games one dictionary would represent all the statistics about some game. Finally,
		every dictionary has a _score key with a decimal value representing how relevant
		that score is. As stated before, left most indices will have higher scores because
		they are more relevant.
			e.g. 
			{
				"umpire-search-results": 
					[
						{
							"ump": "Bill Welke", 
							...
							"_score": "0.7546"
						}, 
						...
					], 
				"game-search-results": [...]
			}
	"""
	if request.method == 'GET':
		query = request.args.get('search')
		data = {
			'umpire-search-results': umpires_text_search.get(query), 
			'game-search-results': games_text_search.get(query)
		}
		data = json.dumps(data, use_decimal=True)
		resp = Response(data, status=200, mimetype='application/json')
		return resp

@app.route('/get-all-umps')
def getAllUmps():
	"""
	Returns every umpire within the Umpires dynamodb table

	Response
	----------
	array of dictionaries
		Every dictionary within this array contains all of the statistics
		regarding a singular umpire. The response will resemble the following:
			e.g. 
			[
				{
					"Umpires": "jordan baker"
					"attr1": "val1",
					"attr2": "val2",
					...
				}, ...
			]
	"""
	data = json.dumps(umpires_dataset.scan(), use_decimal=True)
	resp = Response(data, status=200, mimetype='application/json')
	return resp

# TODO handle start and end times differently with actual time objects 
# instead of just integers
@app.route('/get-games', methods=['GET'])
def getGames():
	"""
	Returns all the data from every game within some given time frame

	Get
	----------
	start : integer
		beginning pointer for the time frame of which we will select games from
	end : integer
		ending pointer for the time frame of which we will select games from

	Response
	----------
	array of dictionaries
		Every dictionary within this array contains all of the statistics
		regarding a singular game. Every game that occurred within the start
		and end parameters timeframe will be selected for this array.
		The response will resemble the following:
			e.g. 
			[
				{
					"Games": "Game1"
					"Umpire": "ryan additon",
					...
				}, ...
			]
	"""
	if request.method == 'GET':
		try:
			start = int(request.args.get('start'))
			end = int(request.args.get('end'))
		except ValueError as e:
			return 'Please give start and end number fields', 200

		filterExpression = Attr('timeStamp').between(start, end)
		data = json.dumps(
			games_dataset.scan(FilterExpression=filterExpression), use_decimal=True
		)
		resp = Response(data, status=200, mimetype='application/json')
		return resp

if __name__ == '__main__':
	# getUmpires()
	# app.run('0.0.0.0', port=80)
	app.run('127.0.0.1', port=3000)
