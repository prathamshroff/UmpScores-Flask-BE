import flask
from flask import Flask, jsonify, request
import json
import sys, os
import boto3
import decimal 
import time
from boto3.dynamodb.conditions import Key, Attr
sys.path.append('./src')
from Umpires import getUmpires
from umpires_dataset import Dataset
from umpiresearch import Search
with open('.config.json') as f:
	configs = json.load(f)

# TODO list comprehension instead of decimal encoder?
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

app = Flask(__name__)
app.json_encoder = DecimalEncoder
app.config["DEBUG"] = True

# Custom boto3 wrappers. Will handle data sanitization and malicious queries in the future
umpires_dataset = Dataset(configs['iam-user'], 'Refrating-Umpires')
games_dataset = Dataset(configs['iam-user'], 'Refrating-Games')
umpires_text_search = Search(configs['iam-user'], configs['cloud-search']['umpires-url'])
games_text_search = Search(configs['iam-user'], configs['cloud-search']['games-url'])

@app.route('/search', methods=['GET'])
def search():
	if request.method == 'GET':
		query = request.args.get('search')
		data = umpire_text_search.get(query) + games_text_search.get(query)
		return jsonify(data), 200

@app.route('/get-all-umps', methods=['GET'])
def getAllUmps():
	if request.method == 'GET':
		data = jsonify(umpires_dataset.scan())
		return data, 200

@app.route('/get-games', methods=['GET'])
def getGames():
	if request.method == 'GET':
		try:
			start = int(request.args.get('start'))
			end = int(request.args.get('end'))
		except ValueError as e:
			return 'Please give start and end number fields', 200

		filterExpression = Attr('timeStamp').between(start, end)
		data = jsonify(
			games_dataset.scan(FilterExpression=filterExpression)
		)
		return data, 200

if __name__ == '__main__':
	# getUmpires()
	# app.run('0.0.0.0', port=80)
	app.run('127.0.0.1', port=3000)
