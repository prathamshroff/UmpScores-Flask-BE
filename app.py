import flask
from flask import Flask, jsonify, request
import json
import sys, os
import boto3
import decimal 

sys.path.append('./src') # get imports frmo path /src
from Umpires import getUmpires
from Datasets import Dataset
from CloudSearch import Search
# Get creds
with open('.config.json') as f:
	configs = json.load(f)

# TODO list comprehension instead of decimal crappy encoder
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

# Backend Init Stuff
app = Flask(__name__)
app.config["DEBUG"] = True
datasets = Dataset(configs['iam-user'], 'Refrating-Umpires')
cloudsearch = Search(configs['iam-user'], configs['cloud-search']['umpires-url'])

@app.route('/', methods=['GET'])
def home():
	item = datasets.get({
		'umpires':'Mason'
	})
	item.pop('aws:rep:updatetime') # Remove decimal object which cannot be jsonify'd
	return jsonify(item), 200

@app.route('/search', methods=['GET'])
def search():
	if request.method == 'GET':
		query = request.args.get('search')
		return jsonify(cloudsearch.get(query)), 200

@app.route('/get-all-umps', methods=['GET'])
def getAllUmps():
	if request.method == 'GET':
		data = json.dumps(datasets.all()['Items'], cls=DecimalEncoder)
		return data

if __name__ == '__main__':
	# getUmpires()
	# app.run('0.0.0.0', port=80)
	app.run('127.0.0.1', port=3000)
