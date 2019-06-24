import flask
from flask import Flask, jsonify, request
import json
import sys, os
import boto3

sys.path.append('./src') # get imports frmo path /src
from Umpires import getUmpires
from Datasets import Dataset
from CloudSearch import Search
# Get creds
with open('.config.json') as f:
	configs = json.load(f)

# Backend Init Stuff
app = Flask(__name__)
app.config["DEBUG"] = True
datasets = Dataset(configs['iam-user'], 'Umpires')
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

if __name__ == '__main__':
	# getUmpires()
	# app.run('0.0.0.0', port=80)
	app.run('127.0.0.1', port=3000)








