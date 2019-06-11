import flask
from flask import make_response, request, current_app, jsonify
import json
import sys, os

sys.path.append('./src') # get imports frmo path /src
from Umpires import getUmpires
from Datasets import Dataset

# Get creds
with open('config.json') as f:
	configs = json.load(f)

# Backend Init Stuff
app = flask.Flask(__name__)
app.config["DEBUG"] = True
datasets = Dataset(configs['iam-user'])

@app.route('/', methods=['GET'])
def home():
	item = datasets.get({
			'umpires':'Mason'
		})
	item.pop('aws:rep:updatetime') # Remove decimal object which cannot be jsonify'd
	return jsonify(item)

if __name__ == '__main__':
	# getUmpires()
	# app.run('0.0.0.0', port=80)
	app.run('127.0.0.1', port=3000)








