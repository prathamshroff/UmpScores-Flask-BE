import flask
from flask import make_response, request, current_app, jsonify
import json
from Umpires import getUmpires
import sys
from Datasets import Dataset

app = flask.Flask(__name__)
app.config["DEBUG"] = True
datasets = Dataset()

@app.route('/', methods=['GET'])
def home():
	item = dataset.refrating.get_item(Key={
			'umpires':'Mason'
		})['Item']
	return jsonify(item)

if __name__ == '__main__':
	# getUmpires()
	# app.run('0.0.0.0', port=80)
	app.run('127.0.0.1', port=3000)








