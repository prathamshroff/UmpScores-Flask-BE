from flask import Flask
import pytest
import sys
sys.path.append('./src')
from Schedulers.Scheduler import CacheScheduler
import simplejson as json

app = Flask(__name__)

with open('.config.json') as f:
	configs = json.load(f)
	secret = configs['privilege_secret']

def test_index():
    cache_sched = CacheScheduler('http://0.0.0.0:80', secret, freq=120)

    app.config["DEBUG"] = True
    client = app.test_client()
    response = client.get('/')
    assert response.status_code == 200


test_index()