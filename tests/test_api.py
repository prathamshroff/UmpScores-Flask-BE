from flask import Flask
import pytest
import sys
sys.path.append('./src')
from Schedulers.Scheduler import CacheScheduler
import simplejson as json
from unittest.mock import patch
import unittest



with open('.config.json') as f:
	configs = json.load(f)
	secret = configs['privilege_secret']

class TestApi(unittest.TestCase):
    
    @patch('app.CacheScheduler')
    @patch('app.app.run')

    def test_app(self, mock_run, mock_cache_scheduler):
        app = Flask(__name__)
        with patch('sys.argv', ['app.py']):
            app.main()
            mock_run.assert_called_once_with('127.0.0.1', port=3000)



if __name__ == "__main__":
    unittest.main()