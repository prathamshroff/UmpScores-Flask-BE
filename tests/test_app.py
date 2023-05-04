import sys
sys.path.append('../')

import unittest
from unittest.mock import patch
from flask import Flask
# import app


class TestApp(unittest.TestCase):

    @patch('app.CacheScheduler')
    @patch('app.app.run')
    def test_app_testing_mode(self, mock_run, mock_cache_scheduler):
        app = Flask(__name__)
        with patch('sys.argv', ['app.py']):
            app.main()
            mock_run.assert_called_once_with('127.0.0.1', port=3000)

    @patch('app.CacheScheduler')
    @patch('app.app.run')
    def test_app_production_mode(self, mock_run, mock_cache_scheduler):
        app = Flask(__name__)
        with patch('sys.argv', ['app.py', 'p']):
            app.main()
            mock_run.assert_called_once_with('0.0.0.0', port=80)

if __name__ == "__main__":
    unittest.main()
