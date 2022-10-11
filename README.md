# RefRating Back-end Repo

### Getting Started On Linux/Mac:
I will assume you have pip and python3 set up on your local machine

1. `git clone https://github.com/chris-ackerman/refrating-be`
2. `cd refrating-be`
3. Make sure .config.json appears after this command: `ls -la | grep .config.json` otherwise contact curtis or chris
4. Install virtualenv: `pip install virtualenv`
5. `virtualenv -p python3 env`
6. `source env/bin/activate`
7. Install MLB Stats API: `pip install MLB-StatsAPI`
8. Install the required packages with the command `pip install -r requirements.txt`
   1. As of 4/5/2022 the current version of flask-restplus(0.13.0) has a few issues. You will need to change the following:
      1. `from werkzeug import cached_property` to `from werkzeug.utils import cached_property` in the file 'flask_restplus/fields.py' and in the file 'flask_restplus/api.py'
      2. `from werkzeug.wrappers import BaseResponse` to `from werkzeug.wrappers import Response as BaseResponse` in the file 'flask_restplus/api.py' and in the file 'flask_restplus/resource.py'
      3. `from flask.helpers import _endpoint_from_view_func` to `from flask.scaffold import _endpoint_from_view_func` in the file 'flask_restplus/api.py'

 
9. Windows: `python3 app.py`, Linux/Mac: `make`

### Start Production Version on EC2
1. `tmux attach`
2. `make pid`
3. For every Python pid instance excluding grep command: `sudo kill -9 <pid>`
2. `make prod`


