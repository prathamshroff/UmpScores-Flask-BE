# RefRating Back-end Repo

### Getting Started:
I will assume you have pip and python3 set up on your local machine

1. `git clone https://github.com/chris-ackerman/refrating-be`
2. `cd refrating-be`
3. Make sure .config.json appears after this command: `ls | grep config` otherwise contact curtis or chris
4. Install virtualenv: `pip install virtualenv`
5. `virtualenv -p python3 env`
6. `source env/bin/activate`
7. Install the required packages with the command `pip install -r requirements.txt`
8. Windows: `python3 app.py`, Linux/Mac: `make`

### Start Production Version on EC2
1. `tmux attach`
2. `make pid`
3. For every Python pid instance excluding grep command: `sudo kill -9 <pid>`
2. `make prod`


