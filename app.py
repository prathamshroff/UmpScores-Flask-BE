import simplejson as json
from Schedulers.Scheduler import CacheScheduler
from multiprocessing import Process
from routes import *
import sys
sys.path.append('./src')

with open('.config.json') as f:
    configs = json.load(f)
    secret = configs['privilege_secret']

if __name__ == '__main__':

    if len(sys.argv) > 1:
        if 'p' in sys.argv:
            cache_sched = CacheScheduler('http://0.0.0.0:80', secret, freq=120)
            print('Starting in production mode')
            app.config["DEBUG"] = False
            app.run('0.0.0.0', port=80)
    else:
        cache_sched = CacheScheduler('http://127.0.0.1:3000', secret, freq=120)
        print('Starting in testing mode')
        app.config["DEBUG"] = True
        app.run('127.0.0.1', port=3000)
