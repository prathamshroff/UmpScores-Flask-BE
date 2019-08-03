import pandas as pd
import sys
import math
import simplejson as json
sys.path.append('../src')
from CloudSearch import Search
from Datasets import Table
raw_filepath = 'datasets/raw/umpire2019.xlsx'
refined_filepath = 'datasets/refined/umps2019.csv'

config = eval(open('../.config.json').read())
iam = config["iam-user"]

if __name__ == '__main__':
	cloudsearch = Search(iam, config['cloudsearch']['umpires-url'])
	table = Table(iam, 'refrating-umpires-v1', cloudsearch)

	table.make_umps()

	table.clearTable('ump', 'number')
	table.uploadUmpires(refined_filepath)

	cloudsearch.emptyCloudSearch()
	cloudsearch.refreshCloudSearch()
 