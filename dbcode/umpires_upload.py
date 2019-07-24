import pandas as pd
import sys
import math
import simplejson as json
from preprocessors.CloudSearch import CloudSearchPP
from preprocessors.Dynamodb import DynamodbPP
import preprocessors.Datasets as RefratingDatasets

raw_filepath = 'datasets/raw/umpire2019.xlsx'
refined_filepath = 'datasets/refined/umps2019.csv'

config = eval(open('../.config.json').read())
iam = config["iam-user"]

if __name__ == '__main__':
	cloudsearchpp = CloudSearchPP(config, iam)
	table = DynamodbPP(config, iam, 'refrating-umpires-v1', cloudsearchpp)

	RefratingDatasets.make_umps()

	table.clearTable('ump', 'number')
	table.uploadUmpires(refined_filepath)

	cloudsearchpp.emptyCloudSearch()
	cloudsearchpp.refreshCloudSearch()
