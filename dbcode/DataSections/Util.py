import pandas as pd
import sys
import os
import boto3
sys.path.append('../src')
from AWS.CloudSearch import Search
from AWS.Datasets import Table
def pickle(f, kwargs):
	return f(**kwargs)

def single_file_upload(table, filepath, primary_key, output_filepath=None, 
	columns_to_rename={'ump':'name'}, sort_key=None, reader=pd.read_csv, clear=True):
	if output_filepath == None:
		output_filepath = filepath

	df = reader(filepath)
	df.rename(columns = columns_to_rename, inplace=True)
	df = Table.fillna(df, [])
	if 'Unnamed: 0' in df.columns:
		df.drop(columns=['Unnamed: 0'], inplace=True)
	df.to_csv(output_filepath)
	if clear:
		table.clear(primary_key, sort_key = sort_key)
	table.upload(output_filepath)
	print('Uploaded {0} to {1}'.format(output_filepath, table))

def simple_merge_folder(table, root, primary_key, get_season, columns_to_rename={'ump':'name'},
	sort_key=None, exclude_files = ['merged.csv']):
	files = [os.path.join(root, file) for file in os.listdir(root) if file not in exclude_files]
	merge = pd.read_csv(files[0])
	print(files)
	merge[sort_key] = len(merge) * [get_season(files[0])]
	merge.rename(columns=columns_to_rename, inplace=True)	
	for i in range(len(files))[1:]:
		if files[i] not in exclude_files:
			# added try: except to deal with .DS_Store files because my (chris's) computer is stupid
			try:
				df = pd.read_csv(files[i])
				df.rename(columns=columns_to_rename, inplace=True)
				print(sort_key)
				df[sort_key] = len(df) * [get_season(files[i])]
				merge = pd.concat((merge, df))
				merge = Table.drop_y(merge)
			except Exception as e:
				print(e)
	output = os.path.join(root, 'merged.csv')

	if 'Unnamed: 0' in merge.columns:
		merge.drop(columns=['Unnamed: 0'], inplace=True)
	merge = Table.fillna(merge, [])
	merge.to_csv(output)
	table.clear(primary_key, sort_key=sort_key)
	table.upload(output)
	print('Uploaded {0} to {1}'.format(output, table))
