import pandas as pd
import sys
import os
import boto3
sys.path.append('../src')
from AWS.CloudSearch import Search
from AWS.Datasets import Table
###output-data/Career/BCR By Season/BCR_among_all_umps.csv GIVE TOTAL A NAME
def pickle(f, kwargs):
	return f(**kwargs)

def single_file_upload(table, filepath, primary_key, output_filepath=None, 
	columns_to_rename={'ump':'name'}, sort_key=None, reader=pd.read_csv, clear=True):
	"""
	Uploads a singular file to the given dynamodb table. 

	Parameters
	----------
	table : Table
		table is the corresponding dynamodb table to upload to. Table object is found in
		AWS.Datasets.Table
	filepath : str
		filepath for the file which we are uploading
	primary_key : str
		primary_key is the primary key name for this table.
			e.g. 'name' or 'game'
	output_filepath : str or None
		output_filepath is the file in which we'll save the cleaned file. If left
		as None, output_filepath will be set to the input filepath
	columns_to_rename : Dict[str, str]
		columns_to_rename is a dictionary mapping deprecated_column_names to new_column_name
	sort_key : str or None
		sort_key represents whether this table requires a sort_key. Leave as None if table does
		not have a sort key. Common sort keys include 'season' and 'data_year'
	reader : f(filename: str) -> pd.DataFrame
	"""
	if output_filepath == None:
		output_filepath = filepath
	print('Starting upload for {0}'.format(output_filepath))

	df = reader(filepath)
	df.rename(columns = columns_to_rename, inplace=True)
	df = Table.fillna(df, [])
	if 'Unnamed: 0' in df.columns:
		df.drop(columns=['Unnamed: 0'], inplace=True)
	if 'name' in df.columns:
		try:
			df['name'] = df['name'].apply(lambda row: row.lower())
		except Exception as e:
			print(e)
	
	df.to_csv(output_filepath)
	if clear:
		table.clear(primary_key, sort_key = sort_key)

	table.upload(output_filepath)
	print('Uploaded {0} to {1}\n'.format(output_filepath, table))

def simple_merge_folder(table, root, primary_key, get_season, columns_to_rename={'ump':'name'},
	sort_key=None, exclude_files = ['merged.csv']):
	"""
	Uploads simple folders to a designated dynamodb table. 

	Parameters
	----------
	table : Table
		table is the corresponding dynamodb table to upload to. Table object is found in
		AWS.Datasets.Table
	root : str
		root is the root folder directory to upload content from
	primary_key : str
		primary_key is the primary key name for this table.
			e.g. 'name' or 'game'
	get_season : lambda(filename: str) -> str
		get_season should be a lambda expression. It will take a filename and
		it should return a string indicating the year/season of that filenames content.
	columns_to_rename : Dict[str, str]
		columns_to_rename is a dictionary mapping deprecated_column_names to new_column_name
	sort_key : str or None
		sort_key represents whether this table requires a sort_key. Leave as None if table does
		not have a sort key. Common sort keys include 'season' and 'data_year'
	exclude_files : List[str]
		exclude_files represents a list of files we do not want to compress into the merged.csv file.
		You should ALWAYS have merged.csv within this list otherwise you may upload duplicate content

	Algorithm
	----------
	We define a simple folder as a folder with no sub folders, all contents are files whose filenames
	have year within that filename such that we may easily extract season column names.
	We then parallelize the upload process for all of these files
	"""
	files = [os.path.join(root, file) for file in os.listdir(root) if file not in exclude_files]
	merge = pd.read_csv(files[0])
	merge[sort_key] = len(merge) * [get_season(files[0])]
	merge.rename(columns=columns_to_rename, inplace=True)	
	for i in range(len(files))[1:]:
		if files[i] not in exclude_files:
			# added try: except to deal with .DS_Store files because my (chris's) computer is stupid
			try:
				df = pd.read_csv(files[i])
				df.rename(columns=columns_to_rename, inplace=True)
				df[sort_key] = len(df) * [get_season(files[i])]
				merge = pd.concat((merge, df))
				merge = Table.drop_y(merge)
			except Exception as e:
				print(e)
	output = os.path.join(root, 'merged.csv')

	if 'Unnamed: 0' in merge.columns:
		merge.drop(columns=['Unnamed: 0'], inplace=True)
	if 'name' in merge.columns:
		merge['name'] = merge['name'].apply(lambda row: row.lower())

	merge = Table.fillna(merge, [])
	merge.to_csv(output)
	table.clear(primary_key, sort_key=sort_key)
	table.upload(output)
	print('Uploaded {0} to {1}'.format(output, table))
