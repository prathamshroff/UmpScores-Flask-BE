import pandas as pd
import os
def __fillna(df, string_fields):
	"""Fills in empty fields with -1 for number values and 'n/a' for strings
	"""
	values = {key: 'n/a' for key in string_fields}

	number_fields = list(df.columns)
	for column in string_fields:
		number_fields.remove(column)

	values.update({key: -1 for key in number_fields})

	df = df.fillna(value=values)
	return df

def make_umps():
	"""Makes refined/2019.csv
	"""
	string_fields = [
		'Data source',
		'ump',
		'name'
	]
	profiles = pd.read_excel('datasets/raw/umpire2019.xlsx')
	stats = pd.read_csv('datasets/raw/umpire_bcr_2019.csv')
	df = pd.merge(profiles, stats, left_on='ump', right_on='name')	
	dataset = __fillna(df, string_fields)
	dataset = dataset.drop(columns=['Unnamed: 0', 'name'])
	dataset.to_csv('datasets/refined/umps2019.csv', index=False)
	print(dataset.columns)

if __name__ == '__main__':
	make_umps()