from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
import copy

def getUmpires():
	html = urlopen("http://www.statfox.com/mlb/umpiremain.asp")
	soup = BeautifulSoup(html)
	# class = datatable
	table = soup.find_all('table', class_="datatable")
	umpireData = {}
	key_order = []

	# Get all table headers and store in array
	for row in table[0].findAll("th"):
		key = row.text.split("\n")
		key = key[0].replace("/", "").replace(" ", "_").lower()
		umpireData[key] = []
		key_order.append(key)

	# Clean the headers I don't care about
	bad_headers = ["", "home_plate_umpire", "scoring", "home_team_record", "average_game_stats"]
	for bad_header in bad_headers:
		if bad_header in umpireData:
			del umpireData[bad_header]
			key_order.remove(bad_header)

	# Put all of the table cell values in an array
	all_cells = []
	for td in table[1].findAll("td"):
		# this actually goes through each cell that I need, this is the function I need to have
		value = td.text
		value = value.replace(u'\xa0', ' ')
		value = value.replace("\n", "")
		value = value.replace("\r\n", "")
		value = value.replace("\r", "")
		all_cells.append(value)
	
	# Clean the duplicates
	clean_data = []
	duplicate_count = 0
	i = 0
	for item in all_cells:
		if (i == 0):
			clean_data.append(item)
			i += 1
		else:
			if (all_cells[i - 1] == item):
				if ("+" in item):
					duplicate_count += 1
					i += 1
				elif ("-" in item):
					duplicate_count += 1
					i += 1
				else:
					clean_data.append(item)
					i += 1
			else:
				clean_data.append(item)
				i += 1
	# iterate through and place the items where they need to in a dictionary
	final_dict = copy.copy(umpireData)
	j = 0
	for item in clean_data:
		key = key_order[j%15]
		final_dict[key].append(item)
		j += 1
	print(final_dict)