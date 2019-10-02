# TODO /umpire is array (how do I handle season then??)
# TODO uncomment /get-games
from boto3.dynamodb.conditions import Key, Attr
from StorageSolutions.tables import *
# from Util.RefratingCache import TEAM_NAMES
# importing libraries for /games endpoint
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dateutil import parser
from bs4 import BeautifulSoup
import re
import simplejson as json
from flask import Flask, jsonify, request, Response
import time

from decimal import *

# importing things from cache
TEAM_NAMES = [name.replace('total_call_', '') for name in \
    team_stats_dataset.get(query_map = {'name':'jordan baker', 'data_year' : 2019}).keys() if \
    name.startswith('total_call_')]
TEAM_NAMES = [name.split('_')[0] for name in TEAM_NAMES if name.endswith('_')]
# def create_umpire_list():
# 	print(ALL_UMPIRE_KEYS

def create_chart_object(name, year_range):
	name = name.lower()
	filterExpression = Key('name').eq(name)
	data = umpire_zones.query(KeyConditionExpression = filterExpression)
	
	resp = {
		'heatMap': [],
		'heatMapSL': [],
		'heatMapFT': [],
		'heatMapCU': [],
		'heatMapFF': [],
		'heatMapSI': [],
		'heatMapCH': [],
		'heatMapFC': [],
		'heatMapEP': [],
		'heatMapKC': [],
		'heatMapFS': [],
		'heatMapPO': [],
		'heatMapKN': [],
		'heatMapSC': [],
		'heatMapFO': [],
		'heatMapUN': [],
		'heatMapFA': [],
		'heatMapIN': [],
		'allUmpsBcrOverCareer': []
	}
	range_resp = careers_range.get({'name':name})
	resp['bcrOverCareer'] = [{'x': year, 'y': range_resp['BCR_{0}'.format(year)]} for year in \
		year_range if 'BCR_{0}'.format(year) in range_resp]

	resp['seasonalBcrByMonth'] = []
	month_resp = profile_best_worst_month_table.query(KeyConditionExpression = Key('name').eq(name))
	for year_data in month_resp:
		season = year_data['season']


		months = [month.replace('bcr_{0}_'.format(season), '') for \
			month in year_data if month.startswith('bcr_{0}_'.format(season))]

		resp['seasonalBcrByMonth'].append({
			'season': season, 
			'data': [{'x': month, 'y': year_data['bcr_{0}_{1}'.format(season, month)]} for month in months]
		})

		resp['allUmpsBcrOverCareer'].append({'x': season, 'y': year_data['bcr_{0}'.format(season)]})

	for entry in data:
		year = entry['season']
		careers_season_resp = careers_season.get({'name': name, 'data_year': year},
			AttributesToGet = ['BCR_z{0}'.format(i) for i in range(1, 15)])


		resp['heatMap'].append({
			'data': [careers_season_resp['BCR_z{0}'.format(i)] for i in range(1,15) if i != 10],
			'season': year
		})
		resp['heatMapSL'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_SL_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapFT'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_FT_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapCU'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_CU_{0}'.format(i) for i in \
				range(1, 15)if i != 10]], 
			'season': year
		})

		resp['heatMapFF'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_FF_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapSI'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_SI_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapCH'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_CH_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapFC'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_FC_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		# OLD VERSION
		# resp['heatMapEP'].append({
		# 	'data': [entry[elem] for elem in ['BCR_EP_{0}'.format(i) for i in \
		# 		range(1, 15) if i != 10]], 
		# 	'season': year
		# })
		# print("ENTRY: ", [Decimal(entry[elem]).compare(-1) == Decimal('0') for elem in ['BCR_EP_{0}'.format(i) for i in \
		# 		range(1, 15) if i != 10]])
		resp['heatMapEP'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_EP_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapKC'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_KC_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapFS'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_FS_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapPO'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_PO_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapKN'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_KN_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapSC'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_SC_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapFO'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_FO_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapUN'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_UN_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapFA'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_FA_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapIN'].append({
			'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_IN_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

	return resp

def get_event_lines(event_id):
	resp = {}
	# old token: !m5__dQ_ZN-aH-v4
	# new token: !FSUT!-5S_7ogtMs
	xmlData = requests.get("http://xml.donbest.com/v2/odds/5/" + event_id +"/?token=!FSUT!-5S_7ogtMs")
	root = ET.fromstring(xmlData.text)
	try:
		for line in root.iter("line"):
			if (line.get("period") == "FG" and line.get("type") == "previous"):
				awayLine = line.find("money").get("away_money")
				homeLine = line.find("money").get("home_money")
				resp["awayLine"] = awayLine
				resp["homeLine"] = homeLine
				# awayLine & homeLine
		# USE PERIOD = FG, WHICH MEANS FULL GAME
		if (len(resp) > 0):
			resp["status"] = 200
		else:
			resp["status"] = 404
	except Exception as e:
		print("EXCEPTION: ", e)
	return resp

def get_team_abbreviation(team):
	abbreviation_dict = {
		"Arizona Diamondbacks":"ARI",
		"Atlanta Braves": "ATL",
		"Baltimore Orioles": "BAL",
		"Boston Red Sox": "BOS",
		"Chicago Cubs": "CHC",
		# chicago white sox also use CHW but we use CWS
		"Chicago White Sox": "CWS",
		"Cincinnati Reds": "CIN",
		"Cleveland Indians": "CLE",
		"Colorado Rockies": "COL",
		"Detroit Tigers": "DET",
		"Miami Marlins": "FLA",
		"Houston Astros": "HOU",
		"Kansas City Royals": "KAN",
		# also called Los Angeles Angels of Anaheim, might need to do something with that
		"Los Angeles Angels": "LAA",
		"Los Angeles Dodgers": "LAD",
		"Milwaukee Brewers": "MIL",
		"Minnesota Twins": "MIN",
		"New York Mets": "NYM",
		"New York Yankees": "NYY",
		"Oakland Athletics": "OAK",
		"Philadelphia Phillies": "PHI",
		"Pittsburgh Pirates": "PIT",
		"San Diego Padres": "SD",
		"San Francisco Giants": "SF",
		"Seattle Mariners": "SEA",
		"St. Louis Cardinals": "STL",
		"Tampa Bay Rays": "TB",
		"Texas Rangers": "TEX",
		"Toronto Blue Jays": "TOR",
		"Washington Nationals": "WAS"
	}
	if (len(team) > 3):
		try:
			abbreviated = abbreviation_dict[team]
		except Exception as e:
			return e
	return abbreviated

def get_umpires_for_games():
	page = requests.get("https://www.statfox.com/mlb/umpiremain.asp")
	soup = BeautifulSoup(page.text)
	tables = soup.findAll("table")
	cells = tables[2].findAll("td")
	headerObject = tables[2].findAll("th", { "class" : "header1" })
	headerArray = []
	for header in headerObject:
		headerArray.append(header.text)
	ump_games = {}
	current_key = "trash"
	for item in cells:
		#regex pattern: [A-Z][A-Z][A-Z].at.[A-Z][A-Z][A-Z]
		# key is a match, change new key
		if (re.match("[A-Z][A-Z][A-Z].at.[A-Z][A-Z][A-Z]", item.text.strip())):
			current_key = item.text.strip()
		# no new key, check if key exists
		else:
			if (current_key in ump_games):
				ump_games[current_key].append(item.text.strip())
			else:
				ump_games[current_key] = [item.text.strip()]
	# empty out the trash key
	ump_games["trash"] = ""
	# make a dictionary with the team names as keys, storing the last used key until a new one is found and then replacing it
	return ump_games

def format_umpire_name(ALL_UMPIRE_NAMES, name):
	umpire_name = ""
	if (name != "Umpire information is NOT AVAILABLE for this game"):
		last_name = name[1:].title().strip().lower()
		for item in ALL_UMPIRE_NAMES:
			if (last_name in item):
				umpire_name = item
	else:
		umpire_name = "NA"
	return umpire_name

def get_game_values(ALL_UMPIRE_NAMES, ump_table, event):
	resp = {}
	event_lines = get_event_lines(event.get("id"))
	try:
		if (event_lines["status"] == 200):
			resp["awayLine"] = event_lines["awayLine"]
			resp["homeLine"] = event_lines["homeLine"]
			print("LINES: ", resp)
		# now we can query the money lines in a new function
		for participant in event.iter("participant"):
			# get side + team
			side = participant.get("side").lower()
			team = get_team_abbreviation(participant.find("team").get("name"))
			resp[side + "Team"] = team
			# need to run the "value" part of this through a dictionary to get the right team abbreviations
			# get pitcher name + arm
			pitcher = participant.find("pitcher").text.title()
			resp[side + "PitcherName"] = pitcher
			pitcherArm = participant.find("pitcher").get("hand")[0]
			resp[side + "PitcherArm"] = pitcherArm
		# get_umpire_for_game(resp["awayTeam"], resp["homeTeam"])
		location = event.find("location").get("name")
		resp["location"] = location
		dateString = event.get("date")
		dateCorrected = parser.parse(dateString)
		dateCorrectedString = dateCorrected.strftime("%Y-%m-%dT%H:%M:%S")
		resp["date"] = dateCorrectedString
		found = 0
		for key in ump_table.keys():
			print("GETTING TO  THE KEY IN UMP_TABLE.keys() point")
			if (resp["awayTeam"] in key and resp["homeTeam"] in key):
				# grab the umpire value in ump_table
				resp["umpireName"] = format_umpire_name(ALL_UMPIRE_NAMES, ump_table[key][1])
				print("UMP NAME: ", resp["umpireName"])
				found = 1
		swaps = {"CWS":"CHW", "CHW":"CWS", "TB":"TAM", "TAM":"TB", "FLA":"MIA", "MIA":"FLA"}
		# means there might be mismatched team abbreviations
		if (found == 0):
			if (resp["awayTeam"] in swaps):
				for key in ump_table.keys():
					if (swaps[resp["awayTeam"]] in key and resp["homeTeam"] in key):
						# grab the umpire value in ump_table
						resp["umpireName"] = format_umpire_name(ALL_UMPIRE_NAMES, ump_table[key][1])
			if(resp["homeTeam"] in swaps):
				# MODIFY HOME TEAM
				for key in ump_table.keys():
					if (resp["awayTeam"] in key and swaps[resp["homeTeam"]] in key):
						# grab the umpire value in ump_table
						resp["umpireName"] = format_umpire_name(ALL_UMPIRE_NAMES, ump_table[key][1])
	except Exception as e:
		print("EXCEPTION: ", e)
	return resp
'''
<event id="970457" season="REGULAR" date="2019-09-26T16:35:00+0" name="Milwaukee Brewers vs Cincinnati Reds">
	<event_type>team_event</event_type>
	<event_state>PENDING</event_state>
	<event_state_id>0</event_state_id>
	<time_changed>false</time_changed>
	<neutral>false</neutral>
	<game_number>1</game_number>
	<location name="Great American Ballpark" id="650" link="/v2/location/650"/>
	<participant rot="901" side="AWAY">
		<team id="1298" name="Milwaukee Brewers" link="/v2/team/1298"/>
		<pitcherChanged>false</pitcherChanged>
		<pitcher hand="RIGHT" id="342944" full_name="Chase Anderson">C ANDERSON</pitcher>
	</participant>
	<participant rot="902" side="HOME">
		<team id="1295" name="Cincinnati Reds" link="/v2/team/1295"/>
		<pitcherChanged>false</pitcherChanged>
		<pitcher hand="RIGHT" id="342058" full_name="Luis Castillo">L CASTILLO</pitcher>
	</participant>
	<live>true</live>
	<lines>
		<current link="/v2/odds/5/970457"/>
		<opening link="/v2/open/5/970457"/>
	</lines>
	<score link="/v2/score/970457"/>
	<pitcher_changed>false</pitcher_changed>
</event>
		'''

def verifyGameData(event_info):
	keys = ["awayLine", "homeLine", "awayTeam", "awayPitcherName", "awayPitcherArm", "homeTeam", "homePitcherName", "homePitcherArm", 
	"location", "date", "umpireName"]
	for key in keys:
		if key in event_info:
			# key is there, check if info is good
			if (event_info[key] != ""):
				# data is not empty, check next key
				return True
	# return False if conditions are not met above
	return False

def get_all_games(ALL_UMPIRE_NAMES, q):
	games = []
	# storing this to pass to get_game_values so I can get the right data back
	ump_table = get_umpires_for_games()
	xmlData = requests.get("http://xml.donbest.com/v2/schedule/?token=!FSUT!-5S_7ogtMs")
	root = ET.fromstring(xmlData.text)
	count = 0
	try:
		for league in root.iter("league"):
			if (league.get("name") == "Major League Baseball"):
				for event in league.iter("event"):
					event_type = event.findall("event_type")[0].text
					if (event_type == "team_event"):
						test = event.get("date")
						testDate = parser.parse(test)
						corrected = testDate - timedelta(hours=4, minutes=0)
						dateReal = corrected.strftime("%Y-%m-%d")
						dateRealObject = datetime.strptime(dateReal, "%Y-%m-%d").date()
							# subtract four from date object to correct for UTC time
						dateObject = event.get("date").split("T")
						date = datetime.strptime(dateObject[0],"%Y-%m-%d").date()
						today = date.today()
						# compare date to today
						if (today == dateRealObject):
							count += 1
							print("EVENT: ", event.attrib)
							# 2018-11-15T12:54:55.604Z
							# pass event object for further parsing
							event_info = get_game_values(ALL_UMPIRE_NAMES, ump_table, event)
							if (verifyGameData(event_info)):
								games.append(event_info)
	except Exception as e:
		print("EXCEPTION: ", e)
	q.put(games)


def get_pitcher_names(name):
	name = name.lower()
	names = set()
	resp = umpire_pitchers.query(KeyConditionExpression = Key('name').eq(name))
	for page in resp:
		pitchers = page.keys()
		subnames = [name.replace('total_call_', '').replace('.', ' ') for name in pitchers if \
			name.startswith('total_call_')]
		names = names.union(subnames)
	return list(names)


def create_pitcher_object(umpire_name, pitcher_name):
	pitcher_name = pitcher_name.lower()
	umpire_name = umpire_name.lower()

	data = pitcher_stats.query(
		KeyConditionExpression = Key('name').eq(pitcher_name)
	)
	resp = []
	for pitcher in data:
		obj = {}
		# if pitcher['season'] != 2010 and pitcher['season'] - 1 in data:
		# 	obj['seasonChangeIcr'] = data[season - 1]['BCR']

		obj = {
			'season': pitcher['season'], 
			'name': pitcher['name'],
			'pitchesCalled': pitcher['total_call'],
			'icr': pitcher['BCR'],
			'icrSL': pitcher['BCR_SL'],
			'icrFT': pitcher['BCR_FT'],
			'icrCU': pitcher['BCR_CU'],
			'icrFF': pitcher['BCR_FF'],
			'icrSI': pitcher['BCR_SI'],
			'icrCH': pitcher['BCR_CH'],
			'icrFC': pitcher['BCR_FC'],
			'icrEP': pitcher['BCR_EP'],
			'icrKC': pitcher['BCR_KC'],
			'icrFS': pitcher['BCR_FS'],
			# 'icrPO': pitcher['BCR_PO'],
			'icrKN': pitcher['BCR_KN'],
			# 'icrSC': pitcher['BCR_SC'],
			'icrFO': pitcher['BCR_FO'],
			# 'icrUN': pitcher['BCR_UN'],
			# 'icrFA': pitcher['BCR_FA'],
			# 'icrIN': pitcher['BCR_IN'],
			'ballsCalled': pitcher['call_ball'],
			'strikesCalled': pitcher['call_strike'],
			'blindSpot': pitcher['blindspot_pitch']
		}
		resp.append(obj)
	return resp

def columns_rename(d, columns_map):
	for key in columns_map:
		if key in d:
			d[columns_map[key]] = d.pop(key)
		else:
			d[columns_map[key]] = -1

def create_rankings_object(name, year_range):
	subarr = []
	name = name.lower()
	parts = name.split()
	# for year in year_range:
	career_resp = careers_season.query(
		KeyConditionExpression = Key('name').eq(name)
	)
	resp_2019 = umpires_2019_table.get({'name':name}, AttributesToGet=['age'])
	if 'age' in resp_2019:
		age = resp_2019['age']
	else:
		age = -1

	for resp in career_resp:
		resp = {key: resp[key] for key in ['name', 'data_year', 'total_call', 'bad_call_ratio', 
			'games', 'bad_call_per_game', 'bad_call_per_inning']}
		if career_resp != {}:
			team_preference_resp = profile_team_preference_table.get(
				{'name':name,'season':resp['data_year']},
				AttributesToGet = ['mostAccurateTeam', 'leastAccurateTeam']
			)
			ejections_resp = ejections_table.get(
				{
					'name':name
				},
				AttributesToGet = ['ej_{0}'.format(resp['data_year'])]
			)
			crew_update_get_resp = crew_update_table.get({'name': resp['name'], 'season': resp['data_year']},
				AttributesToGet=['status', 'crew.number'])
			resp.update(team_preference_resp)
			resp.update(ejections_resp)
			resp.update(crew_update_get_resp)
			columns_rename(resp, {
				'bad_call_ratio': 'icr',
				'total_call': 'pitchesCalled',
				'games': 'gamesUmped',
				'data_year': 'season',
				'bad_call_per_game': 'bcpg',
				'bad_call_per_inning': 'bcpi',
				'leastAccurateTeam': 'mostBadCalls',
				'mostAccurateTeam': 'leastBadCalls',
				'ej_{0}'.format(resp['data_year']): 'ejections',
				'crew.number': 'crew',
				'status': 'status'
			})
			resp.update({'firstName': parts[0], 'lastName': parts[-1]})
			if resp['season'] in [2019, '2019']:
				resp['age'] = age
			subarr.append(resp)
	return subarr

#TODO CACHE UMPIRES
def create_umpire_object(name, year):
	name = name.lower()
	career_resp_bcvals = careers_season.get({'name':name, 'data_year':2019},
		AttributesToGet=['bad_call_per_inning', 'bad_call_per_game'])
	career_resp = careers.get(
		{
			'name': name
		},
		AttributesToGet = ['id', 'name']
	)

	# THIS COMMENTED CODE MAY BE A BUG??
	# if 'name' not in career_resp:
	# 	print(name)
	if 'name' not in career_resp:
		return {}
	parts = career_resp['name'].split()
	first_name, last_name = parts[0], parts[-1]
	ump_id = career_resp['id']
	resp_2019 = umpires_2019_table.get({'name':name}, AttributesToGet=['age'])
	crew_update_resp = crew_update_table.get({'name': name, 'season': year}, AttributesToGet = ['years.active', 'ranking'])
	
	range_table = careers_range.get(
		{
			'name': name
		},
		AttributesToGet = ['BCR_{0}'.format(year)]
	)
	career_seasonal_resp = careers_season.get(
		{
			'name': name,
			'data_year': year	
		},
		AttributesToGet = ['total_call', 'games', 'data_year', 'bc_strike', 'bc_ball']
	)
	crew_resp = crew_update_table.get(
		{
			'name': name,
			'season': year
		},
		AttributesToGet = ['crew.number', 'status', 'crew_chief', 'ump.number']
	)
	# want to add crew rank here. Might just make a dict of values for now so I don't have to set up another table
	bcr_best_year_resp = bcr_best_year_table.get({'name':name}, AttributesToGet=['best_year'])
	ejections_resp = ejections_table.get({'name':name},AttributesToGet=['ej_{0}'.format(year)])
	average_game_length_table_resp = average_game_length_table.get({
			'name': name,
		},
		AttributesToGet = ['average_game_length_2019']
	)
	team_preference_resp = profile_team_preference_table.get(
		{'name':name,'season':year},
		AttributesToGet = ['mostAccurateTeam', 'leastAccurateTeam']
	)
	profile_best_worst_month_resp = profile_best_worst_month_table.get({'name': name,'season':year},
		AttributesToGet=['best_month', 'worst_month'])
	bcr_weather_resp = bcr_weather_table.get({'name':name}, AttributesToGet=['best_weather'])
	profile_best_worst_park_resp = profile_best_worst_park_table.get({'name': name, 'season': year},
		AttributesToGet=['best_park', 'worst_worst'])
	bcr_start_time_resp = bcr_start_time_table.get({'name': name}, AttributesToGet=['best_start_time'])
	
	bcr_std_resp = bcr_std_table.get({'name':name}, AttributesToGet=['bcr_std_2019'])
	data = career_seasonal_resp
	data.update(crew_resp)
	data.update(range_table)
	data.update(average_game_length_table_resp)
	data.update(team_preference_resp)
	data.update(ejections_resp)
	data.update(profile_best_worst_month_resp)
	data.update(profile_best_worst_park_resp)
	data.update(career_resp_bcvals)
	data.update(career_resp)
	data.update(bcr_weather_resp)
	data.update(bcr_std_resp)
	data.update(bcr_best_year_resp)
	data.update(bcr_start_time_resp)
	data.update(crew_update_resp)
	data.update({'name': name})
	data.update({
		'firstName': first_name, 
		'last_name': last_name,
		'id': ump_id
	})
	if 'age' in resp_2019:
		data['age'] = resp_2019['age']
	else:
		data['age'] = -1
	columns_rename(data, {
		'BCR_{0}'.format(year): 'icr',
		'crew.number': 'crewNumber',
		'ump.number': 'umpNumber',
		'crew_chief': 'isCrewChief',
		'total_call': 'pitchesCalled',
		'games': 'gamesUmped',
		'best_weather': 'weatherPreference',
		'average_game_length_2019': 'paceOfPlay',
		'best_year': 'bestSeason',
		'best_start_time': 'timePreference',
		'bad_call_per_game': 'bcpg',
		'bad_call_per_inning': 'bcpi',
		'leastAccurateTeam': 'mostBadCalls',
		'mostAccurateTeam': 'leastBadCalls',
		'ej_{0}'.format(year): 'ejections',
		'best_month': 'bestMonth',
		'bcr_std_2019': 'consistency',
		'worst_month': 'worstMonth',
		'worst_worst': 'worstPark',
		'best_park': 'bestPark',
		'years.active': 'yearsExperience',
		'ranking': 'rank'
	})
	return data


def create_career_object(name, data_range):
	name = name.lower()
	career = []
	average_game_length_table_resp = average_game_length_table.get({'name':name}, 
		AttributesToGet=['average_game_length_{0}'.format(year) for year in data_range])
	for year in data_range:
		range_resp = careers_range.get({'name': name}, AttributesToGet=['BCR_{0}'.format(year)])
		if year > 2010:
			change_resp = careers_range_change.get({'name':name}, AttributesToGet=['BCR_change_{0}'.format(year-1) + '_{0}'.format(year)])
		else:
			# change_resp = careers_range_change.get({'name':name}, AttributesToGet=['BCR_change_{0}'.format(year) + '_{0}'.format(year + 1)])
			change_resp = {'BCR_change_2009_2010': -1}
			# need to change this to return -1 but need to check the variable type
		season_resp = careers_season.get({'name': name, 'data_year': year},
			AttributesToGet=['best_pitch', 'worst_pitch', 'data_year', 'games', 'total_call', 'BCR_SL', 'BCR_FT', 'BCR_CU', 'BCR_FF', 'BCR_SI', 
				'BCR_CH', 'BCR_FC', 'BCR_EP', 'BCR_KC', 'BCR_FS', 'BCR_PO', 'BCR_KN', 
				'BCR_SC', 'BCR_FO', 'BCR_UN', 'BCR_FA', 'BCR_IN'])

		career_crucial_calls_resp = career_crucial_calls_table.get({'name':name, 'season':year},
			AttributesToGet=['bad_crucial_call'])
		crew_resp = crew_update_table.get({'name': name, 'season': year},
			AttributesToGet = ['status'])
		# change_resp = careers_range_change.get({'name': name}, AttributesToGet=[
		# 	'BCR_change_{0}-1_{0}'
		# ])
		if season_resp != {}:
			data = range_resp
			if 'average_game_length_{0}'.format(year) in average_game_length_table_resp:
				data.update(
					{
						'paceOfPlay': average_game_length_table_resp['average_game_length_{0}'.format(year)]
					}
				)
			else:
				data.update({'paceOfPlay': -1})

			data.update(career_crucial_calls_resp)
			data.update(season_resp)
			data.update(crew_resp)
			data.update(change_resp)
			data.update({'name': name})
			columns_rename(data, {
				'BCR_SL': 'icrSL',
				'BCR_FT': 'icrFT',
				'BCR_CU': 'icrCU',
				'BCR_FF': 'icrFF',
				'BCR_SI': 'icrSI',
				'BCR_CH': 'icrCH',
				'BCR_FC': 'icrFC',
				'BCR_EP': 'icrEP',
				'BCR_KC': 'icrKC',
				'BCR_FS': 'icrFS',
				'BCR_PO': 'icrPO',
				'BCR_KN': 'icrKN',
				'BCR_SC': 'icrSC',
				'BCR_FO': 'icrFO',
				'BCR_UN': 'icrUN',
				'BCR_FA': 'icrFA',
				'BCR_IN': 'icrIN',
				'BCR_{0}'.format(year): 'icr',
				'total_call': 'pitchesCalled',
				'games': 'gamesUmped'
				})
			data.update({'season': year})
			career.append(data)
	return career

def create_umpire_game_object(name):
	umpire_games = []
	name = name.lower()

	filterExpression = Key('name').eq(name)
	resp = ump_game_lookup.query(
		KeyConditionExpression = filterExpression
	)

	game_ids = [str(item['game']) for item in resp]
	req = [{'game': {'N': game}} for game in game_ids]
	data = games_dataset.batch_get(req)
	keys = ['hometeam','awayteam', 'date', 'bad_call_ratio', 'preference', 'BCR_SL', 
		'BCR_FT', 'BCR_CU', 'BCR_FF', 'BCR_SI', 'BCR_CH', 'BCR_FC', 'BCR_EP', 
		'BCR_KC', 'BCR_FS', 'BCR_KN', 'BCR_FO', 
		'total_call', 'call_ball', 'call_strike'
	]
	for games_resp in data:
		games_resp = {key: games_resp[key] for key in keys}
		if games_resp != {}:
			games_resp.update({'name': name})
			columns_rename(games_resp, {
				'BCR_SL': 'icrSL',
				'BCR_FT': 'icrFT',
				'BCR_CU': 'icrCU',
				'BCR_FF': 'icrFF',
				'BCR_SI': 'icrSI',
				'BCR_CH': 'icrCH',
				'BCR_FC': 'icrFC',
				'BCR_EP': 'icrEP',
				'BCR_KC': 'icrKC',
				'BCR_FS': 'icrFS',
				# 'BCR_PO': 'icrPO',
				'BCR_KN': 'icrKN',
				# 'BCR_SC': 'icrSC',
				'BCR_FO': 'icrFO',
				# 'BCR_UN': 'icrUN',
				# 'BCR_FA': 'icrFA',
				# 'BCR_IN': 'icrIN',
				'preference': 'teamPref',
				'bad_call_ratio': 'icr',
				'awayteam': 'away',
				'hometeam': 'home',
				'total_call': 'totalCalled',
				'call_ball' : 'ballsCalled',
				'call_strike': 'strikesCalled'
			})
			umpire_games.append(games_resp)
	return umpire_games


def create_team_object(name, data_range):
	data = []
	name = name.lower()
	# TEAM_NAMES
	keys = [{'name': {'S': name}, 'data_year': {'N': str(year)}} for year in data_range]
	response = team_stats_dataset.batch_get(keys)
	# careers_season_resp = careers_season.batch_get(keys)
	print('TEAM NAMES: {0}'.format(TEAM_NAMES))
	for resp in response:
		if resp != {}:
			year = int(resp['data_year'])
			keys = list(resp.keys())

			# Spaghetti line of code
			prev = team_stats_dataset.get({'name':name, 'data_year': year-1})
			for team in TEAM_NAMES:
				team_stats = {
					'team': team,
					'season': year,
					'pitchesCalled': resp['total_call_{0}'.format(team)],
					'ballsCalled': resp['call_ball_{0}'.format(team)],
					'strikesCalled': resp['call_strike_{0}'.format(team)],
					'bcr': resp['BCR_{0}'.format(team)],
					'seasonChangeBcr': prev['BCR_{0}'.format(team)] if prev != {} else -1,
					'bcrFO': resp['BCR_{0}'.format(team) + '_FO'],
					'bcrFF': resp['BCR_{0}'.format(team) + '_FF'],
					'bcrFT': resp['BCR_{0}'.format(team) + '_FT'],
					'bcrFC': resp['BCR_{0}'.format(team) + '_FC'],
					'bcrSI': resp['BCR_{0}'.format(team) + '_SI'],
					'bcrCH': resp['BCR_{0}'.format(team) + '_CH'],
					'bcrSL': resp['BCR_{0}'.format(team) + '_SL'],
					'bcrCU': resp['BCR_{0}'.format(team) + '_CU'],
					'bcrEP': resp['BCR_{0}'.format(team) + '_EP'],
					'bcrKC': resp['BCR_{0}'.format(team) + '_KC'],
					'bcrFS': resp['BCR_{0}'.format(team) + '_FS'],
					'bcrKN': resp['BCR_{0}'.format(team) + '_KN']
				}
				columns_rename(team_stats, {
					'bcr': 'icr',
					'seasonChangeBcr': 'seasonChangeIcr'
				})
				data.append(team_stats)
	return data
