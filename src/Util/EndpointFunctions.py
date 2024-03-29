# TODO /umpire is array (how do I handle season then??)
# TODO uncomment /get-games
from boto3.dynamodb.conditions import Key, Attr
from StorageSolutions.tables import *
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
import statsapi
from datetime import datetime
# importing things from cache

TEAM_NAMES = [name.replace('total_call_', '') for name in \
    team_stats_dataset.get(query_map = {'name':'jordan baker', 'data_year' : 2019}).keys() if \
    name.startswith('total_call_')]
TEAM_NAMES = [name.split('_')[0] for name in TEAM_NAMES if name.endswith('_')]

def create_chart_object(name, year_range):
	"""Creates chart object for /charts endpoint"""
	name = name.lower()
	filterExpression = Key('name').eq(name)
	data = umpire_zones.query(KeyConditionExpression = filterExpression)
	
	resp = {
		'name': name,
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
	bcr_vals = [{'name': {'S': name}, 'data_year': {'N': str(year)}} for year in year_range]
	bcr_vals = careers_season.batch_get(bcr_vals)
	resp['bcrOverCareer'] = [{'x': d['data_year'], 'y': d['bad_call_ratio']} for d in bcr_vals]

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

		rang = list(range(11, 15)) + list(range(1, 10))
		try:
			resp['heatMap'].append({
				'data': [careers_season_resp['BCR_z{0}'.format(i)] for i in rang],
				'season': year
			})
		except Exception as e:
			print("EXCEPTION: ", e)

		tokens = ['SL', 'FT', 'CU', 'FF', 'SI', 'CH', 'FC', 'EP', 'KC', 'FS', 'PO', 'KN', 'SC',
			'FO', 'UN', 'FA', 'IN']
		for token in tokens:
			resp['heatMap{0}'.format(token)].append({
				'data': [0 if Decimal(entry[elem]).compare(-1) == Decimal('0') else entry[elem] for elem in ['BCR_{0}_{1}'.format(token, i) for i in \
					rang]],
				'season': year
			})
	return resp


def get_event_lines(event_id):
	resp = {}
	# old token: !m5__dQ_ZN-aH-v4
	# new token: !FSUT!-5S_7ogtMs
	xmlData = requests.get("http://xml.donbest.com/v2/odds/5/" + event_id +"/?token=Q6T7J0_!QcUs!_Bx",
		headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'})
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
	page = requests.get("https://www.statfox.com/mlb/umpiremain.asp",
		headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'})
	soup = BeautifulSoup(page.text, "lxml")
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
			# print("LINES: ", resp)
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
			#print("GETTING TO  THE KEY IN UMP_TABLE.keys() point")
			if (resp["awayTeam"] in key and resp["homeTeam"] in key):
				# grab the umpire value in ump_table
				resp["umpireName"] = format_umpire_name(ALL_UMPIRE_NAMES, ump_table[key][1])
				# print("UMP NAME: ", resp["umpireName"])
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
	"""Creates all game objects for /games endpoint"""
	"""
	games = []
	# storing this to pass to get_game_values so I can get the right data back
	ump_table = get_umpires_for_games()
	xmlData = requests.get("http://xml.donbest.com/v2/schedule/?token=Q6T7J0_!QcUs!_Bx",
		headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'})
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
						corrected = testDate - timedelta(hours=7, minutes=0)
						dateReal = corrected.strftime("%Y-%m-%d")
						dateRealObject = datetime.strptime(dateReal, "%Y-%m-%d").date()
							# subtract four from date object to correct for UTC time
						dateObject = event.get("date").split("T")
						date = datetime.strptime(dateObject[0],"%Y-%m-%d").date()
						today = date.today()
						# this is going to be the actual current time - 4 to get current, - another 6 for good measure
						current_time = datetime.now()
						adjusted_time = current_time - timedelta(hours=10)
						new_today_string = adjusted_time.strftime('%Y-%m-%d')
						new_today_date = datetime.strptime(new_today_string, '%Y-%m-%d').date()
						# compare date to today
						if (new_today_date == dateRealObject):
							count += 1
							#print("EVENT: ", event.attrib)
							# 2018-11-15T12:54:55.604Z
							# pass event object for further parsing
							event_info = get_game_values(ALL_UMPIRE_NAMES, ump_table, event)
							if (verifyGameData(event_info)):
								games.append(event_info)
	except Exception as e:
		print("EXCEPTION: ", e)
	"""
	today = datetime.today().strftime('%m/%d/%Y') #Gather information for current day

	events = statsapi.schedule(start_date=today)
	games = []

	for x in events:
		resp = {}
		resp["date"] = x['game_datetime'][:-1]

		currgame = x['game_id']
		box = statsapi.boxscore_data(currgame) #Box score provides all information we will need

		awayteamabv = box['teamInfo']['away']['abbreviation'] #Path to data was found through JSON inspection after API call
		resp["awayTeam"] = awayteamabv

		hometeamabv = box['teamInfo']['home']['abbreviation']
		resp["homeTeam"] = hometeamabv

		for key in box['gameBoxInfo']:
			if key['label'] == 'Venue': #Checks for different formatting to ensure proper variable separation
				stadium = key['value']
				resp["location"] = stadium[:-1]
			if key['label'] == 'Umpires':
				umpires = key['value']
				if umpires == '':
					resp["umpireName"] = "NA" #Error-checking
				else:
					homeplateump = umpires.split('. ', 1)
					homeplatename = homeplateump[0].split(': ')
					commacheck = ","  #Format string to input into frontend
					if commacheck in homeplatename[0]:
						resp["umpireName"] = "NA"
					else:
						final = homeplatename[1]
						resp["umpireName"] = final.lower()
		games.append(resp)
	q.put(games)


def get_pitcher_names(name):
	"""Creates all pitcher name objects for /get-pitchers endpoint"""
	name = name.lower()
	names = set()
	resp = umpire_pitchers.query(KeyConditionExpression = Key('name').eq(name))
	for page in resp:
		pitchers = page.keys()
		subnames = [name.replace('total_call_', '').replace('.', ' ') for name in pitchers if \
			name.startswith('total_call_')]
		names = names.union(subnames)
	return {'name':name, 'data':list(names)}


def create_pitcher_object(umpire_name, pitcher_name):
	"""Creates pitcher object for /pitchers endpoint"""
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
	"""
	Renames keys in a dict if key exists else creates key and sets value to -1

	Parameters
	----------
	d : dict
		dictionary to have column names renamed
	columns_map : Dict[str, str]
		keys represent keys in d that we will rename and values represent the new names
			e.g. columns_map={'ump': 'name'}
	"""
	for key in columns_map:
		if key in d:
			d[columns_map[key]] = d.pop(key)
		else:
			d[columns_map[key]] = -1


def create_rankings_object(name, year_range):
	"""Creates all rankings objects for /rankings endpoint"""
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


def create_umpire_object(name, year):
	"""Creates umpire object for /umpires endpoint"""
	name = name.lower()
	career_resp_bcvals = careers_season.get({'name':name, 'data_year':2019},
		AttributesToGet=['bad_call_per_inning', 'bad_call_per_game'])
	career_resp = careers.get(
		{
			'name': name
		},
		AttributesToGet = ['id', 'name']
	)

	# THIS COMMENTED CODE MAY BE A BUG?? Alex Tosi will be empty because of this line is 
	# career.csv out of date?
	# if 'name' not in career_resp:
	# 	print(name)
	if 'name' not in career_resp:
		return {}
	parts = career_resp['name'].split()
	first_name, last_name = parts[0], parts[-1]
	ump_id = career_resp['id']
	resp_2019 = umpires_2019_table.get({'name':name}, AttributesToGet=['age'])
	crew_update_resp = crew_update_table.get({'name': name, 'season': year}, AttributesToGet = ['years.active', 'ranking'])
	
	career_seasonal_resp = careers_season.get(
		{
			'name': name,
			'data_year': year	
		},
		AttributesToGet = ['total_call', 'games', 'data_year', 'bc_strike', 'bc_ball', 'bad_call_ratio']
	)
	crew_resp = crew_update_table.get(
		{
			'name': name,
			'season': year
		},
		AttributesToGet = ['crew.number', 'status', 'crew.chief', 'ump.number']
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
		# 'BCR_{0}'.format(year): 'icr',
		'crew.number': 'crewNumber',
		'ump.number': 'umpNumber',
		'crew.chief': 'isCrewChief',
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
		'ranking': 'rank',
		'bad_call_ratio': 'icr'
	})
	return data


def create_career_object(name, data_range):
	"""Creates career object for /career endpoint"""
	name = name.lower()
	career = []
	average_game_length_table_resp = average_game_length_table.get({'name':name}, 
		AttributesToGet=['average_game_length_{0}'.format(year) for year in data_range])
	for year in data_range:
		# range_resp = careers_range.get({'name': name}, AttributesToGet=['BCR_{0}'.format(year)])
		if year > 2010:
			change_resp = careers_range_change.get({'name':name}, AttributesToGet=['BCR_change_{0}'.format(year-1) + '_{0}'.format(year)])
		else:
			# change_resp = careers_range_change.get({'name':name}, AttributesToGet=['BCR_change_{0}'.format(year) + '_{0}'.format(year + 1)])
			change_resp = {'BCR_change_2009_2010': -1}
			# need to change this to return -1 but need to check the variable type
		season_resp = careers_season.get({'name': name, 'data_year': year},
			AttributesToGet=['best_pitch', 'worst_pitch', 'data_year', 'games', 'total_call', 'BCR_SL', 'BCR_FT', 'BCR_CU', 'BCR_FF', 'BCR_SI', 
				'BCR_CH', 'BCR_FC', 'BCR_EP', 'BCR_KC', 'BCR_FS', 'BCR_PO', 'BCR_KN', 
				'BCR_SC', 'BCR_FO', 'BCR_UN', 'BCR_FA', 'BCR_IN', 'bad_call_ratio'])

		career_crucial_calls_resp = career_crucial_calls_table.get({'name':name, 'season':year},
			AttributesToGet=['bad_crucial_call'])
		crew_resp = crew_update_table.get({'name': name, 'season': year},
			AttributesToGet = ['status'])
		# change_resp = careers_range_change.get({'name': name}, AttributesToGet=[
		# 	'BCR_change_{0}-1_{0}'
		# ])
		if season_resp != {}:
			data = {}
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
				'bad_call_ratio': 'icr',
				'total_call': 'pitchesCalled',
				'games': 'gamesUmped'
				})
			data.update({'season': year})
			career.append(data)
	return career


def create_umpire_game_object(name):
	"""Creates umpire_game object for /umpireGames endpoint"""
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
	"""Creates team object for /teams endpoint"""
	data = []
	name = name.lower()
	# TEAM_NAMES
	keys = [{'name': {'S': name}, 'data_year': {'N': str(year)}} for year in data_range]
	response = team_stats_dataset.batch_get(keys)
	# careers_season_resp = careers_season.batch_get(keys)
	for resp in response:
		if resp != {}:
			year = int(resp['data_year'])
			keys = list(resp.keys())

			# MIAMI Changed Team Name in 2012 therefore MIA pre 2012 is FLA
			prev = team_stats_dataset.get({'name':name, 'data_year': year-1})
			for team in TEAM_NAMES:
				if ('total_call_FLA' in resp and team == 'MIA'):
					team = 'FLA'

				if 'BCR_{0}'.format(team) in prev:
					season_change_bcr = prev['BCR_{0}'.format(team)] if prev != {} else -1
				else:
					season_change_bcr = prev['BCR_FLA'] if prev != {} else -1

				team_stats = {
					'name': name,
					'team': team if team != 'FLA' else 'MIA',
					'season': year,
					'pitchesCalled': resp['total_call_{0}'.format(team)],
					'ballsCalled': resp['call_ball_{0}'.format(team)],
					'strikesCalled': resp['call_strike_{0}'.format(team)],
					'bcr': resp['BCR_{0}'.format(team)],
					'seasonChangeBcr': season_change_bcr,
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

def create_awards_object():
	all_awards = awards_table.scan()
	data = {}
	data["Award Categories"] = [] #Intialize list to store the different types of awards
	for award in all_awards : 
		award_type = award["Award"]
		if award_type in data:
			continue
		else:
			data[award_type] = {}
			if award_type != "Best Crew Chief" and award_type != "Best Crew":
				data[award_type]["FT"] = {}
				data[award_type]["CU"] = {}
				data["Award Categories"].append([award_type, "FT"])
				data["Award Categories"].append([award_type, "CU"])
			else:
				data[award_type]["null"] = {} #Best Crew Chief and Best Crew don't have statuses
				data["Award Categories"].append([award_type, "null"])
	for award in all_awards:
		award_type = award["Award"]
		status = award["Status"]
		ranking = award["Ranking"]
		name = award["Name"]
		name_lower = name.lower()
		if "Crew" not in name:
			bcr = float(careers_range.get({'name':name_lower}, AttributesToGet=['BCR_2019'])['BCR_2019'])
			career_bcr = float(careers.get({'name':name_lower}, AttributesToGet=['career_bcr'])['career_bcr'])
			total_games = int(careers.get({'name':name_lower}, AttributesToGet=['total_games'])['total_games'])
		data[award_type][status][int(ranking)] = {"Name" : name, "BCR": bcr, "Career BCR": career_bcr, "HP Appearances": total_games}
	return data