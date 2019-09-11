# TODO /umpire is array (how do I handle season then??)
# TODO uncomment /get-games
from boto3.dynamodb.conditions import Key, Attr
from StorageSolutions.tables import *
# importing libraries for /games endpoint
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dateutil import parser
from bs4 import BeautifulSoup
import re

def create_chart_object(name, year_range):
	name = ' '.join([word.capitalize() for word in name.lower().split()])
	filterExpression = Key('name').eq(name)
	data = umpire_zones.query(KeyConditionExpression = filterExpression)['Items']

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
	    'heatMapIN': []
	}
	for entry in data:
		year = entry['season']
		careers_season_resp = careers_season.get({'name': name, 'data_year': year},
			AttributesToGet = ['BCR_z{0}'.format(i) for i in range(1, 15)])

		resp['heatMap'].append({
			'data': [careers_season_resp['BCR_z{0}'.format(i)] for i in range(1,15) if i != 10],
			'season': year
		})
		resp['heatMapSL'].append({
			'data': [entry[elem] for elem in ['bad_call_SL_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapFT'].append({
			'data': [entry[elem] for elem in ['bad_call_FT_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapCU'].append({
			'data': [entry[elem] for elem in ['bad_call_CU_{0}'.format(i) for i in \
				range(1, 15)if i != 10]], 
			'season': year
		})

		resp['heatMapFF'].append({
			'data': [entry[elem] for elem in ['bad_call_FF_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapSI'].append({
			'data': [entry[elem] for elem in ['bad_call_SI_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapCH'].append({
			'data': [entry[elem] for elem in ['bad_call_CH_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapFC'].append({
			'data': [entry[elem] for elem in ['bad_call_FC_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapEP'].append({
			'data': [entry[elem] for elem in ['bad_call_EP_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapKC'].append({
			'data': [entry[elem] for elem in ['bad_call_KC_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapFS'].append({
			'data': [entry[elem] for elem in ['bad_call_FS_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapPO'].append({
			'data': [entry[elem] for elem in ['bad_call_PO_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapKN'].append({
			'data': [entry[elem] for elem in ['bad_call_KN_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapSC'].append({
			'data': [entry[elem] for elem in ['bad_call_SC_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapFO'].append({
			'data': [entry[elem] for elem in ['bad_call_FO_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapUN'].append({
			'data': [entry[elem] for elem in ['bad_call_UN_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapFA'].append({
			'data': [entry[elem] for elem in ['bad_call_FA_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

		resp['heatMapIN'].append({
			'data': [entry[elem] for elem in ['bad_call_IN_{0}'.format(i) for i in \
				range(1, 15) if i != 10]], 
			'season': year
		})

	return resp

def get_event_lines(event_id):
	resp = {}
	# "http://xml.donbest.com/v2/odds/5/event_id" + event_id +"/?token=K_E_Oc-S6!F!Kypt"
	xmlData = requests.get("http://xml.donbest.com/v2/odds/5/" + event_id +"/?token=K_E_Oc-S6!F!Kypt")
	root = ET.fromstring(xmlData.text)
	for line in root.iter("line"):
		if (line.get("period") == "FG" and line.get("type") == "current"):
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
	# print(tables[2])
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

def get_game_values(ump_table, event):
	resp = {}
	event_lines = get_event_lines(event.get("id"))
	if (event_lines["status"] == 200):
		resp["awayLine"] = event_lines["awayLine"]
		resp["homeLine"] = event_lines["homeLine"]
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
	found = 0
	for key in ump_table.keys():
		if (resp["awayTeam"] in key and resp["homeTeam"] in key):
			# grab the umpire value in ump_table
			resp["umpireName"] = ump_table[key][1]
			found = 1

	swaps = {"CWS":"CHW", "CHW":"CWS", "TB":"TAM", "TAM":"TB", "FLA":"MIA", "MIA":"FLA"}
	# means there might be mismatched team abbreviations
	if (found == 0):
		if (resp["awayTeam"] in swaps):
			for key in ump_table.keys():
				if (swaps[resp["awayTeam"]] in key and resp["homeTeam"] in key):
					# grab the umpire value in ump_table
					resp["umpireName"] = ump_table[key][1]
		if(resp["homeTeam"] in swaps):
			# MODIFY HOME TEAM
			for key in ump_table.keys():
				if (resp["awayTeam"] in key and swaps[resp["homeTeam"]] in key):
					# grab the umpire value in ump_table
					resp["umpireName"] = ump_table[key][1]
	return resp
'''
<event id="970233" season="REGULAR" date="2019-09-09T23:05:00+0" name="Atlanta Braves vs Philadelphia Phillies">
	<event_type>team_event</event_type>
	<event_state>PENDING</event_state>
	<event_state_id>0</event_state_id>
	<time_changed>false</time_changed>
	<neutral>false</neutral>
	<game_number>1</game_number>
	<location name="Citizens B Park" id="8" link="/v2/location/8"/>
	<participant rot="951" side="AWAY">
		<team id="1288" name="Atlanta Braves" link="/v2/team/1288"/>
		<pitcherChanged>false</pitcherChanged>
		<pitcher hand="RIGHT" id="341549" full_name="Mike Foltynewicz">M FOLTYNEWICZ</pitcher>
	</participant>
	<participant rot="952" side="HOME">
		<team id="1290" name="Philadelphia Phillies" link="/v2/team/1290"/>
		<pitcherChanged>false</pitcherChanged>
		<pitcher hand="RIGHT" id="343344" full_name="Aaron Nola">A NOLA</pitcher>
	</participant>
	<live>true</live>
	<lines>
		<current link="/v2/odds/5/970233"/>
		<opening link="/v2/open/5/970233"/>
	</lines>
	<score link="/v2/score/970233"/>
	<pitcher_changed>false</pitcher_changed>
</event>
		'''

def get_all_games():
	resp = {}
	games = []
	# storing this to pass to get_game_values so I can get the right data back
	ump_table = get_umpires_for_games()
	xmlData = requests.get("http://xml.donbest.com/v2/schedule/?token=K_E_Oc-S6!F!Kypt")
	root = ET.fromstring(xmlData.text)
	count = 0
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
						# pass event object for further parsing
						event_info = get_game_values(ump_table, event)
						print(event_info)
						games.append(event_info)
	print(count)
	resp["games"] = games
	return resp

def create_pitcher_object(name, year_range):
	name = ' '.join([word.capitalize() for word in name.lower().split()])
	filterExpression = Key('name').eq(name)

	data = pitcher_stats.query(
		KeyConditionExpression = filterExpression
	)['Items']
	resp = []
	data = {int(pitcher['season']): pitcher for pitcher in data}
	for season in data:
		pitcher = data[season]
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
		if season != 2010 and season - 1 in data:
			obj['seasonChangeIcr'] = data[season - 1]['BCR']
		resp.append(obj)
	return resp

def columns_rename(d, columns_map):
	for key in columns_map:
		d[columns_map[key]] = d.pop(key)

def create_rankings_object(umpire_names, year_range):
	umpires = []
	for year in year_range:
		for name in umpire_names:
			career_resp = careers_season.get(
				{
					'name': name,
					'data_year': year
				},
				AttributesToGet = ['name', 'games', 'total_call', 'bad_call_ratio']
			)

			# team_resp = team_stats_dataset.get(
			# 	{
			# 		'name': name,
			# 		'data_year': year
			# 	},
			# 	AttributesToGet = ['number']
			# )

			if career_resp != {}:
				columns_rename(career_resp, {
					'bad_call_ratio': 'icr',
					'total_call': 'pitchesCalled',
					'games': 'gamesUmped'
				})
				career_resp.update({'season': year})
				# career_resp['number'] = team_resp['number']
				umpires.append(career_resp)
	return umpires

def create_umpire_object(name, year_range):
	name = ' '.join([word.capitalize() for word in name.lower().split()])

	career_resp = careers.get(
		{
			'name': name
		},
		AttributesToGet = ['id', 'name']
	)
	parts = career_resp['name'].split()
	first_name, last_name = parts[0], parts[-1]
	ump_id = career_resp['id']

	year = year_range[-1]
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
		AttributesToGet = ['total_call', 'games', 'data_year']
	)
	crew_resp = crews.get(
		{
			'name': name,
			'data_year': year
		},
		AttributesToGet = ['crew', 'status', 'crew_chief']
	)
	if career_seasonal_resp != {} and crew_resp != {} and range_table != {}:
		data = career_seasonal_resp
		data.update(crew_resp)
		data.update(range_table)
		data.update({
			'firstName': first_name, 
			'last_name': last_name,
			'id': ump_id
		})
		columns_rename(data, {
			'BCR_{0}'.format(year): 'icr',
			'crew': 'crewNumber',
			'crew_chief': 'isCrewChief',
			'total_call': 'pitchesCalled',
			'games': 'gamesUmped'
		})
	return data


def create_career_object(name, data_range):
	name = ' '.join([word.capitalize() for word in name.lower().split()])
	career = []
	for year in data_range:
		range_resp = careers_range.get({'name': name}, AttributesToGet=['BCR_{0}'.format(year)])
		season_resp = careers_season.get({'name': name, 'data_year': year},
			AttributesToGet=['games', 'total_call', 'BCR_SL', 'BCR_FT', 'BCR_CU', 'BCR_FF', 'BCR_SI', 
				'BCR_CH', 'BCR_FC', 'BCR_EP', 'BCR_KC', 'BCR_FS', 'BCR_PO', 'BCR_KN', 
				'BCR_SC', 'BCR_FO', 'BCR_UN', 'BCR_FA', 'BCR_IN'])
		crew_resp = crews.get({'name': name, 'data_year': year},
			AttributesToGet = ['status'])
		# change_resp = careers_range_change.get({'name': name}, AttributesToGet=[
		# 	'BCR_change_{0}-1_{0}'
		# ])
		if range_resp != {} and season_resp != {}:
			data = range_resp
			data.update(season_resp)
			data.update(crew_resp)
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
	name = ' '.join([word.capitalize() for word in name.lower().split()])

	filterExpression = Key('name').eq(name)
	resp = ump_game_lookup.query(
		KeyConditionExpression = filterExpression
	)['Items']

	game_ids = [int(item['game']) for item in resp]
	for game in game_ids:
		games_resp = games_dataset.get({'game': game}, 
			AttributesToGet = ['hometeam','awayteam', 'date', 'bad_call_ratio', 'preference', 'BCR_SL', 
				'BCR_FT', 'BCR_CU', 'BCR_FF', 'BCR_SI', 'BCR_CH', 'BCR_FC', 'BCR_EP', 
				'BCR_KC', 'BCR_FS', 'BCR_KN', 'BCR_FO', 
				'total_call', 'call_strike'
			]
		)
		if games_resp != {}:
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
				'total_call': 'ballsCalled',
				'call_strike': 'strikesCalled'
			})
			umpire_games.append(games_resp)
	return umpire_games


def create_team_object(name, team, data_range):
	data = []
	name = ' '.join([word.capitalize() for word in name.lower().split()])
	for year in data_range:
		resp = team_stats_dataset.get({'name': name, 'data_year': year})
		if resp != {}:
			keys = list(resp.keys())

			# Spaghetti line of code
			prev = team_stats_dataset.get({'name':name, 'data_year': year-1})

			team_stats = {
				'team': team,
				'season': year,
				'pitchesCalled': resp['total_call_{0}'.format(team)],
				'ballsCalled': resp['call_ball_{0}'.format(team)],
				'strikesCalled': resp['call_strike_{0}'.format(team)],
				'bcr': resp['BCR_{0}'.format(team)],
				'seasonChangeBcr': prev['BCR_{0}'.format(team)] if prev != {} else -1
			}
			columns_rename(team_stats, {
				'bcr': 'icr',
				'seasonChangeBcr': 'seasonChangeIcr'
			})
			data.append(team_stats)
	return data

