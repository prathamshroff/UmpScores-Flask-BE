# TODO /umpire is array (how do I handle season then??)
# TODO uncomment /get-games
from boto3.dynamodb.conditions import Key, Attr
from StorageSolutions.tables import *
TEAM_NAMES = [name.replace('total_call_', '') for name in \
    team_stats_dataset.get(query_map = {'name':'Jordan Baker', 'data_year' : 2019}).keys() if \
    name.startswith('total_call_')]
TEAM_NAMES = [name for name in TEAM_NAMES if '_' not in name]

def create_chart_object(name, year_range):
	name = ' '.join([word.capitalize() for word in name.lower().split()])
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

def get_pitcher_names(name):
	name = ' '.join([word.capitalize() for word in name.lower().split()])
	names = set()
	resp = umpire_pitchers.query(KeyConditionExpression = Key('name').eq(name))
	for page in resp:
		pitchers = page.keys()
		subnames = [name.replace('total_call_', '').replace('.', ' ') for name in pitchers if \
			name.startswith('total_call_')]
		names = names.union(subnames)
	return list(names)

def create_pitcher_object(umpire_name, pitcher_name):
	pitcher_name = ' '.join([word.capitalize() for word in pitcher_name.lower().split()])
	umpire_name = ' '.join([word.capitalize() for word in umpire_name.lower().split()])

	print(pitcher_name, umpire_name)
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
		d[columns_map[key]] = d.pop(key)

def create_rankings_object(umpire_names, year_range):
	umpires = []
	for name in umpire_names:
		subarr = []
		parts = name.split()
		for year in year_range:
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
				career_resp.update({'firstName': parts[0], 'lastName': parts[-1]})
				# career_resp['number'] = team_resp['number']
				subarr.append(career_resp)
		umpires.append(subarr)
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
	)

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


def create_team_object(name, data_range):
	data = []
	name = ' '.join([word.capitalize() for word in name.lower().split()])
	# TEAM_NAMES
	print(TEAM_NAMES)
	for year in data_range:
		resp = team_stats_dataset.get({'name': name, 'data_year': year})
		if resp != {}:
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
					'seasonChangeBcr': prev['BCR_{0}'.format(team)] if prev != {} else -1
				}
				columns_rename(team_stats, {
					'bcr': 'icr',
					'seasonChangeBcr': 'seasonChangeIcr'
				})
				data.append(team_stats)
	return data

