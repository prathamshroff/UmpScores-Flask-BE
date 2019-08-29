# TODO /umpire is array (how do I handle season then??)
# TODO uncomment /get-games
from boto3.dynamodb.conditions import Key, Attr
def columns_rename(d, columns_map):
	for key in columns_map:
		d[columns_map[key]] = d.pop(key)

def create_rankings_object(career_seasonal_table, team_stats_table, umpire_names, year_range):
	umpires = []
	for year in year_range:
		for name in umpire_names:
			career_resp = career_seasonal_table.get(
				{
					'name': name,
					'data_year': year
				},
				AttributesToGet = ['name', 'games', 'total_call', 'bad_call_ratio']
			)

			# team_resp = team_stats_table.get(
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

def create_umpire_object(name, career_table, career_seasonal_table, crews_table, career_range_table,
	year_range):
	umpire = {}
	first, last = name.lower().split()
	name = ' '.join((first.capitalize(), last.capitalize()))
	career_resp = career_table.get(
		{
			'name': name
		},
		AttributesToGet = ['id', 'name']
	)
	first_name, last_name = career_resp['name'].split()
	ump_id = career_resp['id']

	range_table = career_range_table.get({
		'name': name
	})
	for year in year_range:
		career_seasonal_resp = career_seasonal_table.get(
			{
				'name': name,
				'data_year': year	
			},
			AttributesToGet = ['total_call', 'games', 'BCR_', 'data_year']
		)
		crew_resp = crews_table.get(
			{
				'name': name,
				'data_year': year
			},
			AttributesToGet = ['crew', 'status', 'crew_chief']
		)
		if career_seasonal_resp != {} and crew_resp != {} and range_table != {}:
			if (career_seasonal_resp['BCR_'] == -1):
				career_seasonal_resp.pop('data_year')

			data = career_seasonal_resp
			data.update(crew_resp)
			data.update({
				'firstName': first_name, 
				'last_name': last_name,
				'id': ump_id
			})
			columns_rename(data, {
				'BCR_': 'icr',
				'crew': 'crewNumber',
				'crew_chief': 'isCrewChief',
				'total_call': 'pitchesCalled',
				'games': 'gamesUmped'
			})
			umpire[year] = data
	return umpire


def create_career_object(name, career_seasonal_table, crews_table, career_range_table, career_change_range_table, 
	data_range):
	first, last = name.lower().split()
	name = ' '.join((first.capitalize(), last.capitalize()))
	career = []
	for year in data_range:
		range_resp = career_range_table.get({'name': name}, AttributesToGet=['BCR_{0}'.format(year)])
		season_resp = career_seasonal_table.get({'name': name, 'data_year': year},
			AttributesToGet=['games', 'total_call', 'BCR_SL', 'BCR_FT', 'BCR_CU', 'BCR_FF', 'BCR_SI', 
				'BCR_CH', 'BCR_FC', 'BCR_EP', 'BCR_KC', 'BCR_FS', 'BCR_PO', 'BCR_KN', 
				'BCR_SC', 'BCR_FO', 'BCR_UN', 'BCR_FA', 'BCR_IN'])
		crew_resp = crews_table.get({'name': name, 'data_year': year},
			AttributesToGet = ['status'])
		# change_resp = career_change_range_table.get({'name': name}, AttributesToGet=[
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

def create_umpire_game_object(name, games_table, ump_game_lookup):
	umpire_games = []
	first, last = name.lower().split()
	name = ' '.join((first.capitalize(), last.capitalize()))
	filterExpression = Key('name').eq(name)
	resp = ump_game_lookup.query(
		KeyConditionExpression = filterExpression
	)['Items']

	game_ids = [int(item['game']) for item in resp]
	for game in game_ids:
		resp = games_table.get({'game': game}, 
			AttributesToGet = ['hometeam','awayteam', 'date', 'bad_call_ratio', 'preference', 'BCR_SL', 
				'BCR_FT', 'BCR_CU', 'BCR_FF', 'BCR_SI', 'BCR_CH', 'BCR_FC', 'BCR_EP', 
				'BCR_KC', 'BCR_FS', 'BCR_KN', 'BCR_FO', 
				'total_call', 'call_strike'
			]
		)
		if resp != {}:
			columns_rename(resp, {
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
			umpire_games.append(resp)
		else:
			print(game)
	return umpire_games


def create_team_object(name, team, team_stats_table, data_range):
	data = []
	first, last = name.lower().split()
	name = ' '.join((first.capitalize(), last.capitalize()))
	for year in data_range:
		resp = team_stats_table.get({'name': name, 'data_year': year})
		keys = list(resp.keys())

		# Spaghetti line of code
		prev = team_stats_table.get({'name':name, 'data_year': year-1})

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

