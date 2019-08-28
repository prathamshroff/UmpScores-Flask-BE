def columns_rename(d, columns_map):
	for key in columns_map:
		d[columns_map[key]] = d.pop(key)

def create_rankings_object(career_seasonal_table, team_stats_table, umpire_names, year_range):
	umpires = {}
	for year in year_range:
		umpires[year] = []
		for name in umpire_names:
			career_resp = career_seasonal_table.get(
				{
					'name': name,
					'data_year': year
				},
				AttributesToGet = ['season', 'name', 'games', 'total_call', 'bad_call_ratio']
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
					'bad_call_ratio': 'bcr',
					'total_call': 'pitchesCalled',
					'games': 'gamesUmped'
				})
				# career_resp['number'] = team_resp['number']
				umpires[year].append(career_resp)
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
				'BCR_': 'bcr',
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
	career = {}
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
				'BCR_SL': 'bcrSL',
				'BCR_FT': 'bcrFT',
				'BCR_CU': 'bcrCU',
				'BCR_FF': 'bcrFF',
				'BCR_SI': 'bcrSI',
				'BCR_CH': 'bcrCH',
				'BCR_FC': 'bcrFC',
				'BCR_EP': 'bcrEP',
				'BCR_KC': 'bcrKC',
				'BCR_FS': 'bcrFS',
				'BCR_PO': 'bcrPO',
				'BCR_KN': 'bcrKN',
				'BCR_SC': 'bcrSC',
				'BCR_FO': 'bcrFO',
				'BCR_UN': 'bcrUN',
				'BCR_FA': 'bcrFA',
				'BCR_IN': 'bcrIN',
				'BCR_{0}'.format(year): 'bcr',
				'total_call': 'pitchesCalled',
				'games': 'gamesUmped'
				})
			career[year] = data
	return career

def create_umpire_game_object(game, games_table, data_range):
	umpire_game = {}
	for year in data_range:
		resp = games_table.get({'game': game}, 
			AttributesToGet = ['hometeam','awayteam', 'date', 'bad_call_ratio', 'preference', 'BCR_SL', 
				'BCR_FT', 'BCR_CU', 'BCR_FF', 'BCR_SI', 'BCR_CH', 'BCR_FC', 'BCR_EP', 
				'BCR_KC', 'BCR_FS', 'BCR_KN', 'BCR_FO', 
				'total_call', 'call_strike'
			]
		)
		columns_rename(resp, {
			'BCR_SL': 'bcrSL',
			'BCR_FT': 'bcrFT',
			'BCR_CU': 'bcrCU',
			'BCR_FF': 'bcrFF',
			'BCR_SI': 'bcrSI',
			'BCR_CH': 'bcrCH',
			'BCR_FC': 'bcrFC',
			'BCR_EP': 'bcrEP',
			'BCR_KC': 'bcrKC',
			'BCR_FS': 'bcrFS',
			# 'BCR_PO': 'bcrPO',
			'BCR_KN': 'bcrKN',
			# 'BCR_SC': 'bcrSC',
			'BCR_FO': 'bcrFO',
			# 'BCR_UN': 'bcrUN',
			# 'BCR_FA': 'bcrFA',
			# 'BCR_IN': 'bcrIN',
			'preference': 'teamPref',
			'bad_call_ratio': 'bcr',
			'awayteam': 'away',
			'hometeam': 'home',
			'total_call': 'ballsCalled',
			'call_strike': 'strikesCalled'
		})
		umpire_game[year] = resp
	return umpire_game




