def create_rankings_object(career_table, team_stats_table, umpire_names, year_range):
	umpires = {}
	for year in year_range:
		umpires[year] = []
		for name in umpire_names:
			career_resp = career_table.get(
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
				career_resp['bcr'] = career_resp.pop('bad_call_ratio')
				career_resp['pitchesCalled'] = career_resp.pop('total_call')
				career_resp['gamesUmped'] = career_resp.pop('games')
				# career_resp['number'] = team_resp['number']
				umpires[year].append(career_resp)
	return umpires
