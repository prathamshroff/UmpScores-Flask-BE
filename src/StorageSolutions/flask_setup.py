from flask_cors import CORS
from flask import Flask, request
from flask_restplus import Resource, Api, reqparse, fields
from StorageSolutions.models import *
app = Flask(__name__)
api = Api(app, default ="Umpires and Games")
CORS(app)
app.config["RESTPLUS_MASK_SWAGGER"] = False

# Swagger Models
# ----------
# DEPRECATED
# game_date_pair = api.model('Game Date Pair', GameDatePair)
# game_model = api.model('Game', GameModel)
# get_games_model = api.model('Get Games Model', {'games': fields.List(fields.Nested(game_model))})
#DEPRECATED

charts_model = api.model('Chart Object', ChartsObject)

umpire_id_pair = api.model('Umpire ID Pair', UmpireIDPair)
get_all_umpire_id_pairs = api.model('Umpire ID Pairs', {'umpires': fields.List(fields.Nested(umpire_id_pair))})

rankings_api_object = api.model('Ranking Umpire Item', RankingsObjects)
umpire_model = api.model('Umpire', UmpireObject)

# UmpireInfo['team'] = TeamObject
# UmpireInfo['career'] = CareerObject


career_model = api.model('Career', CareerObject)
umpire_game_model = api.model('Umpire Game', UmpireGameObject)
search_api_object = api.model('Search Items', SearchObject)
team_model = api.model('Team', TeamObject)
pitcher_model = api.model('Pitcher', PitcherObject)
# get_umpire_info_model = api.model('Get Umpire Info', {'umpire': UmpireObject})
# Parsers
# ----------
search_parser = api.parser()
search_parser.add_argument('q', type=str, help=
    '''query string which will find relevant Umpire, and Game data
        
    ?q="jordan baker"''', required=True)

umpire_parser = api.parser()
umpire_parser.add_argument('name', type=str, help=
    '''umpire name to get

    ?name=jordan baker or ?name=jordan%20baker''', required=True)

game_parser = api.parser()
game_parser.add_argument('game', type=int, help=
    '''game id
    ?game=564837''')

team_parser = api.parser()
team_parser.add_argument('name', type=str, help=
    '''umpire full name
    ?name=jordan baker''')
team_parser.add_argument('team', type=str, help=
    '''team name completely capitalized
    ?name=BOS''')

pitcher_parser = api.parser()
pitcher_parser.add_argument('pitcher_name', type=str, help="pitcher fullname")
