from flask import Flask, jsonify, request, Response
import simplejson as json
import sys, os
import boto3
import time
from boto3.dynamodb.conditions import Key, Attr
from flask_cors import CORS
from flask import Flask, request
from flask_restplus import Resource, Api, reqparse, fields


# Import custom dependencies
from models import *
with open('.config.json') as f:
    configs = json.load(f)
sys.path.append('./src')
from Datasets import Table
from CloudSearch import Search
from EndpointFunctions import *

# Connect boto3 resources
iam = configs['iam-user']
umpires_text_search = Search(iam, configs['cloudsearch']['umpires']['url'], 
    configs['cloudsearch']['umpires']['name'])
games_text_search = Search(iam, configs['cloudsearch']['games']['url'],
        configs['cloudsearch']['games']['name'])

team_stats_dataset = Table(iam, 'refrating-team-stats-v1', umpires_text_search)
games_dataset = Table(iam, 'refrating-game-stats-v1', games_text_search)
umpire_id_lookup = Table(iam, 'refrating-umps-lookup')
games_date_lookup = Table(iam, 'refrating-games-lookup')
careers_season = Table(iam, 'refrating-careers-season')
careers_range = Table(iam, 'refrating-career-range')
careers_range_change = Table(iam, 'refrating_career_range_change')
careers = Table(iam, 'refrating-careers')
crews = Table(iam, 'refrating-crews')

data_year_range = range(2010, 2020)

ALL_UMPIRE_KEYS = umpire_id_lookup.scan()
ALL_UMPIRE_NAMES = [obj['name'] for obj in ALL_UMPIRE_KEYS]
RANKINGS_OBJECT = create_rankings_object(careers_season, team_stats_dataset, ALL_UMPIRE_NAMES, data_year_range)
RANKINGS_OBJECT = json.dumps(RANKINGS_OBJECT, use_decimal=True)
RANKINGS_OBJECT = Response(RANKINGS_OBJECT, status=200, mimetype='application/json')
print('Finished caching heavy load data')

# Flask Objects
# ----------
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

umpire_id_pair = api.model('Umpire ID Pair', UmpireIDPair)
get_all_umpire_id_pairs = api.model('Umpire ID Pairs', {'umpires': fields.List(fields.Nested(umpire_id_pair))})

rankings_api_object = api.model('Ranking Umpire Item', RankingsObjects)
umpire_model = api.model('Umpire', UmpireObject)
career_model = api.model('Career', CareerObject)
umpire_game_model = api.model('Umpire Game', UmpireGameObject)
search_api_object = api.model('Search Items', SearchObject)
team_model = api.model('Team', TeamObject)
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

@api.route('/teams')
class Teams(Resource):
    @api.doc(parser = umpire_parser)
    @api.response(200, 'OK', team_model)
    def get(self):
        """
        Will return a dict where keys represent years, and values are the team object

        Description
        ----------
        Takes in some full umpire name and generates a team object
        keyed by years where the values will be of the format of the below team
        model
        """
        name = request.args.get('name')
        data = create_team_object(name, team_stats_dataset, data_year_range)
        data = json.dumps(data, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp

@api.route('/umpire')
class Umpire(Resource):
    @api.doc(parser = umpire_parser)
    @api.response(200, 'OK', umpire_model)
    def get(self):
        """
        Will return a dict where keys represent years, and values are the umpire object

        Description
        ----------
        Takes in some full umpire name and generates an umpire object
        keyed by years where the values will be of the format of the below umpire
        model
        """
        name = request.args.get('name')
        data = create_umpire_object(name, careers, careers_season, crews, careers_range, data_year_range)
        data = json.dumps(data, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp


@api.route('/rankings')
class Rankings(Resource):
    @api.response(200, 'OK', rankings_api_object)
    def get(self):
        """
        Returns a list of all umpire objects from every year in the rankings format
        """ 
        return RANKINGS_OBJECT



@api.route('/career')
class Career(Resource):
    @api.doc(parser = umpire_parser)
    @api.response(200, 'OK', career_model)
    def get(self):
        """
        Will return a career object about this umpire

        Description
        ----------
        Takes in some full umpire name and generates a career object. Response will be
        a dictionary where keys represent the year, and values will be the umpires career
        stats for that year
        """
        name = request.args.get('name')
        data = create_career_object(name, careers_season, crews, careers_range, careers_range_change, data_year_range)
        data = json.dumps(data, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp

@api.route('/search')
class QuerySearch(Resource):
    @api.doc(parser=search_parser)
    @api.response(200, 'OK', search_api_object)
    def get(self):
        """
        Search query against our database and get relevant data
        
        Description
        ----------
        Takes in some arbitrary query string such as 'Jordan Baker'
        and returns relevant umpire name results with their respective profile picture.
        """
        query = request.args.get('q')
        resp = umpires_text_search.get(query)
        for obj in resp:
            obj.update({'ump_profile_pic': 'https://{0}.s3.amazonaws.com/umpires/{1}+{2}'.format(
                configs['media_bucket'],
                *obj['name'][0].split()
            )})
        data = json.dumps(resp, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp


@api.route('/get-all-ump-ids')
class GetAllUmps(Resource):
    @api.response(200, 'OK', get_all_umpire_id_pairs)
    def get(self):
        """
        Returns the names with our unique identifiers for every umpire within our dataset

        Description
        ----------
        Will return a list of all umpire names and id's. Can be used as a quick hash map
        to convert id's into names and vice versa, or to simply have a list of all umpire names
        """
        data = json.dumps({'umpires': ALL_UMPIRE_KEYS}, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp


@api.route('/umpireGames')
class UmpireGames(Resource):
    @api.doc(parser = game_parser)
    @api.response(200, 'OK', umpire_game_model)
    def get(self):
        """
        Returns a game object for some umpire given the game id

        Description
        ----------
        Will return a game object for some umpire given the game id. See below for
        return format
        """
        game = int(request.args.get('game'))
        data = create_umpire_game_object(game, games_dataset, data_year_range)
        data = json.dumps(data, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp

# get_games_parser = api.parser()
# get_games_parser.add_argument('start', type=str, help='20xx-xx-xx', required=True)
# get_games_parser.add_argument('end', type=str, help='20xx-xx-xx', required=True)
# @api.route('/get-games', methods=['GET'])
# class GetGames(Resource):
#     @api.response(200, 'OK', get_games_model)
#     @api.doc(parser=get_games_parser)
#     def get(self):
#         """
#         Returns every Game object within our database within some given time frame.

#         Description
#         ----------
#         Will return every game object within some timeframe.
#         """
#         if request.method == 'GET':
#             try:
#                 start = str(request.args.get('start'))
#                 end = str(request.args.get('end'))
#             except ValueError as e:
#                 return 'Please give start and end number fields', 200
#             filterExpression = Attr('date').between(start, end)
#             resp = games_date_lookup.scan(FilterExpression=filterExpression)
#             for i in range(len(resp)):
#                 query = {
#                     'game': resp[i]['game']
#                 }
#                 resp[i] = games_dataset.get(query)
#             data = json.dumps(
#                 {'games':resp}, use_decimal=True
#             )
#             resp = Response(data, status=200, mimetype='application/json')
#             return resp


if __name__ == '__main__':
    # getUmpires()
    # app.run('0.0.0.0', port=80)
    if len(sys.argv) > 1:
        if sys.argv[1] == 'p':
            print('Starting in production mode')
            app.config["DEBUG"] = False
            app.run('0.0.0.0', port=80)
    else:
        print('Starting in testing mode')
        app.config["DEBUG"] = True
        app.run('127.0.0.1', port=3000)



