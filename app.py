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


# Connect boto3 resources
umpires_text_search = Search(configs['iam-user'], configs['cloudsearch']['umpires']['url'], 
    configs['cloudsearch']['umpires']['name'])
games_text_search = Search(configs['iam-user'], configs['cloudsearch']['games']['url'],
        configs['cloudsearch']['games']['name'])
umpires_dataset = Table(configs['iam-user'], 'refrating-team-stats-v1', umpires_text_search)
games_dataset = Table(configs['iam-user'], 'refrating-game-stats-v1', games_text_search)
umpire_id_lookup = Table(configs['iam-user'], 'refrating-umps-lookup')
# ALL_UMPIRE_DATA = umpires_dataset.scan()
games_date_lookup = Table(configs['iam-user'], 'refrating-games-lookup')


# Setup flask cors and swagger
app = Flask(__name__)
api = Api(app, default ="Umpires and Games")
CORS(app)
app.config["RESTPLUS_MASK_SWAGGER"] = False


# Create swagger documentation objects
game_date_pair = api.model('Game Date Pair', GameDatePair)
get_games_model = api.model('Get Games Model', {'games': fields.List(fields.Nested(game_date_pair))})

umpire_model = api.model('Umpire', UmpireModel)
get_all_umps_model = api.model('Umpire', {'umpires': fields.List(fields.Nested(umpire_model))})

umpire_id_pair = api.model('Umpire ID Pair', UmpireIDPair)
get_all_umpire_id_pairs = api.model('Umpire ID Pairs', {'umpires': fields.List(fields.Nested(umpire_id_pair))})

game_model = api.model('Game', GameModel)
search_api_object = api.model('SearchAPIObject', 
    {
        'umpire-search-results': fields.List(fields.Nested(umpire_model))
    }
)



umpires_model = api.model('Umpires', {'items': fields.List(fields.Nested(umpire_model))})
search_parser = api.parser()
search_parser.add_argument('q', type=str, help=
    '''query string which will find relevant Umpire, and Game data
        
    ?q="jordan baker"''', required=True)


# Setup endpoints
@api.route('/search')
class QuerySearch(Resource):
    @api.doc(parser=search_parser)
    @api.response(200, 'OK', search_api_object)
    def get(self):
        """
        Search query against our database and get relevant data
        
        Description
        ----------
        Takes in some arbitrary query string such as 'Jordan Baker' or '1984 all stars'
        which we use to find relevant search results within both our games and umpire databases.
        Unfourtunately, games aren't quite functional yet so only umpire search results will be relevant
        for the time being. Return data will be a dictionary with two keys, 'umpire-search-results' 
        which contains an array of Umpire objects, and 'game-search-results' which is an array of
        Game objects. Both arrays are sorted with respect to relevancy with more relevant searches 
        being at smaller indices.
        """
        query = request.args.get('q')

        # print(umpires_text_search.get(query))
        resp = umpires_text_search.get(query)
        for obj in resp:
            obj.update({'ump_profile_pic': 'https://{0}.s3.amazonaws.com/umpires/{1}+{2}'.format(
                configs['media_bucket'],
                *obj['name'][0].split()
            )})
        data = {
            'umpire-search-results': resp
        }
        data = json.dumps(data, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp

@api.route('/get-all-ump-ids')
class GetAllUmps(Resource):
    @api.response(200, 'OK', get_all_umpire_id_pairs)
    def get(self):
        """
        Returns the names and our unique identifiers for every umpire within our dataset

        Description
        ----------
        Will return a list of all umpire names and id's. Can be used as a quick hash map
        to convert id's into names and vice versa, or to simply have a list of all umpire names
        """
        data = umpire_id_lookup.scan()
        data = json.dumps({'umpires': data}, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp



@api.route('/get-all-umps')
class GetAllUmps(Resource):
    @api.response(200, 'OK', get_all_umps_model)
    def get(self):
        """
        Returns the names and our unique identifiers for every umpire within our dataset

        Description
        ----------
        Will return a list of all umpire names and id's. Can be used as a quick hash map
        to convert id's into names and vice versa, or to simply have a list of all umpire names
        """
        data = json.dumps({'umpires': ALL_UMPIRE_DATA}, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp

get_games_parser = api.parser()
get_games_parser.add_argument('start', type=str, help='20xx-xx-xx', required=True)
get_games_parser.add_argument('end', type=str, help='20xx-xx-xx', required=True)
@api.route('/get-games', methods=['GET'])
class GetGames(Resource):
    @api.response(200, 'OK', get_games_model)
    @api.doc(parser=get_games_parser)
    def get(self):
        """
        Returns every Game object within our database.

        Description
        ----------
        Will return every game object within some timeframe. Is currently deprecated because
        game data is currently mock info and not usable.
       
        """
        if request.method == 'GET':
            try:
                start = str(request.args.get('start'))
                end = str(request.args.get('end'))
            except ValueError as e:
                return 'Please give start and end number fields', 200
            filterExpression = Attr('date').between(start, end)
            resp = games_date_lookup.scan(FilterExpression=filterExpression)
            data = json.dumps(
                {'games':resp}, use_decimal=True
            )
            print(resp)

            resp = Response(data, status=200, mimetype='application/json')
            return resp

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



