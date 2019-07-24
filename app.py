from flask import Flask, jsonify, request, Response
import simplejson as json
import sys, os
import boto3
import time
from boto3.dynamodb.conditions import Key, Attr
from flask_cors import CORS
from flask import Flask, request
from flask_restplus import Resource, Api, reqparse, fields
from models import UmpireModel, GameModel
sys.path.append('./src')
from Datasets import Dataset
from CloudSearch import Search

with open('.config.json') as f:
    configs = json.load(f)

app = Flask(__name__)
api = Api(app, default ="Umpires and Games")
CORS(app)
app.config["RESTPLUS_MASK_SWAGGER"] = False

# Custom boto3 wrappers. Will handle data sanitization and malicious queries in the future
umpires_dataset = Dataset(configs['iam-user'], 'refrating-umpires-v1')
games_dataset = Dataset(configs['iam-user'], 'Refrating-Games')
umpires_text_search = Search(configs['iam-user'], configs['cloud-search']['umpires-url'])
games_text_search = Search(configs['iam-user'], configs['cloud-search']['games-url'])

umpire_model = api.model('Umpire', UmpireModel)
game_model = api.model('Game', GameModel)
search_api_object = api.model('SearchAPIObject', 
    {
        'umpire-search-results': fields.List(fields.Nested(umpire_model)),
        'game-search-results': fields.List(fields.Nested(game_model))
    }
)
umpires_model = api.model('Umpires', {'items': fields.List(fields.Nested(umpire_model))})

search_parser = api.parser()
search_parser.add_argument('q', type=str, help=
    '''query string which will find relevant Umpire, and Game data
        
    ?q="jordan baker"''', required=True)


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
        data = {
            'umpire-search-results': umpires_text_search.get(query), 
            'game-search-results': games_text_search.get(query)
        }
        data = json.dumps(data, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp

@api.route('/get-all-umps')
class GetAllUmps(Resource):
    @api.response(200, 'OK', umpires_model)
    def get(self):
        """
        Returns every umpire within the Umpires dynamodb table

        Description
        ----------
        Will return a list of every Umpire object we own. Used for the front end to periodically cache 
        Umpires. Ideally, this dataset will be updated/changed at some set periodic interval to include
        recent game statistics.
        """
        data = umpires_dataset.scan()
        data = json.dumps({'items': data}, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp

# TODO handle start and end times differently with actual time objects 
# instead of just integers
get_games_parser = api.parser()
get_games_parser.add_argument('start', type=int, help='Starting point for the timeframe', required=True)
get_games_parser.add_argument('end', type=int, help='Ending point for the timeframe', required=True)
@api.deprecated
@api.route('/get-games', methods=['GET'])
class GetGames(Resource):
    @api.marshal_list_with(game_model)
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
                start = int(request.args.get('start'))
                end = int(request.args.get('end'))
            except ValueError as e:
                return 'Please give start and end number fields', 200

            filterExpression = Attr('timeStamp').between(start, end)
            data = json.dumps(
                {'items': games_dataset.scan(FilterExpression=filterExpression)}, use_decimal=True
            )
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
