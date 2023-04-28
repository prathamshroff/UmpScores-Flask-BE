from StorageSolutions.flask_setup import *
from StorageSolutions.tables import *
from Util.EndpointFunctions import *
from Util.RefratingCache import recache_everything, recache_games
from flask_restx import Resource, Api, reqparse, fields
from flask_swagger_ui import get_swaggerui_blueprint
from flask import Flask, request
import simplejson as json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from flask import Flask, jsonify, request, Response
from multiprocessing.pool import ThreadPool as Pool
import time
import threading

from flask_swagger_ui import get_swaggerui_blueprint

data_year_range = range(2010, 2022)

cache_lock = threading.Lock()
cache = {'blue': {}, 'green': {}, 'use': 'blue'}

refPool = Pool()
recache_everything(cache, cache_lock, refPool, data_year_range)

# Define the Swagger UI blueprint
SWAGGER_URL = '/swagger'
API_URL = '/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': 'UmpScores API'
        'title': 'UmpScores API',
        'description': 'This is the documentation for the UmpScores API, which provides access to umpire scores and statistics.'
    }
)

# Register the Swagger UI blueprint with your Flask app
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

@api.route('/charts')
class Charts(Resource):
    """
    Returns chart statistics regarding a specific umpire

    Description
    ----------
    Give an umpire name and returns chart statistics for the front end
    """
    @api.doc(parser = umpire_parser)
    def get(self):
        try: 
            name = request.args.get('name')
            data = json.dumps(cache[cache['use']]['/charts'][name.lower()], use_decimal=True)
            resp = Response(data, status=200, mimetype='application/json')
            return resp
        except Exception as e:
            return {'error': str(e)}, 500

@api.route('/pitcher')
class Pitcher(Resource):
    """
    Get statistics for every season a player has worked with a given umpire

    Description
    ----------
    Give an umpire name and a pitcher name and retrieve a array of dicts where
    each dict represents one seasonal statistic for that umpire pitcher pair.
    To be used in conjunction with /get-pitchers endpoint
    """
    @api.doc(parser = pitcher_parser)
    def get(self):
        try: 
            umpire_name = request.args.get('u')
            pitcher_name = request.args.get('p')
            data = create_pitcher_object(umpire_name, pitcher_name)
            data = json.dumps(data, use_decimal = True)
            resp = Response(data, status=200, mimetype='application/json')
            return resp
        except Exception as e:
            return {'error': str(e)}, 500

@api.route('/get-pitchers')
class GetPitchers(Resource):
    """
    Will return a list of pitcher names

    Description
    ----------
    Give an umpire name and get a list of pitchers that umpire has
    worked with. Used in conjunction with /pitcher endpoint
    """
    @api.doc(parser = umpire_parser)
    def get(self):
        try: 
            name = request.args.get('name')
            data = json.dumps(cache[cache['use']]['/get-pitchers'][name.lower()], use_decimal=True)
            resp = Response(data, status=200, mimetype='application/json')
            return resp
        except Exception as e:
            return {'error': str(e)}, 500
        
@api.route('/teams')
class Teams(Resource):
    @api.doc(parser = umpire_parser)
    def get(self):
        """
        Will return an array of dicts where a dict represents team stats
        for that object

        Description
        ----------
        Takes in some full umpire name and generates an array of team objects
        """
        try: 
            name = request.args.get('name')
            data = json.dumps(cache[cache['use']]['/teams'][name.lower()], use_decimal=True)
            resp = Response(data, status=200, mimetype='application/json')
            return resp
        except Exception as e:
            return {'error': str(e)}, 500

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
        try: 
            name = request.args.get('name')
            data = cache[cache['use']]['/umpire'][name.lower()]
            data = json.dumps(data, use_decimal=True)
            resp = Response(data, status=200, mimetype='application/json')
            return resp
        except Exception as e:
            return {'error': str(e)}, 500

@api.route('/rankings')
class Rankings(Resource):
    @api.response(200, 'OK', rankings_api_object)
    def get(self):
        """
        Returns a list of all umpire objects from every year in the rankings format
        """ 
        try: 
            return cache[cache['use']]['/rankings']
        except Exception as e:
            return {'error': str(e)}, 500

@api.route('/career')
class Career(Resource):
    @api.doc(parser = umpire_parser)
    @api.response(200, 'OK', career_model)
    def get(self):
        """
        Will return an array of yearly career objects about this umpire

        Description
        ----------
        Takes in some full umpire name and generates a career object. Response will be
        an array of career objects
        """
        try:
            name = request.args.get('name')
            data = cache[cache['use']]['/career'][name.lower()]
            data = json.dumps(data, use_decimal=True)
            resp = Response(data, status=200, mimetype='application/json')
            return resp
        except Exception as e:
            return {'error': str(e)}, 500

@api.route('/games')
class GetTodaysGames(Resource):
    def get(self):
        """
        Generates a list of games for today

        Description
        ----------
        Will return a cached object representing the games for this day
        """
        try:
            games = json.dumps(cache[cache['use']]['/games'], use_decimal=True)
            resp = Response(games, status=200, mimetype='application/json')
            return resp
        except Exception as e:
            return {'error': str(e)}, 500

@api.route('/recache')
class Recache(Resource):
    @api.doc(parser=cache_parser)
    def get(self):
        """
        Will recache all internal objects

        Description
        ----------
        Takes in a secret. If the secret is valid, caching will commence.
        """
        try: 
            password = request.args.get('secret')
            if password == configs['privilege_secret']:
                recache_everything(cache, cache_lock, refPool, data_year_range)
                return Response([], status=200)
                
            else:
                return Response([], status=400)
        except Exception as e:
            return {'error': str(e)}, 500

@api.route('/updateGamesCache')
class UpdateGamesCache(Resource):
    @api.doc(parser=cache_parser)
    def get(self):
        """
        Will recache all game objects

        Description
        ----------
        Takes in a secret. If the secret is valid, caching will commence.
        """
        try: 
            password = request.args.get('secret')
            if password == configs['privilege_secret']:
                recache_games(cache, cache_lock)
                return Response([], status=200)
            else: 
                return Response([], status=400)
        except Exception as e:
            return {'error': str(e)}, 500

@api.route('/umpireList')
class GetAllUmps(Resource):
    def get(self):
        """
        Returns the names with our unique identifiers for every umpire within our dataset

        Description
        ----------
        Will return a list of all umpire names and id's. Can be used as a quick hash map
        to convert id's into names and vice versa, or to simply have a list of all umpire names
        """
        try:
            data = json.dumps(cache[cache['use']]['/umpireList'], use_decimal=True)
            resp = Response(data, status=200, mimetype='application/json')
            return resp
        except Exception as e:
            return {'error': str(e)}, 500

@api.route('/umpireGames')
class UmpireGames(Resource):
    @api.doc(parser = umpire_parser)
    @api.response(200, 'OK', umpire_game_model)
    def get(self):
        """
        Returns all game objects for some umpire

        Description
        ----------
        Will return a game object for some umpire given the game id. See below for
        return format
        """
        try: 
            name = request.args.get('name')
            data = cache[cache['use']]['/umpireGames'][name.lower()]
            data = json.dumps(data, use_decimal=True)
            resp = Response(data, status=200, mimetype='application/json')
            return resp
        except Exception as e:
            return {'error': str(e)}, 500

@api.route('/whichCache')
class UmpireGames(Resource):
    def get(self):
        """
        Returns which deployment is currently being used

        Description
        ----------
        Currently using a blue green deployment setup. Depending on which
        is currently in use, output of this endpoint will either be blue or green.
        Used for debugging purposes and to test if caching was done correctly.
        """
        try: 
            return cache['use']
        except Exception as e:
            return {'error': str(e)}, 500

@api.route('/awards')
class Awards(Resource):
    @api.doc(parser = awards_parser)
    def get(self):
        """
        Returns award winners for a given award category and status. 
        
        Description
        -----------

        Categories include Best Crew, Most Improved, Rising Star, and Strongest Performance
        Statuses include FT and CU. Best Crew does not have a status so it does not require a status parameter.
        """
        try:
            data = cache[cache['use']]['/awards']
            award_category = request.args.get("category")
            if (award_category == "Best Crew Chief" or award_category == "Best Crew"):
                status = "null"
            else:
                status = request.args.get("status")
            return data[award_category][status]
        except Exception as e:
            return {'error': str(e)}, 500

@api.route("/awardCategories")
class AwardCategories(Resource):
    def get(self):
        """
        Return all the different types of awards.
        """
        try: 
            data = cache[cache['use']]['/awards']["Award Categories"]
            return data 
        except Exception as e:
            return {'error': str(e)}, 500     
