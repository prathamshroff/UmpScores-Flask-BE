from StorageSolutions.flask_setup import *
from StorageSolutions.tables import *
from Util.EndpointFunctions import *
"""from Util.RefratingCache import recache_everything, recache_games"""
from flask_restplus import Resource, Api, reqparse, fields
from flask import Flask, request
import simplejson as json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from flask import Flask, jsonify, request, Response
from multiprocessing.pool import ThreadPool as Pool
import time
import threading
data_year_range = range(2010, 2020)

cache_lock = threading.Lock()
cache = {'blue': {}, 'green': {}, 'use': 'blue'}

refPool = Pool()
#recache_everything(cache, cache_lock, refPool, data_year_range)

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
        name = request.args.get('name')
        data = json.dumps(cache[cache['use']]['/charts'][name.lower()], use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp

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
        umpire_name = request.args.get('u')
        pitcher_name = request.args.get('p')
        data = create_pitcher_object(umpire_name, pitcher_name)
        data = json.dumps(data, use_decimal = True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp

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
        name = request.args.get('name')
        data = json.dumps(cache[cache['use']]['/get-pitchers'][name.lower()], use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp
        

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
        name = request.args.get('name')
        data = json.dumps(cache[cache['use']]['/teams'][name.lower()], use_decimal=True)
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
        data = cache[cache['use']]['/umpire'][name.lower()]
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
        return cache[cache['use']]['/rankings']



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
        name = request.args.get('name')
        data = cache[cache['use']]['/career'][name.lower()]
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

@api.route('/games')
class GetTodaysGames(Resource):
    def get(self):
        """
        Generates a list of games for today

        Description
        ----------
        Will return a cached object representing the games for this day
        """
        games = json.dumps(cache[cache['use']]['/games'], use_decimal=True)
        resp = Response(games, status=200, mimetype='application/json')
        return resp

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
        password = request.args.get('secret')
        if password == configs['privilege_secret']:
            recache_everything(cache, cache_lock, refPool, data_year_range)
            return Response([], status=200)
            
        else:
            return Response([], status=400)

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
        password = request.args.get('secret')
        if password == configs['privilege_secret']:
            recache_games(cache, cache_lock)
            return Response([], status=200)
        else: 
            return Response([], status=400)

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
        data = json.dumps(cache[cache['use']]['/umpireList'], use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp


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
        name = request.args.get('name')
        data = cache[cache['use']]['/umpireGames'][name.lower()]
        data = json.dumps(data, use_decimal=True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp


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
        return cache['use']

@api.route('/awards')
class Awards(Resource):
    def get(self):
        data = [{
                "Name": "Sample Name",
                "Information": "Sample Information"
            },
            {
                "Name": "Sample Name 2",
                "Information": "Sample Information 2"
            }
        ]
        return data

