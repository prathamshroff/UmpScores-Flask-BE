from StorageSolutions.flask_setup import *
from StorageSolutions.tables import *
from Util.EndpointFunctions import *
from Util.RefratingCache import *
from flask_restplus import Resource, Api, reqparse, fields
from flask import Flask, request
import simplejson as json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from flask import Flask, jsonify, request, Response
import time
import threading
# @api.route('/get-umpire-info')
# class UmpireInfo(Resource):
#     @api.doc(parser = umpire_parser)
#     @api.response(200, 'OK', umpire_model)
#     def get(self):
#         """
#         Returns the complete umpire data object
#         """
#         now = time.time()
#         name = request.args.get('name')
#         name = ' '.join([word.capitalize() for word in name.split()])

#         data = create_umpire_object(name, careers, careers_season, crews, careers_range, data_year_range)
#         data['career'] = create_career_object(name, careers_season, crews, careers_range, careers_range_change, data_year_range)
#         data['team'] = []
#         for team in team_names:
#             data['team'] += create_team_object(name, team, team_stats_dataset, data_year_range)
#         # data['pitchers'] = pitcher_objects[name]

#         data = json.dumps(data, use_decimal = True)
#         resp = Response(data, status=200, mimetype='application/json')
#         return resp

cache_lock = threading.Lock()

@api.route('/charts')
class Charts(Resource):
    @api.doc(parser = umpire_parser)
    # @api.marshal_with(charts_model)
    def get(self):
        name = request.args.get('name')
        data = create_chart_object(name, data_year_range)
        data = json.dumps(data, use_decimal = True)
        resp = Response(data, status=200, mimetype = 'application/json')
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
        data = get_pitcher_names(name)
        data = json.dumps(data, use_decimal = True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp
        

@api.route('/teams')
class Teams(Resource):
    @api.doc(parser = umpire_parser)
    # @api.response(200, 'OK', team_model)
    def get(self):
        """
        Will return an array of dicts where a dict represents team stats
        for that object

        Description
        ----------
        Takes in some full umpire name and generates an array of team objects
        """
        name = request.args.get('name')
        data = create_team_object(name, data_year_range)
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
        name = ' '.join([word.lower().capitalize() for word in name.split()])
        data = cache[cache['use']]['umpires'][name]
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
        return cache[cache['use']]['rankings']



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
        name = ' '.join([word.lower().capitalize() for word in name.split()])
        data = cache[cache['use']]['career'][name]
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
    @api.response(200, 'OK', get_all_umpire_id_pairs)
    def get(self):
        games = json.dumps(cache[cache['use']]['games'])
        resp = Response(games, status=200, mimetype='application/json')
        return resp

@api.route('/recache')
class Recache(Resource):
    @api.doc(parser=cache_parser)
    def get(self):
        password = request.args.get('secret')
        try:
            cache_lock.acquire()
            if password == configs['privilege_secret']:
                cache[cache['use']]['games'] = get_all_games(ALL_UMPIRE_NAMES)
                cache[cache['use']]['umpires'] = refPool.starmap(create_umpire_object, [(name, data_year_range[-1]) for name in ALL_UMPIRE_NAMES])
                cache[cache['use']]['umpires'] = {obj['name']: obj for obj in cache['umpires'] if 'name' in obj}                

                cache[cache['use']]['rankings'] = refPool.starmap(create_rankings_object, [(name, data_year_range) for name in ALL_UMPIRE_NAMES])
                cache[cache['use']]['rankings'] = json.dumps(cache[cache['use']]['rankings'], use_decimal=True)
                cache[cache['use']]['rankings'] = Response(cache[cache['use']]['rankings'], status=200, mimetype='application/json')
                return Response([], status=200)
                
            else:
                return Response([], status=400)
        finally:
            cache_lock.release()

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
        data = json.dumps({'umpires': ALL_UMPIRE_KEYS}, use_decimal=True)
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
        data = create_umpire_game_object(name)
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
