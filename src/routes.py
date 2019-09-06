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

@api.route('/get-umpire-info')
class UmpireInfo(Resource):
    @api.doc(parser = umpire_parser)
    @api.response(200, 'OK', umpire_model)
    def get(self):
        """
        Returns the complete umpire data object
        """
        now = time.time()
        name = request.args.get('name')
        name = ' '.join([word.capitalize() for word in name.split()])

        data = create_umpire_object(name, careers, careers_season, crews, careers_range, data_year_range)
        data['career'] = create_career_object(name, careers_season, crews, careers_range, careers_range_change, data_year_range)
        data['team'] = []
        for team in team_names:
            data['team'] += create_team_object(name, team, team_stats_dataset, data_year_range)
        data['pitchers'] = pitcher_objects[name]

        data = json.dumps(data, use_decimal = True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp


@api.route('/pitchers')
class Pitchers(Resource):
    @api.doc(parser = pitcher_parser)
    @api.response(200, 'OK', pitcher_model)
    def get(self):
        name = request.args.get('pitcher_name')
        data = create_pitcher_object(name, pitcher_stats, data_year_range)
        data = json.dumps(data, use_decimal = True)
        resp = Response(data, status=200, mimetype='application/json')
        return resp
        

@api.route('/teams')
class Teams(Resource):
    @api.doc(parser = team_parser)
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
        team = request.args.get('team')
        data = create_team_object(name, team, team_stats_dataset, data_year_range)
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
        Will return an array of yearly career objects about this umpire

        Description
        ----------
        Takes in some full umpire name and generates a career object. Response will be
        an array of career objects
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
        data = create_umpire_game_object(name, games_dataset, ump_game_lookup)
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
