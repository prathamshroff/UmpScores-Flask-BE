import flask
from flask import make_response, request, current_app, jsonify
from datetime import timedelta
from functools import update_wrapper
from werkzeug import generate_password_hash, check_password_hash
from flask_jwt_extended import (JWTManager, create_access_token, create_refresh_token, jwt_required, jwt_refresh_token_required, get_jwt_identity, get_raw_jwt)
import flask_bcrypt
from flask_cors import CORS, cross_origin
from werkzeug import serving
import ssl

import json
from db import *


app = flask.Flask(__name__)
CORS(app)
app.config["DEBUG"] = True
# JWT stuff is not relevant for this build
app.config['JWT_SECRET_KEY'] = 'super-secret-12'
app.config['CORS_ORIGINS'] = 'https://evening-badlands-39009.herokuapp.com, http://localhost:3000'
app.config['CORS_HEADERS'] = 'X-Requested-With, Content-Type, Authorization, Origin, Accept'

bcrypt = flask_bcrypt.Bcrypt(app)
jwt = JWTManager(app)

def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, list):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, list):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

@app.route('/', methods=['GET'])
@crossdomain(origin='*')
def home():
	test = testing()
	print(test)
	return jsonify({"responseStatus":"success"})

@app.route('/create-account', methods= ['GET'])
@crossdomain(origin='*')
def createAccount():
	print("hitting the back end correctly")
	# print("REQUEST EMAIL: ", request.args['email'])
	# return jsonify({"responseStatus":"success"})
	email = request.args['email']
	password = request.args['password']
	confirmPassword = request.args['confirmPassword']
	userType = 'email'
	is_user = isUser(email, userType)
	is_user = json.loads(is_user.get_data().decode("utf-8"))
	is_user = is_user['user_exists']
	print("user exists status: ", is_user)
	if (is_user):
		# account with that email already exists, 
		return jsonify({"responseStatus": "error", "errorMessage":"Account already exists with that email!", "email":email, "password":password})
	else:
		if (password == confirmPassword):
			# create password hash
			print("make a password hash")
			pw_hash = bcrypt.generate_password_hash(password).decode('utf-8')
			addUser(email, pw_hash)
		else:
			return jsonify({"responseStatus": "error", "errorMessage":"Passwords do not match!", "email":email, "password":password})
	return jsonify({"responseStatus":"success", "errorMessage":"none", "email":email, "password":password})

@app.route('/create-google-account', methods=['POST'])
@crossdomain(origin='*')
def createGoogleAccount():
	# define params from request
	userEmail = request.args['email']
	firstName = request.args['firstName']
	lastName = request.args['lastName']
	googleId = request.args['googleId']
	userType = 'google'

	print("create google account email: ", userEmail)
	is_user = isUser(userEmail, userType)
	is_user = json.loads(is_user.get_data().decode("utf-8"))
	is_user = is_user['user_exists']
	if (is_user):
		print("user already exists in database")
		return jsonify({"responseStatus": "error", "errorMessage":"Account already exists with that email!"})
	else:
		# need to create an addGoogleUser() function to add the user to the db
		print("creating new google user")
		add_user = addGoogleUser(userEmail, userType, firstName, lastName, googleId)
		add_user = json.loads(add_user.get_data().decode("utf-8"))
		print(add_user)
		return jsonify({"responseStatus":"success"})

@app.route('/login', methods=['GET', 'OPTIONS'])
# @cross_origin()
@crossdomain(origin='*')
def login():
	# get url args
	email = request.args['email']
	password = request.args['password']
	# pass jsonified email + bcrypted password
	userType = "email"
	is_user = isUser(email, userType)
	is_user = json.loads(is_user.get_data().decode("utf-8"))
	is_user = is_user['user_exists']
	if(is_user):
		# user exists, check password
		user = getUser(email)
		user = json.loads(user.get_data().decode("utf-8"))
		user_pw_hash = user["db_hashed_pw"]
		user_email = user["db_email"]
		if (bcrypt.check_password_hash(user_pw_hash, password)):
			# create access token and refresh token and add to a cookie
			access_token = create_access_token(identity=email)
			refresh_token = create_refresh_token(identity = email)
			return jsonify({"responseStatus":"success", "email":email, "access_token":access_token, "refresh_token":refresh_token})
		else:
			return jsonify({"responseStatus":"error", "errorMessage":"Incorrect password!"})
	else:
		# user does not exist, print a message that says email does not exist
		return jsonify({"responseStatus":"error", "errorMessage":"User does not exist!"})

@app.route('/login-google', methods=['GET'])
# @crossdomain(origin='*')
def loginGoogle():
	userEmail = request.args['email']
	googleId = request.args['googleId']
	userType = "google"
	is_google_user = isGoogleUser(userEmail, userType)
	is_google_user = json.loads(is_google_user.get_data().decode("utf-8"))
	is_google_user = is_google_user["google_user_exists"]
	print("is google user status: ", is_google_user)
	if (is_google_user):
		access_token = create_access_token(identity=userEmail)
		refresh_token = create_refresh_token(identity = userEmail)
		return jsonify({"responseStatus":"success", "email":userEmail, "access_token":access_token, "refresh_token":refresh_token})
	else:
		return jsonify({"responseStatus":"error", "errorMessage":"Account error"})

@app.route('/data', methods=['GET'])
# @crossdomain(origin='*')
@jwt_required
def data():
	user = get_jwt_identity()
	dataset = request.args["dataset"];
	response = getLineGraphData(user, dataset)
	response = json.loads(response.get_data().decode("utf-8"))
	print(response["responseStatus"])
	if (response["responseStatus"] == "success"):
		# good data load,
		return jsonify({"responseStatus":"success", "data":response["data"]})
	else:
		return jsonify({"responseStatus":"error"})


@app.route('/testing-action', methods=['GET'])
@jwt_required
def testingAction():
	user = get_jwt_identity()
	dataset = request.args["dataset"];
	response = getLineGraphData(user, dataset)
	response = json.loads(response.get_data().decode("utf-8"))
	print(response["responseStatus"])
	if (response["responseStatus"] == "success"):
		# good data load,
		return jsonify({"responseStatus":"success", "data":response["data"]})
	else:
		return jsonify({"responseStatus":"error"})








