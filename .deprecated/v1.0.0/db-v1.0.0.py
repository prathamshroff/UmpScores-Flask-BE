from flask import jsonify
from sqlalchemy import *
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

db_uri = 'mysql+pymysql://bluumio_admin:6qS5VT_7KoJm@162.241.225.30/bluumio_template'
## change to ENV variables in the future
engine = create_engine(db_uri)

Base = declarative_base()
Base.metadata.reflect(engine)
db_session = scoped_session(sessionmaker(bind=engine))

class Users(Base):
	__table__ = Base.metadata.tables['users']

class Data(Base):
	__table__ = Base.metadata.tables['data']

def testing():
	# testing db setup
	for item in db_session.query(Users).filter_by(email='caa96@bu.edu'):
		print("email: ", item.email)
		print("password: ", item.password)
		print("type: ", item.user_type)
	return jsonify({"response":"success"})

def isUser(userEmail, userType):
	user_query = db_session.query(Users).filter_by(email=userEmail, user_type=userType)
	is_user = db_session.query(user_query.exists()).scalar()
	return jsonify({"user_exists":is_user})

def isGoogleUser(userEmail, userType):
	google_user_query = db_session.query(Users).filter_by(email=userEmail, user_type=userType)
	is_google_user = db_session.query(google_user_query.exists()).scalar()
	print("google user check status: ", is_google_user)
	return jsonify({"google_user_exists":is_google_user})

def getUser(userEmail):
	user_query = db_session.query(Users).filter_by(email=userEmail)
	db_hashed_pw = ""
	db_email = ""
	for item in user_query:
		db_hashed_pw = item.password
		db_email = item.email
	return jsonify({"response":"success", "db_email":db_email, "db_hashed_pw":db_hashed_pw})

def addUser(userEmail, passwordHash):
	newUser = Users(email=userEmail, password=passwordHash)
	db_session.add(newUser)
	if (db_session.commit()):
		return jsonify({"response":"success"})
	else:
		return jsonify({"response":"error"})

def addGoogleUser(userEmail, userType, firstName, lastName, googleId):
	newGoogleUser = Users(email=userEmail, user_type=userType, google_id=googleId, first_name=firstName, last_name=lastName)
	db_session.add(newGoogleUser)
	db_session.commit()
	user_query = db_session.query(Users).filter_by(email=userEmail)
	is_user = db_session.query(user_query.exists()).scalar()
	if (is_user):
		return jsonify({"response":"success"})
	else:
		return jsonify({"response":"error"})

def getLineGraphData(userEmail, dataset):
	data = []
	data_query = db_session.query(Data).filter_by(user=userEmail)
	for item in data_query:
		data_row = {"name":item.title, "uv":item.uv, "pv":item.pv, "amt":item.amt}
		data.append(data_row)
	print(data)
	return jsonify({"responseStatus":"success", "data":data})

