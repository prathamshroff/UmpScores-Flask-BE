import boto3
from readCredentials import credentials

# Get data from original AWS

# CODE FROM: https://stackoverflow.com/questions/36780856/complete-scan-of-dynamodb-with-boto3

original_key_id = credentials["oldAWS"]["ACCESS_KEY_ID"]
original_secret_key = credentials["oldAWS"]["SECRET_ACCESS_KEY"]
original_region = credentials["oldAWS"]["REGION"]

dynamodb = boto3.resource('dynamodb',
    aws_access_key_id = original_key_id,
    aws_secret_access_key = original_secret_key,
    region_name = original_region
)

tables = {}

tables[0] = dynamodb.Table('hotfix-table')
tables[1] = dynamodb.Table('refrating-career-range')
tables[2] = dynamodb.Table('refrating-careers')
tables[3] = dynamodb.Table('refrating-careers-season')
tables[4] = dynamodb.Table('refrating-crews')
tables[5] = dynamodb.Table('refrating-game-stats-v1')
tables[6] = dynamodb.Table('refrating-games-lookup')
tables[7] = dynamodb.Table('refrating-pitcher-stats')
tables[8] = dynamodb.Table('refrating-pitcher-zone')
tables[9] = dynamodb.Table('refrating-profile-month')
tables[10] = dynamodb.Table('refrating-team-stats-v1')
tables[11] = dynamodb.Table('refrating-ump-game-lookup')
tables[12] = dynamodb.Table('refrating-umpire-pitchers')
tables[13] = dynamodb.Table('refrating-umps-lookup')

