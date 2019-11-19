import boto3 
from readCredentials import credentials

"""
Get Tables 14-27 from the original AWS account, and then create those tables on the new AWS account 
"""

original_key_id = credentials["oldAWS"]["ACCESS_KEY_ID"]
original_secret_key = credentials["oldAWS"]["SECRET_ACCESS_KEY"]
original_region = credentials["oldAWS"]["REGION"]

originalDbClient = boto3.client('dynamodb',
    aws_access_key_id = original_key_id,
    aws_secret_access_key = original_secret_key,
    region_name = original_region
)

originalDbResource = boto3.resource('dynamodb',
    aws_access_key_id = original_key_id,
    aws_secret_access_key = original_secret_key,
    region_name = original_region
)

response = originalDbClient.list_tables(ExclusiveStartTableName="refrating-umps-lookup")


tableNames = response["TableNames"]

newDb = boto3.resource(
    'dynamodb',
    aws_access_key_id=credentials["newAWS"]["ACCESS_KEY_ID"],
    aws_secret_access_key=credentials["newAWS"]["SECRET_ACCESS_KEY"],
    region_name=credentials["newAWS"]["REGION"]
)

for tableName in range(1, len(tableNames)):
    table = originalDbResource.Table(tableNames[tableName])
    newDb.create_table(
        TableName=table.name, 
        KeySchema=table.key_schema, 
        AttributeDefinitions=table.attribute_definitions, 
        ProvisionedThroughput={
            "ReadCapacityUnits": table.provisioned_throughput["ReadCapacityUnits"],
            "WriteCapacityUnits": table.provisioned_throughput["WriteCapacityUnits"]
        }
        )
    print("Creating table:", table.name)