import boto3
from tables import *
from readCredentials import credentials

newDb = boto3.resource(
    'dynamodb',
    aws_access_key_id=credentials["newAWS"]["ACCESS_KEY_ID"],
    aws_secret_access_key=credentials["newAWS"]["SECRET_ACCESS_KEY"],
    region_name=credentials["newAWS"]["REGION"]
)

for tableToImport in range(2, len(tables)):
    tableName = tables[tableToImport].name
    newDb.create_table(
        TableName=tables[tableToImport].name, 
        KeySchema=tables[tableToImport].key_schema, 
        AttributeDefinitions=tables[tableToImport].attribute_definitions, 
        ProvisionedThroughput={
            "ReadCapacityUnits": tables[tableToImport].provisioned_throughput["ReadCapacityUnits"],
            "WriteCapacityUnits": tables[tableToImport].provisioned_throughput["WriteCapacityUnits"]
        }
        )
    print("Creating table: ", tableName)