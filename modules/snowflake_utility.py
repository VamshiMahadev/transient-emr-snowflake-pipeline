import json
import sys
import boto3
import snowflake.connector

#To find the AWS Account
region = "eu-west-1"
sts = boto3.client("sts")
account_id = sts.get_caller_identity()["Account"]

s3 = boto3.client('s3',region_name=region)
s3_resource = boto3.resource('s3',region_name=region)
s3_client = boto3.client('s3',region_name=region)
session = boto3.session.Session(region_name=region)
client = session.client(service_name="secretsmanager",region_name=region)

print(account_id)

def find_s3_bucket():
    if account_id == str(12345):
        return 'dev-bucket'
    elif account_id == str(1234):
        return 'prod-bucket'
    else:
        return 'xxx'

def snowflakesecretid():
    if account_id == str(12345):
        return "snow-key"
    elif account_id == str(12345):
        return "snow-prod-key"
    else:
        return "xyz"

def get_secret(secret_key):
    response = client.get_secret_value(SecretId=secret_key)
    secret_json_string = response['SecretString']
    return json.loads(secret_json_string) 

def db_snowflake():
    secret_key = snowflakesecretid()
    dbcredentials = get_secret(secret_key)
    sfOptions_metadata = {
        "sfURL": dbcredentials['sfURL'],
        "sfDatabase": dbcredentials['sfDatabase'],
        "sfSchema": dbcredentials['sfSchema'],
        "sfRole": dbcredentials['sfRole'],
        "sfUser": dbcredentials['sfsid'],
        "sfWarehouse" : dbcredentials["sfWarehouse"],
        "sfPassword": dbcredentials['sfpwd'],
        "truncate_table" : "ON",
        "usestagingtable" : "OFF"
    }
    return sfOptions_metadata

def fetch(query):
    try:
        secret_key = snowflakesecretid()
        dbcredentials = get_secret(secret_key)
        connection = snowflake.connector.connect(
            user = dbcredentials['sfsid'],
            password = dbcredentials['sfpwd'],
            account = dbcredentials['sfAccount'],
            database = dbcredentials['sfDatabase'],
            schema =  dbcredentials['sfSchema'],
            warehouse = dbcredentials['sfWarehouse'],
            role = dbcredentials['sfRole']
        )
        cursor = connection.cursor()
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        cursor.execute(query)
        return cursor.fetchall()

    except (Exception) as ex:
        print("Error while connecting to Snowflake", ex)
        raise ex
