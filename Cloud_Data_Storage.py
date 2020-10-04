#!/usr/bin/env python3

import boto3
import csv

s3 = boto3.resource('s3', aws_access_key_id='AKIA3JGJJOJCJSW4KK5X', aws_secret_access_key='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')

try:
    s3.create_bucket(Bucket='datacont-hw3', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except:
    print("this may already exist")
bucket = s3.Bucket("datacont-hw3")
bucket.Acl().put(ACL='public-read')
body = open('/home/matt/exp1', 'rb')
o = s3.Object('datacont-hw3', 'test').put(Body=body)
s3.Object('datacont-hw3', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
    region_name='us-west-2',
    aws_access_key_id='AKIA3JGJJOJCJSW4KK5X',
    aws_secret_access_key='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'
)
try:
    table = dyndb.create_table(
    TableName='DataTable',
    KeySchema=[{
    'AttributeName': 'PartitionKey',
    'KeyType': 'HASH'
    },
    {
    'AttributeName': 'RowKey',
    'KeyType': 'RANGE'
    }
    ],
    AttributeDefinitions=[
    {
    'AttributeName': 'PartitionKey',
    'AttributeType': 'S'
    },
    {
    'AttributeName': 'RowKey',
    'AttributeType': 'S'
    },
    ],
    ProvisionedThroughput={
    'ReadCapacityUnits': 5,
    'WriteCapacityUnits': 5
    }
    )
except:
#if there is an exception, the table may already exist.
    table = dyndb.Table("DataTable")
with open('/home/matt/experiments.csv', 'rt') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        body = open('/home/matt/'+item[3], 'rb')
        s3.Object('datacont-hw3', item[3]).put(Body=body )
        md = s3.Object('datacont-hw3', item[3]).Acl().put(ACL='public-read')
        url = "https://s3-us-west-2.amazonaws.com/datacont-hw3/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description' : item[4], 'date' : item[2], 'url':url}
try:
    table.put_item(Item=metadata_item)
except:
    print("item may already be there or another failure")
response = table.get_item(
    Key={
        'PartitionKey': 'experiment3',
        'RowKey': '4'
    }
)
item = response['Item']
print(item)
response
