#!/usr/bin/env python3

import boto3
import csv

s3 = boto3.resource('s3', aws_access_key_id='AKIAIW2SEK5LCDON7KSQ',
                    aws_secret_access_key='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')
try:
    s3.create_bucket(Bucket='datacont-hw3',
                     CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except:
    print("this may already exist")

# Upload a file, 'testt.jpg' into the newly created bucket
s3.Object('datacont-hw3', 'test.jpg').put(Body=open('/home/mydata/test.jpg', 'rb'))

dyndb = boto3.resource('dynamodb', region_name='us-west-2')

# The first time that we define a table, we use
table = dyndb.create_table(
    TableName = 'DataTable',
    KeySchema = [
        { 'AttributeName': 'PartitionKey', 'KeyType': 'HASH' },
        { 'AttributeName': 'RowKey', 'AttributeType': 'S' }
    ]
)
# Wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

# If the table has been previously defined, use:
# table = dyndb.Table("DataTable")

urlbase = "https://s3-us-west-2.amazonaws.com/datacont-hw3/"
with open('\path-to-your-data\experiments.csv', 'rb') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        body = open('path-to-your-data\datafiles\\'+item[3], 'rb')
        md = s3.Object('datacont-hw3', item[3].Acl()).put(ACL='public-read')
        url=urlbase+item[3]
        metadat_item={'PartitionKey' : item[0], 'RowKey' : item[1],
                      'description' : item[4], 'date' : item[2], 'url' : url}
        table.put_item(Item=metadata_item)
