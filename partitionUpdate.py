#!/usr/bin/python

#
# This program used to sync the glue table schema and partitions
#
import boto3
from sys import argv
import json
import time
from datetime import date, datetime

dbname = argv[1]
table_name = argv[2]

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()


client = boto3.client('glue', region_name='us-east-1')

response = client.get_table(
    DatabaseName=dbname,
    Name=table_name
)
print(response)
storagedes=response['Table']['StorageDescriptor']

#res=client.get_partitions(DatabaseName=dbname,TableName=tablename,MaxResults=1000)

paginator = client.get_paginator('get_partitions')

# Create a PageIterator from the Paginator
response_iterator = paginator.paginate(DatabaseName=dbname, TableName=table_name)



#print('Total partitions',len(paginator['Partitions']))

print('Partition sync started')
count = 0
for part in response_iterator:

    for epart in part['Partitions']:
        epart.pop('DatabaseName', 'None')
        epart.pop('CreationTime', 'None')
        epart.pop('TableName', 'None')
        epart['StorageDescriptor']['Columns']=storagedes['Columns']
        count += 1
        print("{} partitions updated".format(count))
        #print(json.dumps(epart['Values']))
        #print(json.dumps(epart,default=json_serial))
        response = client.update_partition(DatabaseName=dbname, TableName=table_name,
                                           PartitionValueList=epart['Values'], PartitionInput=epart)
    print('Next page')
    time.sleep(5)

print('Partition sync Completed')

