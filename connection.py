import boto3
import csv
import time
import json
import pymongo as pymongo
import mysql as mysql
import mysql.connector
from boto3.dynamodb.conditions import Key


def DynamoDbConnection(filename):
    dynamodb = boto3.resource('dynamodb',
                              region_name='us-west-2',
                              aws_access_key_id='AKIAZEWZ54WMTTNOFUWR',
                              aws_secret_access_key='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')
    table = dynamodb.Table('customers')
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        timedict = {}
        # insert time
        st = time.time()
        with table.batch_writer() as writer:
            for row in reader:
                mdict = {'name': row[0], 'address': row[1]}
                writer.put_item(Item=mdict)
        et = time.time()
        insert_time = et - st
        timedict['insert time'] = insert_time

        # query time
        qst = time.time()
        table.query(KeyConditionExpression=Key('name').eq('CHAO'))
        qet = time.time()
        query_time = qet - qst
        timedict['query time'] = query_time

        # update time
        ust = time.time()
        table.update_item(Key={'name': 'CHAO', 'address': 'YANG'},
                          UpdateExpression='set tax = :tax',
                          ExpressionAttributeValues={
                              ':tax':'1',
                          },
                          ReturnValues='UPDATED_NEW')
        uet = time.time()
        update_time = uet - ust
        timedict['update time'] = update_time

        # delete time
        dst = time.time()
        table.delete_item(Key={'name': 'ZHE', 'address': 'YANG'})
        det = time.time()
        delete_time = det - dst
        timedict['delete time'] = delete_time

        return timedict


def MongoDbConnection(filename):
    client = pymongo.MongoClient(
        "mongodb+srv://cyang72:gniyzcpsr1yNK87U@csp554project.ufifwyv.mongodb.net/?retryWrites=true&w=majority")
    db = client['mydatabase']
    mycol = db['customers']
    timedict = {}
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        mydict = []
        for row in reader:
            mdict = {'name': row[0], 'address': row[1]}
            mydict.append(mdict)
        # insert time
        st = time.time()
        mycol.insert_many(mydict)
        et = time.time()
        stime = et - st
        timedict['insert time'] = stime

        # query time
        qt = time.time()
        mydoc = mycol.find({"address": { "$regex": "^S" } })
        print(mydoc)
        qet = time.time()
        query_time = qet - qt
        timedict['query time'] = query_time

        # update time
        ust = time.time()
        mycol.update_one({'address': 'YANG'}, { "$set": { "address": "Canyon 123" } })
        uet = time.time()
        timedict['update time'] = uet-ust

        # delete time
        dst = time.time()
        mycol.delete_one({'name': 'CHAO'})
        det = time.time()
        etime = det - dst
        timedict['delete time'] = etime

    return timedict


def MySQLConnection(filename):
    mydb = mysql.connector.connect(
        host="localhost",
        user='root',
        password='ccyyUSA2018',
        database='mydatabase'
    )
    mycursor = mydb.cursor()
    timedict = {}
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        insert_query = 'insert into customers (name, address) values (%s, %s)'
        delete_query = "delete from customers where name = 'CHAO'"
        array = []
        for row in reader:
            array.append((row[0], row[1]))
        # insert time
        st = time.time()
        mycursor.executemany(insert_query, array)
        mydb.commit()
        et = time.time()
        insertTime = et - st
        timedict['insert time'] = insertTime

        # query time
        qst = time.time()
        mycursor.execute(delete_query)
        qet = time.time()
        timedict['query time'] = qet - qst

        # update time
        ust = time.time()
        mycursor.execute(
            "UPDATE customers SET address = 'YANG' WHERE address = 'ZHE' "
        )
        uet = time.time()
        timedict['update time'] = uet - ust

        # delete time
        dst = time.time()
        mycursor.execute(delete_query)
        mydb.commit()
        det = time.time()
        deleteTime = det - dst
        timedict['delete time'] = deleteTime
    return timedict


if __name__ == '__main__':
    print(DynamoDbConnection('160000_rows.csv'))
    print(MongoDbConnection('100_rows.csv'))
    print(MySQLConnection('160000_rows.csv'))
