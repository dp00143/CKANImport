from itertools import chain
from time import sleep

__author__ = 'Daniel'
import urllib2
import urllib
import json
from pprint import pprint
from httplib2 import Http
from unicodedata import normalize
from datetime import datetime
import pika

#specify hostname by name or ip adress
def establishConnection(hostname='127.0.0.1'):
    connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
    return connection

lastTimeStamp = False
datetimeFormat = '%Y-%m-%dT%H:%M:%S'

#msgType store, forward or transform
def wrapAndSendInitialData(inp, msgTypes, connection, channelName):
    global lastTimeStamp
    channel = connection.channel()
    channel.exchange_declare(exchange='stream', type="direct")
    for arr in inp:
        data = {}
        data["type"] = msgTypes
        data["data"] = arr
        data["metadata"] = getMetaData(arr["REPORT_ID"])
        jsonData = json.dumps(data)
        channel.basic_publish(exchange='stream', routing_key=channelName, body=jsonData)
    lastTimeStamp = datetime.strptime(data["data"]["TIMESTAMP"], datetimeFormat)


# ,"limit":2
def importData(url, resource):
    data_string = urllib.quote(json.dumps({'resource_id': resource}))
    response = urllib2.urlopen(url,data_string)
    assert response.code == 200

    response_dict = json.loads(response.read())
    assert response_dict['success'] is True
    result = response_dict['result']
    # pprint(result)
    return result["records"]

def wrapAndSendData(inp, msgTypes, connection, channelName):
    global lastTimeStamp
    channel = connection.channel()
    channel.exchange_declare(exchange='stream', type="direct")
    timestamp = str(datetime.now())
    for arr in inp:
        data = {}
        data["type"] = msgTypes
        data["data"] = arr
        data["metadata"] = getMetaData(arr["REPORT_ID"])
        jsonData = json.dumps(data)
        #only publish if data is new
        currentTimeStamp = datetime.strptime(data["data"]["TIMESTAMP"], datetimeFormat)
        sentData = currentTimeStamp > lastTimeStamp
        if sentData:
            channel.basic_publish(exchange='stream', routing_key=channelName, body=jsonData)
    lastTimeStamp = datetime.strptime(data["data"]["TIMESTAMP"], datetimeFormat)
    return sentData

def importAllData(channelname, updaterate):
    connection = establishConnection()
    url = "http://www.gatesense.com/ckan/api/action/datastore_search"
    resourceValues = "3c2200b6-3d8e-4707-9105-a755680f921a"
    while True:
        try:
            values = importData(url, resourceValues)
            break
        except urllib2.HTTPError:
            print "Could not retrieve data, try again"
            sleep(updaterate)
    types = ['transform', 'store', 'forward']
    print "publish initial data of size %i" % len(values)
    wrapAndSendInitialData(values, types, connection, channelname)
    #give the CKAN archive time to update data
    sleep(updaterate)
    while True:
        try:
            values = importData(url, resourceValues)
        except urllib2.HTTPError:
            continue
        if wrapAndSendData(values,types,connection,channelname):
            time = datetime.now()
            print str(time)+":published new data"
        #give the CKAN archive time to update data
        sleep(updaterate)
    connection.close()
    return

def getMetaData(reportid):
    resource_id = "fdad4220-4d01-4541-8959-35cc539042a7"
    url = "http://www.gatesense.com/ckan/api/action/datastore_search?resource_id=fdad4220-4d01-4541-8959-35cc539042a7&filters={\"REPORT_ID\":%i}" % reportid
    # pprint(url)
    response = urllib2.urlopen(url)
    assert response.code == 200
    response_dict = json.loads(response.read())
    # assert response_dict[u'result'] is True
    result =response_dict[u'result']
    dict = result[u"records"][0]
    meta_data = {}
    for k, v in dict.iteritems():
        meta_data.update({k:v})
    return meta_data

importAllData('trafficdata',300)