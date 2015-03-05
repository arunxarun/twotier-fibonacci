#!/usr/bin/env python

import os
import bottle
import logging
import urlparse
import uuid
from fib_data import  FibDataDB, DataEncoder, FibDataRequest,WorkerData
from messages import Message,MessageQueue
from bottle import route, get, post, request, template
from startup_utils import initializeDB
from threading import Thread
import json
from date_formatting_utils import nowInSeconds, prettyPrintTime
from time import sleep
STATIC_ROOT = os.path.join(os.path.dirname(__file__), 'static')

logging.basicConfig()
log = logging.getLogger('receiver')
log.setLevel(logging.DEBUG)



log.debug("setting up db connection...")

try:
    mysql_url = urlparse.urlparse(os.environ['MYSQL_URL'])
except KeyError:
    log.warn("env variable MYSQL_URL not found, reverting to DATABASE_URL")
    mysql_url = urlparse.urlparse(os.environ['DATABASE_URL'])


fibDataDB = initializeDB(mysql_url)

log.debug("setting up message queue")

rabbit_url = os.environ['RABBITMQ_URL']
queue_name = os.environ['QUEUE_NAME']
rest_interval  = int(os.getenv('REST_INTERVAL',5))
log.debug("rabbit mq url:%s"%os.environ['RABBITMQ_URL'])

messageQueue = MessageQueue(rabbit_url)

messageQueue.createQueue(queue_name)

log.debug("generating worker ID")

workerId = uuid.uuid1()

'''
message queue processing thread logic
'''

def getMessages(messageQueue, fibDataDB, workerId):
    keepGoing = True
    
    while(keepGoing == True):
        nextMessageArray = messageQueue.getMessages(queue_name,1)
        if len(nextMessageArray) != 0:
            
            dataMap = {}
            dataMap['worker_id'] = workerId
            dataMap['fib_id'] = nextMessageArray[0].messageKey
            dataMap['fib_value'] = -1
            dataMap['started_date'] =  nowInSeconds()
            
            request = FibDataRequest(body=dataMap)
            log.debug("worker %s starting Fibonnaci on %d at %s"%(workerId,request.fibId,prettyPrintTime(request.startedDate)) )
            addedRequest = fibDataDB.addRequest(request)
            fibValue = F(nextMessageArray[0].messageKey)
            addedRequest.fibValue = fibValue
            addedRequest.finishedDate = nowInSeconds()
            fibDataDB.updateRequest(addedRequest)
            log.debug("worker %s finished Fibonnaci on %d, calculated value = %d,  at %s"%(workerId,request.fibId,request.fibValue,prettyPrintTime(request.startedDate)) )
        else:
            log.debug('about to sleep %d seconds'%rest_interval)
            sleep(rest_interval)
    
# need to receive messages async so that the process can also handle web requests.

t = Thread(name='daemon', target=getMessages, args = (messageQueue,fibDataDB,workerId,))
t.setDaemon(True)
t.start()

'''
view routes
'''

@route('/')
def home():
    bottle.TEMPLATE_PATH.insert(0, './views')
    return bottle.template('home')
    
@get('/inprocess') 
def getInProcess():
    
    log.debug("handling /inprocess path")
    
    allRequestData = fibDataDB.getRequests(isPending=True)
    
    workerInfo = []
    for request in allRequestData:
        workerInfo.append(WorkerData(request))
        
        
    return json.dumps(workerInfo,cls=DataEncoder)

@get('/instance')
def getInstance():
    log.debug('generating internal id')
    uid = uuid.uuid1()
    
    return "{'instance-id':'%s'}"%uid

'''
Fibonacci sequence, recursive
'''

def F(n):
    if n == 0: return 0
    elif n == 1: return 1
    else: return F(n-1)+F(n-2)



'''
required to serve static resources
'''
@route('/static/:filename')
def serve_static(filename):
    log.debug("serving static assets")
    return bottle.static_file(filename, root=STATIC_ROOT)

'''
service runner code
'''
log.debug("starting web server")
application = bottle.app()
application.catchall = False


# NOTE that these will default to rational values if not set for local run.

assignedHost = os.getenv('VCAP_APP_HOST','127.0.0.1')
assignedPort = os.getenv('VCAP_APP_PORT',8081)

log.debug('launching application at %s:%s'%(assignedHost,assignedPort))
bottle.run(application, host=assignedHost, port=assignedPort)


# this is the last line
