#!/usr/bin/env python

import os
import bottle
import logging
import urlparse
import uuid
from fib_data import  FibDataDB,FibDataRequest, DataEncoder, FormattedRequest
from bottle import route, get, post, request, template
from messages import WorkRequestMessage, WorkResultMessage
import sys

from startup_utils import initializeDB
import json
from messages import MessageQueue
from threading import Thread

STATIC_ROOT = os.path.join(os.path.dirname(__file__), 'static')
PAGE_ROOT = os.path.dirname(__file__)
logging.basicConfig()
log = logging.getLogger('receiver')
log.setLevel(logging.DEBUG)

FibDataDB = None
jobMessageQueue = None


'''
message queue processing thread logic
'''

def doWork(resultsMessageQueue, fibDataDB):
    
        
        def processMessage(message):
            '''
            this is the handler function passed to getAndProcessMessages
            '''
            dataMap = {}
            dataMap['request_id'] = message.requestId
            dataMap['fib_id'] = message.messageKey
            dataMap['fib_value'] = message.messageValue
            dataMap['started_date'] =  message.startedDate
            dataMap['finished_date'] = message.finishedDate
            
            fibDataRequest = FibDataRequest(body=dataMap)
            fibDataDB.updateRequest(fibDataRequest)
            
        def extractWorkResultMessage(messageBody):
            """
            for handling WorkResultMessages from Result queue
            """
            messageContents = json.loads(messageBody)
            try:
                
                message = WorkResultMessage(body=messageContents)
                return message
            except:
                log.error(str(sys.exc_info()[0]))
    
        resultsMessageQueue.getAndProcessMessages(processMessage,extractWorkResultMessage)

'''
view routes
'''

@route('/')
def home():
#     bottle.TEMPLATE_PATH.insert(0, './views')
#     return bottle.template('home')
    return bottle.static_file('index.html', root=PAGE_ROOT)

    
@get('/received') 
def getReceived():
    
    log.debug("handling /received path")
    
    allRequestData = fibDataDB.getRequests()
    
    allFormattedRequests = []
    for request in allRequestData:
        allFormattedRequests.append(FormattedRequest(request))
        
    return json.dumps(allFormattedRequests,cls=DataEncoder)


'''
Fibonacci sequence, recursive
'''

def F(n):
    if n == 0: return 0
    elif n == 1: return 1
    else: return F(n-1)+F(n-2)


@post('/fib') 
def fib():
    number = request.json['number']
    if not number:
        return template('Please add a number to the end of url: /send/5')
    fibDataPayload = {}
    fibDataPayload['fib_id'] = number;
    requestId = str(uuid.uuid1())
    fibDataPayload['request_id'] = requestId
    fibDataRequest = FibDataRequest(body=fibDataPayload)
    newRequest = fibDataDB.addRequest(fibDataRequest)
    jobMessageQueue.sendMessage(WorkRequestMessage(requestId = newRequest.requestId, messageKey = int(number)))
    

'''
Adding this route for use with StormRunner (to automate load, compute utilization)
'''

@route('/fib/<number:int>') 
def fib_num(number):
    if not number:
        return template('Please add a number to the end of url: /fib/5')
    fibDataPayload = {}
    fibDataPayload['fib_id'] = number;
    fibDataRequest = FibDataRequest(body=fibDataPayload)
    newRequest = fibDataDB.addRequest(fibDataRequest)
    jobMessageQueue.sendMessage(jobsQueueName, WorkRequestMessage(requestId = newRequest.requestId, messageKey = int(number)))

'''
required to serve static resources
'''
@route('/static/:filename')
def serve_static(filename):
    log.debug("serving static assets")
    return bottle.static_file(filename, root=STATIC_ROOT)


log.debug("setting up db connection...")

try:
    mysql_url = urlparse.urlparse(os.environ['MYSQL_URL'])
except KeyError:
    log.warn("env variable MYSQL_URL not found, reverting to DATABASE_URL")
    mysql_url = urlparse.urlparse(os.environ['DATABASE_URL'])

fibDataDB = initializeDB(mysql_url)

log.debug("setting up message queue")
rabbitUrl = os.environ['RABBITMQ_URL']
jobsQueueName = os.environ['JOBS_QUEUE_NAME']
resultsQueueName = os.environ['RESULTS_QUEUE_NAME']
log.debug("rabbit mq url:%s"%os.environ['RABBITMQ_URL'])

# jobsMessageQueue is what web layer sends work requests to
jobMessageQueue = MessageQueue(rabbitUrl)
jobMessageQueue.createQueue(jobsQueueName)

# resultsMessageQueue is what worker layer sends results back to
resultsMessageQueue = MessageQueue(rabbitUrl)
resultsMessageQueue.createQueue(resultsQueueName)


# need to receive work result requests async in order to also handle web requests

workerT = Thread(name='daemon', target=doWork, args = (resultsMessageQueue,fibDataDB))
workerT.setDaemon(True)
workerT.start()

'''
service runner code
'''
log.debug("starting web server")
application = bottle.app()
application.catchall = False


# NOTE that these will default to rational values if not set for local run.

assignedHost = os.getenv('VCAP_APP_HOST','127.0.0.1')
assignedPort = os.getenv('VCAP_APP_PORT',8080)

log.debug('launching application at %s:%s'%(assignedHost,assignedPort))
bottle.run(application, host=assignedHost, port=assignedPort)


# this is the last line
