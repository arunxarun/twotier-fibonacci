#!/usr/bin/env python

import os
import bottle
import logging
import urlparse
import uuid
from messages import MessageQueue, WorkResultMessage, WorkRequestMessage
from bottle import route, get
from startup_utils import initializeDB
from threading import Thread
from date_formatting_utils import nowInSeconds, prettyPrintTime
from response_handler import ResponseHandler
from worker_data import WorkerData, WorkerDataDB
import json
import sys

STATIC_ROOT = os.path.join(os.path.dirname(__file__), 'static')

responseHandler = None
workerDataDB = None
jobMessageQueue = None
resultsMessageQueue = None

'''
message queue processing thread logic
'''

def doWork(jobMessageQueue, resultsMessageQueue, workerDataDB, workerId):
    
        
        def processMessage(message):
            '''
            this is the handler function passed to getAndProcessMessages
            '''
            dataMap = {}
            dataMap['worker_id'] = workerId
            dataMap['request_id'] = message.requestId
            dataMap['fib_id'] = message.messageKey
            dataMap['fib_value'] = -1
            dataMap['started_date'] =  message.startedDate
            
            workerData = WorkerData(body=dataMap)
            log.debug("worker %s starting Fibonnaci on %d at %s"%(workerId,workerData.fibId,prettyPrintTime(nowInSeconds())) )
            
            # BUGBUG: need to check that this already hasn't been attempted, and 
            # then remove it from the database. 
            
            addedWorkerData = workerDataDB.getWorkerData(requestId = workerData.requestId)
            
            if addedWorkerData != None:
                workerData.retryCount = workerData.retryCount + 1
                workerDataDB.updateWorkerData(workerData)
            else:
                addedWorkerData = workerDataDB.addWorkerData(workerData)
                
            log.debug("starting fib(%d)"%workerData.fibId)
            fibValue = F(message.messageKey)
            addedWorkerData.fibValue = fibValue
            log.debug("completed fib(%d), value = %d"%(workerData.fibId,fibValue))
            addedWorkerData.finishedDate = nowInSeconds()
            workerDataDB.updateWorkerData(addedWorkerData)
            
            log.debug("worker %s finished Fibonnaci on %d, calculated value = %d,  at %s"%(workerId,addedWorkerData.fibId,addedWorkerData.fibValue,prettyPrintTime(addedWorkerData.finishedDate)) )
            
            # now send result back for result processing.
            resultsMessageQueue.sendMessage(WorkResultMessage(requestId = addedWorkerData.requestId, messageKey = addedWorkerData.fibId, messageValue = addedWorkerData.fibValue,startedDate = addedWorkerData.startedDate,finishedDate = addedWorkerData.finishedDate))
            
            
        def extractWorkRequestMessage(messageBody):
            """
            for handling request messages from job request queue
            """
            messageContents = json.loads(messageBody)
            try:
                
                message = WorkRequestMessage(body=messageContents)
                return message
            except:
                log.error(str(sys.exc_info()[0]))
                    
    
        jobMessageQueue.getAndProcessMessages(processMessage,extractWorkRequestMessage)

@route('/')
@route("/index.html")
def home():
    return bottle.static_file('index.html', root=os.path.dirname(__file__))
    
@get('/inprocess') 
def getInProcess():
    try: 
        return responseHandler.getInProcess()
    except:
        return "internal error",500
    


@get('/complete')
def getComplete():
    try:
        return responseHandler.getComplete()
    except:
        return "internal error",500
    
    
@get('/instance')
def getInstance():
    try:
        return responseHandler.getInstance()
    except:
        return "internal error",500
    

'''
Fibonacci sequence, recursive
'''

def F(n):
    #log.debug("F(%d)"%n)
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

logging.basicConfig()
log = logging.getLogger('worker')
log.setLevel(logging.DEBUG)


log.debug("setting up db connection...")

try:
    mysql_url = urlparse.urlparse(os.environ['MYSQL_URL'])
except KeyError:
    log.warn("env variable MYSQL_URL not found, reverting to DATABASE_URL")
    mysql_url = urlparse.urlparse(os.environ['DATABASE_URL'])


workerDataDB = initializeDB(mysql_url)

log.debug("setting up message queue")

rabbitUrl = os.environ['RABBITMQ_URL']
jobQueueName = os.environ['JOBS_QUEUE_NAME']
resultsQueueName = os.environ['RESULTS_QUEUE_NAME']
restInterval  = int(os.getenv('REST_INTERVAL',5))
log.debug("rabbit mq url:%s"%os.environ['RABBITMQ_URL'])

jobMessageQueue = MessageQueue(rabbitUrl)
jobMessageQueue.createQueue(jobQueueName)

resultsMessageQueue = MessageQueue(rabbitUrl)
resultsMessageQueue.createQueue(resultsQueueName)

# worker ID is unique to this instance of worker
log.debug("generating worker ID")
workerId = uuid.uuid1()

# need to receive work requests async in order to also handle web requests

workerT = Thread(name='daemon', target=doWork, args = (jobMessageQueue,resultsMessageQueue,workerDataDB,workerId))
workerT.setDaemon(True)
workerT.start()


responseHandler = ResponseHandler(workerId,workerDataDB)

log.debug("starting web server")
application = bottle.app()
application.catchall = False


# NOTE that these will default to rational values if not set for local run.

assignedHost = os.getenv('VCAP_APP_HOST','127.0.0.1')
assignedPort = os.getenv('VCAP_APP_PORT',8081)

log.debug('launching application at %s:%s'%(assignedHost,assignedPort))
bottle.run(application, host=assignedHost, port=assignedPort)


# this is the last line
