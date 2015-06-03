#!/usr/bin/env python

import os
import bottle
import logging
import urlparse
import uuid
from fib_data import  FibDataRequest
from messages import MessageQueue
from bottle import route, get
from startup_utils import initializeDB
from threading import Thread
from date_formatting_utils import nowInSeconds, prettyPrintTime
from response_handler import ResponseHandler

STATIC_ROOT = os.path.join(os.path.dirname(__file__), 'static')

responseHandler = None
fibDataDB = None
messageQueue = None

'''
message queue processing thread logic
'''

def getMessages(messageQueue, fibDataDB, workerId):
    
        ''' 
        the processMessage method encapsulates bookkeeping in the database and keeps it separate from
        message queue logic. Not sure if we can do this in other languages...
        '''
    
        def processMessage(message):
            dataMap = {}
            dataMap['worker_id'] = workerId
            dataMap['fib_id'] = message.messageKey
            dataMap['fib_value'] = -1
            dataMap['started_date'] =  nowInSeconds()
            
            request = FibDataRequest(body=dataMap)
            log.debug("worker %s starting Fibonnaci on %d at %s"%(workerId,request.fibId,prettyPrintTime(request.startedDate)) )
            addedRequest = fibDataDB.addRequest(request)
            fibValue = F(message.messageKey)
            addedRequest.fibValue = fibValue
            addedRequest.finishedDate = nowInSeconds()
            fibDataDB.updateRequest(addedRequest)
            log.debug("worker %s finished Fibonnaci on %d, calculated value = %d,  at %s"%(workerId,request.fibId,request.fibValue,prettyPrintTime(request.startedDate)) )
    
        messageQueue.getAndProcessMessages(queueName,processMessage)

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


fibDataDB = initializeDB(mysql_url)

log.debug("setting up message queue")

rabbitUrl = os.environ['RABBITMQ_URL']
queueName = os.environ['QUEUE_NAME']
restInterval  = int(os.getenv('REST_INTERVAL',5))
log.debug("rabbit mq url:%s"%os.environ['RABBITMQ_URL'])

messageQueue = MessageQueue(rabbitUrl)

messageQueue.createQueue(queueName)

log.debug("generating worker ID")

workerId = uuid.uuid1()

# need to receive messages async so that the process can also handle web requests.

t = Thread(name='daemon', target=getMessages, args = (messageQueue,fibDataDB,workerId))
t.setDaemon(True)
t.start()

responseHandler = ResponseHandler(workerId,fibDataDB)

log.debug("starting web server")
application = bottle.app()
application.catchall = False


# NOTE that these will default to rational values if not set for local run.

assignedHost = os.getenv('VCAP_APP_HOST','127.0.0.1')
assignedPort = os.getenv('VCAP_APP_PORT',8081)

log.debug('launching application at %s:%s'%(assignedHost,assignedPort))
bottle.run(application, host=assignedHost, port=assignedPort)


# this is the last line
