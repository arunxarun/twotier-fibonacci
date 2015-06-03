#!/usr/bin/env python

import os
import bottle
import logging
import urlparse
from fib_data import  DataEncoder, FormattedRequest

from bottle import route, get, post, request, template
from messages import Message
from startup_utils import initializeDB
import json
from messages import MessageQueue

STATIC_ROOT = os.path.join(os.path.dirname(__file__), 'static')
PAGE_ROOT = os.path.dirname(__file__)
logging.basicConfig()
log = logging.getLogger('receiver')
log.setLevel(logging.DEBUG)

FibDataDB = None
messageQueue = None



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
    
    messageQueue.sendMessage(queueName, Message(value = int(number)))
    

'''
Adding this route for use with StormRunner (to automate load, compute utilization)
'''

@route('/fib/<number:int>') 
def fib_num(number):
    if not number:
        return template('Please add a number to the end of url: /fib/5')
    
    messageQueue.sendMessage(queueName, Message(value = int(number)))

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
queueName = os.environ['QUEUE_NAME']

log.debug("rabbit mq url:%s"%os.environ['RABBITMQ_URL'])

messageQueue = MessageQueue(rabbitUrl)
messageQueue.createQueue(queueName)


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
