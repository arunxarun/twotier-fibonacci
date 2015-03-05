'''
Created on Feb 18, 2015

@author: arunjacob
'''
from fib_data import FibDataDB
from messages import MessageQueue
import urlparse
import logging

logging.basicConfig()
log = logging.getLogger('startup_utils')
log.setLevel(logging.DEBUG)

def initializeDB(url):
    log.debug("setting up db connection...")
    
    urlHost = url.hostname
    password = url.password
    user = url.username
    dbname = url.path[1:] 
    
    
    fibDataDB = FibDataDB(urlHost,dbname,user,password)
    
    return fibDataDB



