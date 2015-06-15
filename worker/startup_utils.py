'''
Created on Feb 18, 2015

@author: arunjacob
'''
from worker_data import WorkerDataDB 
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
    
    
    workerDataDB = WorkerDataDB(urlHost,dbname,user,password)
    
    return workerDataDB



