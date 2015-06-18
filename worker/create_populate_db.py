'''
Created on Sep 4, 2014

@author: jacoba
'''
import MySQLdb
import urlparse
import os
import logging
from worker_data import WorkerDataDB

if __name__ == '__main__':
    
    try:
        logging.basicConfig()
        log = logging.getLogger('fibonacci')
        log.setLevel(logging.DEBUG)

        try:
            mysqlUrl = urlparse.urlparse(os.environ['MYSQL_URL'])
        except KeyError:
            log.warn("env variable MYSQL_URL not found, reverting to DATABASE_URL")
            mysqlUrl = urlparse.urlparse(os.environ['DATABASE_URL'])
        
        
        url = mysqlUrl.hostname
        log.debug("url = %s"%url)
        password = mysqlUrl.password
        log.debug("password = %s"%password) 
        userName = mysqlUrl.username
        log.debug("username = %s"%userName)
        dbName = mysqlUrl.path[1:] # slice off the '/'
        log.debug("dbName = %s"%dbName) 
          
        
        workerDataDB = WorkerDataDB(url,dbName,userName,password)
        workerDataDB.createTable()    
        log.info("workerdata table created")

    except MySQLdb.Error, e:
        print "Exception during database initialization: %s"%str(e)
