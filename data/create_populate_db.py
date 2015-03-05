'''
Created on Sep 4, 2014

@author: jacoba
'''
import MySQLdb
import urlparse
import os
import logging
from fib_data import FibDataDB

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
        password = mysqlUrl.password
        userName = mysqlUrl.username
        dbName = mysqlUrl.path[1:] # slice off the '/'
        
        fibdataDB = FibDataDB(url,dbName,userName,password)
        
        fibdataDB.createTable()
        
              
        

    except MySQLdb.Error, e:
        print "Exception during database initialization: %s"%str(e)
