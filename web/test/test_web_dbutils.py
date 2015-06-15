'''
Created on Nov 14, 2014

@author: arunjacob
'''

import urlparse
from fib_data import  FibDataDB

def initializeFibDataDB(dbName):
        
    MYSQL_URL = "mysql://dev:devpass@localhost/%s"%dbName
    
    mysql_url = urlparse.urlparse(MYSQL_URL)
    
    
    #rdb = redis.Redis(host=url.hostname, port=url.port, password=url.password)
    
    url = mysql_url.hostname
    password = mysql_url.password
    user = mysql_url.username
    dbname = mysql_url.path[1:] 
    
    fibDB = FibDataDB(url,dbname,user,password)
    
    return fibDB


    

