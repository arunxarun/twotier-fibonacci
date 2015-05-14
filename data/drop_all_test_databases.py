'''
Created on May 8, 2015

@author: arunjacob
'''

import urlparse
import MySQLdb
import re
import logging

if __name__ == '__main__':

    logging.basicConfig()
    log = logging.getLogger('drop_test_databases')
    log.setLevel(logging.DEBUG)
    try:
        databases = []
        exp = re.compile('\d\d\d\d_\d\d_\d\d.*')
        
        MYSQL_URL = "mysql://dev:devpass@localhost/test"
        
        mysqlUrl = urlparse.urlparse(MYSQL_URL)
        url = mysqlUrl.hostname
        password = mysqlUrl.password
        userName = mysqlUrl.username
        dbName = mysqlUrl.path[1:] # slice off the '/'
        
        db = MySQLdb.connect(host=url,user=userName,passwd=password,db=dbName) 
        cur = db.cursor()
        cur.execute('show databases')
        row = cur.fetchone()
        while row != None:
            databases.append(row[0])
            row = cur.fetchone()
            
        for database in databases:
            if exp.match(database) != None:
                print 'dropping %s'%database
                cur.execute('drop database %s'%database)
                
        db.commit()
        db.close()
    except MySQLdb.Error, e:
        log.error("error removing table fibdata")
        