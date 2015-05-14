'''
Created on May 13, 2015

@author: arunjacob
'''
import unittest
import uuid
import datetime
import urlparse
import MySQLdb
from fib_data import FibDataDB, FibDataRequest, WorkerData
from response_handler import ResponseHandler
import json 

class Test(unittest.TestCase):

    def initializeFibDataDB(self,dbName):
        
        MYSQL_URL = "mysql://dev:devpass@localhost/%s"%dbName
        
        mysql_url = urlparse.urlparse(MYSQL_URL)
        
        url = mysql_url.hostname
        password = mysql_url.password
        user = mysql_url.username
        dbname = mysql_url.path[1:] 
        
        fibDB = FibDataDB(url,dbname,user,password)
        
        return fibDB

    def setUp(self):
        
        try:
            workerId = uuid.uuid1()
            # create a test database.
            self.testName = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
            MYSQL_URL = "mysql://dev:devpass@localhost/test"
            
            mysqlUrl = urlparse.urlparse(MYSQL_URL)
            url = mysqlUrl.hostname
            password = mysqlUrl.password
            userName = mysqlUrl.username
            dbName = mysqlUrl.path[1:] # slice off the '/'
            
            db = MySQLdb.connect(host=url,user=userName,passwd=password,db=dbName) 
            cur = db.cursor()
            cur.execute('create database %s'%self.testName)
            db.commit()
            
            #cur.execute("GRANT ALL ON %s.* TO 'dev'@'localhost';"%self.testName)
            #db.commit()
            fibDataDB = FibDataDB(url, self.testName,userName,password)
            
            fibDataDB.createTable()
            db.commit()
            
            db.close()
            
            
        except MySQLdb.Error, e:
            self.log.error("error creating table fibdata")
            self.log.error(e)
            self.handleMySQLException(e,True)
            return None        

    def testInProcess(self):
        
        try:
            fibDataDB = self.initializeFibDataDB(self.testName)
            fibData = FibDataRequest(None,{"worker_id":"abcd","fib_id":3, "fib_value":2})
            testFD = fibDataDB.addRequest(fibData)
            fibDataDB.updateRequest(testFD)
            
            fibData = FibDataRequest(None,{"worker_id":"efgh","fib_id":3, "fib_value":2})
            testFD = fibDataDB.addRequest(fibData)
            fibDataDB.updateRequest(testFD)
            
            
            fibData = FibDataRequest(None,{"worker_id":"efgh","fib_id":4, "fib_value":3})
            testFD = fibDataDB.addRequest(fibData)
            
        
            responseHandler = ResponseHandler(uuid.uuid1(),fibDataDB)
            
            response  = responseHandler.getInProcess()
            
            self.assertTrue(response != None)
            
            parsedResponses = json.loads(response)
            
            self.assertTrue(len(parsedResponses) == 1)
        
            parsedResponse =  parsedResponses[0]
            
            self.assertTrue(parsedResponse.has_key('formattedFinishDate') == False)
            
            # [this['fibData']['fibId'],this['fibData']['fibValue'],this['fibdata']['workerId'],this['formattedStartDate'],this['formattedFinishDate']]).draw();
            
            self.assertTrue(parsedResponse['fibData']['fibId'] == 4)
            self.assertTrue(parsedResponse['fibData']['fibValue'] == 3)
            self.assertTrue(parsedResponse['fibData']['workerId'] == 'efgh')
            
        except MySQLdb.Error, e:
            self.log.error("error creating table fibdata")
            self.log.error(e)
            self.handleMySQLException(e,True)
            return None
            
        


    def testComplete(self):
        
        try:
            fibDataDB = self.initializeFibDataDB(self.testName)
            fibData = FibDataRequest(None,{"worker_id":"abcd","fib_id":3, "fib_value":2})
            testFD = fibDataDB.addRequest(fibData)
            fibDataDB.updateRequest(testFD)
            
            fibData = FibDataRequest(None,{"worker_id":"efgh","fib_id":3, "fib_value":2})
            testFD = fibDataDB.addRequest(fibData)
            fibDataDB.updateRequest(testFD)
            
            
            fibData = FibDataRequest(None,{"worker_id":"efgh","fib_id":4, "fib_value":3})
            testFD = fibDataDB.addRequest(fibData)
            
        
            responseHandler = ResponseHandler(uuid.uuid1(),fibDataDB)
            
            response  = responseHandler.getComplete()
            
            self.assertTrue(response != None)
            
            parsedResponses = json.loads(response)
            
            self.assertTrue(len(parsedResponses) == 2)
        
            parsedResponse =  parsedResponses[0]
            
            self.assertTrue(parsedResponse.has_key('formattedFinishDate') == True)
            
            self.assertTrue(parsedResponse['fibData']['fibId'] == 3)
            self.assertTrue(parsedResponse['fibData']['fibValue'] == 2)
            self.assertTrue(parsedResponse['fibData']['workerId'] == 'abcd')
            
            parsedResponse =  parsedResponses[1]
            
            self.assertTrue(parsedResponse.has_key('formattedFinishDate') == True)
            self.assertTrue(parsedResponse['fibData']['fibId'] == 3)
            self.assertTrue(parsedResponse['fibData']['fibValue'] == 2)
            self.assertTrue(parsedResponse['fibData']['workerId'] == 'efgh')
            
        except MySQLdb.Error, e:
            self.log.error("error creating table fibdata")
            self.log.error(e)
            self.handleMySQLException(e,True)
            return None
        pass
    
   

    def tearDown(self):
        try:
            MYSQL_URL = "mysql://dev:devpass@localhost/test"
            
            mysqlUrl = urlparse.urlparse(MYSQL_URL)
            url = mysqlUrl.hostname
            password = mysqlUrl.password
            userName = mysqlUrl.username
            dbName = mysqlUrl.path[1:] # slice off the '/'
            
            db = MySQLdb.connect(host=url,user=userName,passwd=password,db=dbName) 
            cur = db.cursor()
            cur.execute('drop database %s'%self.testName)
            db.commit()
            db.close()
        except MySQLdb.Error, e:
            self.log.error("error removing table fibdata")
            self.handleMySQLException(e,True)
            return None
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()