'''
Created on Nov 14, 2014

@author: arunjacob
'''
import unittest
import urlparse
import os
import MySQLdb
from fib_data import FibDataRequest, FibDataDB, DisplayData,DataEncoder
from test_web_dbutils import initializeFibDataDB
from date_formatting_utils import nowInSeconds
import sys
import datetime
import json

class Test(unittest.TestCase):

    def setUp(self):
        
        try:
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
            print ("error creating table fibdata")
            print(e)
                    
        
    def test1InitializeFibData(self):
        
       
        try:
            fibDataDB = initializeFibDataDB(self.testName)
            fibData = FibDataRequest(None,{"request_id":"foo1","fib_id":3, "fib_value":2})
            newFD = fibDataDB.addRequest(fibData)
        
            self.assertTrue(newFD.requestId == 'foo1')
            self.assertTrue(newFD.fibId == fibData.fibId)
            self.assertTrue(newFD.fibValue == fibData.fibValue)
            self.assertTrue(newFD.startedDate == fibData.startedDate)
        except:
            e = sys.exc_info()[0]
            print e
            self.fail(e)
        
    
    def test2AddRequest(self):
        
        try:
            fibDataDB = initializeFibDataDB(self.testName)
            startedDate =  nowInSeconds()
            
            fd = FibDataRequest(None, {"request_id":"foo1","fib_id":3,"fib_value":3,"started_date": startedDate})
            
            newFd = fibDataDB.addRequest(fd)
            
            fetchedFd = fibDataDB.getRequest(newFd.requestId)
            
            self.assertTrue(fd.fibId == fetchedFd.fibId)
            self.assertTrue(fd.fibValue == fetchedFd.fibValue)
            self.assertTrue(fd.startedDate == fetchedFd.startedDate)
        except:
            e = sys.exc_info()[0]
            print e
            self.fail(e)
            
    
    def test3GetAllRequests(self):
        try:
            fibDataDB = initializeFibDataDB(self.testName)
            startedDate = nowInSeconds()
            fd = FibDataRequest(None, {"request_id":"foo2","fib_id":3,"fib_value":3,"started_date": startedDate})
            
            newFd = fibDataDB.addRequest(fd)
            fdArr = fibDataDB.getRequests(isPending = True)
            
            self.assertTrue(len(fdArr) > 0)
            
            self.assertTrue(len(fdArr) == 1)
            testFd = fdArr[0]
            self.assertTrue(testFd.fibId == newFd.fibId)
            
            fdArr2 = fibDataDB.getRequests(isPending = True,isDescending=False)
            self.assertTrue(len(fdArr2) == 1)
            testFd2 = fdArr2[0]
            self.assertTrue(testFd2.fibId == newFd.fibId)
            
            fdArr3 = fibDataDB.getRequests()
            self.assertTrue(len(fdArr3) == 0)
        except:
            e = sys.exc_info()[0]
            print e
            self.fail(e)
        
    def test4DropAllFibData(self):
        try:
            fibDataDB = initializeFibDataDB(self.testName)
            fibDataDB.dropAllRequests()
        except:
            e = sys.exc_info()[0]
            print e
            self.fail(e)
        
    def test5InitializeFromJSON(self):
        try:
            dataMap = {}
            dataMap['request_id'] ="foo1"
            dataMap['fib_id'] = 3
            dataMap['fib_value'] = 3
            dataMap['started_date'] =  nowInSeconds()
            request = FibDataRequest(body = dataMap)
            
            self.assertTrue(request != None)
            self.assertTrue(request.fibId == dataMap['fib_id'])
            self.assertTrue(request.fibValue == dataMap['fib_value'])
            
        except:
            e = sys.exc_info()[0]
            print e
            self.fail(e)
            
            
    def test6AddRequestWithFinishedDate(self):
        try:
            fibDataDB = initializeFibDataDB(self.testName)
            fibData = FibDataRequest(None,{"request_id":"foo1","fib_id":3, "fib_value":2, "started_date" :nowInSeconds(), "finished_date" : nowInSeconds()})
            newFD = fibDataDB.addRequest(fibData)
        
            self.assertTrue(newFD.requestId != -1)
            self.assertTrue(newFD.fibId == fibData.fibId)
            self.assertTrue(newFD.fibValue == fibData.fibValue)
            self.assertTrue(newFD.startedDate == fibData.startedDate)
            self.assertTrue(newFD.finishedDate == fibData.finishedDate)
        except:
            e = sys.exc_info()[0]
            print e
            self.fail(e)
        
    def test7Add_UpdateRequest(self):
        try:
        
            fibDataDB = initializeFibDataDB(self.testName)
            fibData = FibDataRequest(None,{"request_id":"foo1","fib_id":3, "fib_value":2, "started_date" :nowInSeconds(), "finished_date" : nowInSeconds()})
            newFD = fibDataDB.addRequest(fibData)
            
            finishedDate = nowInSeconds()+5
            fibData.finishedDate = finishedDate
            
            fibDataDB.updateRequest(fibData)
            
        except:
            e = sys.exc_info()[0]
            print e
            self.fail(e)
            
    def test12Add_UpdateRequest_FetchAllCompleteTasks(self):
        
        fibDataDB = initializeFibDataDB(self.testName)
        finishedDate = nowInSeconds()
        startedDate = finishedDate - 5;
        fd = FibDataRequest(None, {"request_id":"foo1","fib_id":3,"fib_value":3,"started_date": startedDate,"finished_date":finishedDate})
        
        fibDataDB.addRequest(fd)
        fdArr = fibDataDB.getRequests(isPending = False)
        
        self.assertTrue(len(fdArr) == 1)
        
        startedDate = nowInSeconds()
        
        fd = FibDataRequest(None, {"request_id":"foo2","fib_id":3,"fib_value":3,"started_date": startedDate})
                
        fibDataDB.addRequest(fd)
        fdArr = fibDataDB.getRequests(isPending = False)
        
        self.assertTrue(len(fdArr) == 1)
        
    def test13SerializeDisplayData(self):
        try:
            fibDataDB = initializeFibDataDB(self.testName)
            startedDate = nowInSeconds()
            fd = FibDataRequest(None, {"request_id":"foo1","fib_id":3,"fib_value":3,"started_date": startedDate})
            fd2 = FibDataRequest(None,{"request_id":"foo2","fib_id":4,"fib_value":5,"started_date": startedDate})
            fibDataDB.addRequest(fd)
            fibDataDB.addRequest(fd2)
            
            fdArr = fibDataDB.getRequests(isPending = True)
            
            self.assertTrue(len(fdArr) == 2)
            
            displayDataArr = []
            
            
            for request in fdArr:
                displayDataArr.append(DisplayData(request))
                
            str = json.dumps(displayDataArr,cls=DataEncoder)
            
            print str
            
        except:
            e = sys.exc_info()[0]
            print e
            self.fail(e) 
        
            
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