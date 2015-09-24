'''
Created on May 14, 2015

@author: arunjacob
'''
import unittest
import datetime
import MySQLdb
import urlparse
from worker_data import WorkerData, WorkerDataDB
from date_formatting_utils import nowInSeconds
from test_worker_dbutils import initializeWorkerDataDB

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
            workerData = WorkerDataDB(url, self.testName,userName,password)
            
            workerData.createTable()
            db.commit()
            
            db.close()
        except MySQLdb.Error, e:
            self.log.error("error creating table workerdata")
            self.log.error(e)
            self.handleMySQLException(e,True)
            return None       

    def test1AddWorkerDataAtStart(self):
        
        workerDataDB = initializeWorkerDataDB(self.testName)
        workerData = WorkerData(None, {"request_id":"foo1","worker_id":"abcd","fib_id":3})
        
        newWorkerData = workerDataDB.addWorkerData(workerData)
        
        assert newWorkerData.workerId > 0
        
        
    
    def test2GetWorkerData(self):
        
        workerDataDB = initializeWorkerDataDB(self.testName)
        workerData = WorkerData(None, {"request_id":"foo1","worker_id":"abcd","fib_id":3})
        
        newWorkerData = workerDataDB.addWorkerData(workerData)
        
        checkWorkerData = workerDataDB.getWorkerData(workerId = newWorkerData.workerId)
        
        self.assertTrue(checkWorkerData.requestId == workerData.requestId)
        self.assertTrue(checkWorkerData.workerId == workerData.workerId)
        self.assertTrue(checkWorkerData.fibId == workerData.fibId)
        
        checkWorkerData = workerDataDB.getWorkerData(requestId = newWorkerData.requestId)
        
        self.assertTrue(checkWorkerData.requestId == workerData.requestId)
        self.assertTrue(checkWorkerData.workerId == workerData.workerId)
        self.assertTrue(checkWorkerData.fibId == workerData.fibId)
        

    def testUpdateWorkerData(self):
        
        workerDataDB = initializeWorkerDataDB(self.testName)
        lastCheckinDate = nowInSeconds()
        lastCheckinDate = lastCheckinDate-5
        workerData = WorkerData(None, {"request_id":"foo1","worker_id":"abcd","fib_id":3,"started_date":nowInSeconds()-5})
        
        newWorkerData = workerDataDB.addWorkerData(workerData)
        
        workerData.fibValue=3
        workerDataDB.updateWorkerData(workerData)
        
        checkWorkerData = workerDataDB.getWorkerData(workerData.workerId)
        
        self.assertTrue(checkWorkerData.finishedDate != None)
    
    def testGetPendingWorkItems(self):
        
        workerDataDB = initializeWorkerDataDB(self.testName)
        workerData = WorkerData(None, {"request_id":"foo1","worker_id":"abcd","fib_id":3})
        workerDataDB.addWorkerData(workerData)
    
        workerData = WorkerData(None, {"request_id":"foo2","worker_id":"efgh","fib_id":5})
        workerDataDB.addWorkerData(workerData)
        
        timestamp = nowInSeconds()
        workerDatas = workerDataDB.getWorkItems(isPending = True)
        
        for workerData in workerDatas:
            self.assertTrue(workerData.fibValue == -1)
            self.assertTrue(workerData.finishedDate == None)

        workerDatas = workerDataDB.getWorkItems(isPending = False)
        
        self.assertTrue(len(workerDatas) == 0)
    
    def testGetCompletedWorkItems(self):
        
        workerDataDB = initializeWorkerDataDB(self.testName)
        startDate = nowInSeconds()- 5;
        
        workerData = WorkerData(None, {"request_id":"foo1","worker_id":"abcd","fib_id":3,"startedDate":startDate})
        workerDataDB.addWorkerData(workerData)
        workerData.fibValue = 3
        workerDataDB.updateWorkerData(workerData)
        workerData = WorkerData(None, {"request_id":"foo2","worker_id":"efgh","fib_id":5,"startedDate":startDate})
        
        workerDataDB.addWorkerData(workerData)
        workerData.fibValue = 8
        workerDataDB.updateWorkerData(workerData)
        
        
        workerDatas = workerDataDB.getWorkItems(isPending = True)
        
        self.assertTrue(len(workerDatas) == 0)
        
        
        workerDatas = workerDataDB.getWorkItems(isPending = False)
        for workerData in workerDatas:
            self.assertTrue(workerData.fibValue != -1)
            self.assertTrue(workerData.finishedDate != None)

        workerDatas = workerDataDB.getWorkItems(isPending = True)
        
        self.assertTrue(len(workerDatas) == 0)

    def testUpdateRetryCount(self):
        workerDataDB = initializeWorkerDataDB(self.testName)
        workerData = WorkerData(None, {"request_id":"foo1","worker_id":"abcd","fib_id":3})
        workerDataDB.addWorkerData(workerData)
        
        newRetryCount = workerData.retryCount  = workerData.retryCount+1
         
        workerDataDB.updateWorkerData(workerData) 
        
        updatedWorkerData = workerDataDB.getWorkerData(workerId = "abcd")
        self.assertTrue(updatedWorkerData.retryCount == newRetryCount)

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
            self.log.error("error removing table %s"%self.testName)
            self.handleMySQLException(e,True)
            return None


    


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()