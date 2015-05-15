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
from test_dbutils import initializeWorkerDataDB

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

    def test1AddWorkerStatus(self):
        
        workerDataDB = initializeWorkerDataDB(self.testName)
        workerData = WorkerData(None, {"worker_id":"abcd","worker_status":"working", "last_checkin_date":nowInSeconds()})
        
        newWorkerData = workerDataDB.addWorkerData(workerData)
        
        assert newWorkerData.workerId > 0
        
        
    
    def test2GetWorkerData(self):
        
        workerDataDB = initializeWorkerDataDB(self.testName)
        workerData = WorkerData(None, {"worker_id":"abcd","worker_status":"working", "last_checkin_date":nowInSeconds()})
        
        newWorkerData = workerDataDB.addWorkerData(workerData)
        
        checkWorkerData = workerDataDB.getWorkerData(newWorkerData.workerId)
        
        self.assertTrue(checkWorkerData.workerId == workerData.workerId)
        self.assertTrue(checkWorkerData.lastCheckinDate == workerData.lastCheckinDate)

    def testUpdateWorkerData(self):
        
        workerDataDB = initializeWorkerDataDB(self.testName)
        lastCheckinDate = nowInSeconds()
        lastCheckinDate = lastCheckinDate-5
        workerData = WorkerData(None, {"worker_id":"abcd","worker_status":"working", "last_checkin_date":lastCheckinDate})
        
        newWorkerData = workerDataDB.addWorkerData(workerData)
        
        workerData.lastCheckinDate = nowInSeconds()
        workerDataDB.updateWorkerData(workerData)
        
        checkWorkerData = workerDataDB.getWorkerData(workerData.workerId)
        
        self.assertTrue(lastCheckinDate < checkWorkerData.lastCheckinDate)
    
    def testGetWorkerDatas(self):
        
        workerDataDB = initializeWorkerDataDB(self.testName)
        workerData = WorkerData(None, {"worker_id":"abcd","worker_status":"working", "last_checkin_date":nowInSeconds()-5})
        workerDataDB.addWorkerData(workerData)
    
        workerData = WorkerData(None, {"worker_id":"efgh","worker_status":"working", "last_checkin_date":nowInSeconds()-4})
        workerDataDB.addWorkerData(workerData)
        
        timestamp = nowInSeconds()
        workerDatas = workerDataDB.getWorkerDatas(None, 5)
        
        for workerData in workerDatas:
            self.assertTrue(timestamp - workerData.lastCheckinDate < 5)
    
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