'''
Created on May 13, 2015

@author: arunjacob
'''
import MySQLdb

import json
import logging
from  date_formatting_utils import nowInSeconds, prettyPrintTime

logging.basicConfig()

class DataEncoder(json.JSONEncoder):
        
    def default(self,o):
        return o.__dict__
    
workerDataLogger = logging.getLogger('workerdata')
workerDataLogger.setLevel(logging.DEBUG)

class WorkerDataEncoder(json.JSONEncoder):
        
    def default(self,o):
        return o.__dict__
    

class WorkerData(object):
    '''
    a row in the DisplayData table, indicating worker ID, status, and last checkin time
    '''


    def __init__(self, row = None, body = None):
        # SELECT id,request_id, worker_id,fib_id,fib_value, started_date,finished_date
        if row != None:
            workerDataLogger.debug("initializing from database")
            self.id = row[0]
            self.requestId = row[1]
            self.workerId = row[2]
            self.fibId = row[3]
            self.fibValue = row[4]
            self.retryCount = row[5]
            self.startedDate = row[6]
            
            if len(row) == 8:
                self.finishedDate = row[7]
            else:
                self.finishedDate = None
            
        elif body != None:
            workerDataLogger.debug("initializing from JSON")
            self.id = -1
            
            if body.has_key('request_id') == True:
                self.requestId = body['request_id']
            else:
                workerDataLogger.error("invalid JSON format, request_id not found")
                raise 'invalid format'
            
            
            if body.has_key('worker_id') == True:
                self.workerId = body['worker_id']
            else:
                workerDataLogger.error("invalid JSON format, worker_id not found")
                raise 'invalid format'
            
            if body.has_key('fib_id') == True:
                self.fibId = body['fib_id']
            else:
                workerDataLogger.error("invalid JSON format, fib_id not found")
                raise 'invalid format'
            
            if body.has_key('fib_value'):
                self.fibValue = body['fib_value']
            else:
                self.fibValue = -1
            
            if body.has_key("retry_count"):
                self.finishedDate = body['retry_count']
            else:
                self.retryCount = 0
            
            
            if body.has_key("started_date"):
                self.startedDate = body['started_date']
            else:
                self.startedDate = nowInSeconds()
            
                
            if body.has_key("finished_date"):
                self.finishedDate = body['finished_date']
            else:
                self.finishedDate = None
                
  
class WorkerDisplayData(object):
    def __init__(self, workerData):
        self.workerData = workerData
        self.formattedStartDate = prettyPrintTime(workerData.startedDate)
        if workerData.finishedDate != None:
            self.formattedFinishDate = prettyPrintTime(workerData.finishedDate)
        else:
            self.runTime = nowInSeconds() - workerData.startedDate
          
class WorkerDataDB(object):
    
    def __init__(self,url,dbName,userName,password):
        
        self.log =  logging.getLogger('workerDataDB')
        self.log.setLevel(logging.DEBUG)
        self.url= url
        self.dbName = dbName
        self.userName = userName
        self.password = password
        
            
            
    def connectToDB(self):
        try:
            self.log.debug("connecting database")
            db = MySQLdb.connect(host=self.url,user=self.userName,passwd=self.password,db=self.dbName) 
            cur = db.cursor()
            cur.execute('use %s'%self.dbName)
            return db
        except MySQLdb.Error, e:
            self.log.error("unable to connect to database")
            self.handleMySQLException(e,True)
            return None
    
    
    def createTable(self):
        
        try:
            db = self.connectToDB()
            workerDataTableCreate = 'CREATE TABLE IF NOT EXISTS workerdata( id int not null auto_increment, request_id int, worker_id char(100) not null, fib_id int not null, fib_value int DEFAULT -1, retry_count int DEFAULT 0, started_date int not null, finished_date int, PRIMARY KEY(id));'
            
            cur = db.cursor()
    
            self.log.debug('executing workerdata table create')
            
            cur.execute(workerDataTableCreate)
            db.commit()
            self.log.debug('workerdata table created')
            
            try:
                workerIndexDrop = 'drop index worker_idx on workerdata;'
                cur.execute(workerIndexDrop)
                db.commit()
            except:
                self.log.debug('index doesnt exist')
                
            workerIndexCreate = 'CREATE INDEX  worker_idx ON workerdata(worker_id);'
            cur.execute(workerIndexCreate)
            db.commit()
            
            self.disconnectFromDB(db)
        
        except MySQLdb.Error, e:
            self.log.error("error creating table workerdata")
            self.handleMySQLException(e,True)
            return None
        
        
    def dropTable(self): 
        try:
            db = self.connectToDB()
            workerDataTableDrop = 'DROP TABLE workerdata'
            
            cur = db.cursor()
    
            self.log.debug('executing workerdata table drop')
            
            cur.execute(workerDataTableDrop)
            db.commit()
            self.debug('workerdata table removed')
            
            self.disconnectFromDB(db)
        
        except MySQLdb.Error, e:
            self.log.error("error removing table workerdata")
            self.handleMySQLException(e,True)
            return None
               
    def disconnectFromDB(self,db):
        try: 
            db.close()
            
        except MySQLdb.Error, e:
            self.log.error("unable to disconnect from database")
            self.handleMySQLException(e,True)
            
            
            
    def handleMySQLException(self,e,throwEx=False):
        """
        parses sql exceptions into readable format
        """
        try:
            self.log.error( "Error [%d]: %s"%(e.args[0],e.args[1]))
        except IndexError:
            self.log.error( "Error: %s"%str(e))
            
        raise e
    
    def addWorkerData(self,workerData):
        """
        inserts a workerData into the database and timestamps it for readability
        
        """
        try:
            db = self.connectToDB()
            cur = db.cursor()
            
            
            self.log.debug("adding worker entry into database with request_id %d, worker_id = '%s', fib_id=%d, startedDate = %s"%(workerData.requestId,workerData.workerId,workerData.fibId,nowInSeconds()))
            query = "insert into workerdata(request_id, worker_id, fib_id,started_date) values(%d,'%s',%d,%d)"%(workerData.requestId,workerData.workerId,workerData.fibId,nowInSeconds())
            
            cur.execute(query)
            db.commit()
            
            # get generated ID - THIS IS VALID PER CONNECTION
            
            query = "SELECT LAST_INSERT_ID()";
            cur.execute(query)
            
            row = cur.fetchone()
            id = row[0]
            workerData.id = id
            
            self.disconnectFromDB(db)
            
            return workerData
            
        except MySQLdb.Error as e:
            self.log.error(str(e))
            self.handleMySQLException(e)
       
    def  getWorkerData(self,workerId = None, requestId = None):
        """
        returns a workerData with the specified workerId or None
        """
        try:
            
            db = self.connectToDB()
            if workerId != None:
                query = "select id,request_id, worker_id,fib_id,fib_value,retry_count,started_date,finished_date from workerdata where worker_id = '%s'"%workerId
            elif requestId != None:
                query = "select id,request_id, worker_id,fib_id,fib_value,retry_count,started_date,finished_date from workerdata where request_id = '%s'"%requestId
            
            cur = db.cursor()
            cur.execute(query)
            row = cur.fetchone()
            
            workerData = None
            
            if row != None:
                workerData = WorkerData(row)
         
            return workerData
        
        except MySQLdb.Error as e:
            self.log.error(str(e))
            self.handleMySQLException(e)
            
           
    def updateWorkerData(self,workerData):
        """
        updates a specified workerData, setting workerStatus and lastCheckinDate values ONLY.
        (those are the things that can change)
        """
        # explicit fail if the record hasn't been added
        
        if workerData.id == -1:
            raise 'cannot update a request that has not been added already'
        
        try:
            db = self.connectToDB()
            cur = db.cursor()
            
            query = None
            if(workerData.fibValue > -1):
                self.log.debug("update workerdata set fib_value = %d, finished_date = %d, retry_count = %d where id=%d"%(workerData.fibValue,nowInSeconds(), workerData.retryCount, workerData.id))
                query = "update workerdata set fib_value = %d, finished_date = %d, retry_count = %d  where id=%d"%(workerData.fibValue,nowInSeconds(), workerData.retryCount,workerData.id)
            else:
                self.log.debug("update workerdata set  retry_count = %d where id=%d"%( workerData.retryCount, workerData.id))
                query = "update workerdata set retry_count = %d  where id=%d"%(workerData.retryCount,workerData.id)
           
            cur.execute(query)
            db.commit()
            self.disconnectFromDB(db)
                
                
        except MySQLdb.Error as e:
            self.log.error(str(e))
            self.handleMySQLException(e)
    
    
    def getWorkItems(self, isPending = False, isDescending=True,limit = 100):
        """
        returns all workerData with ordering and limit set as specified
        worker = worker ID to filter by
        withinSeconds = filter out all worker data older than now - withinSeconds
        isDescending = True by default, gives most recent timestamps first
        limit = take what is needed to display.
        """
        requests = []
        self.log.debug("retrieving workerData, limit = %d"%limit)
        try:
            
            db = self.connectToDB()
            
            if isPending == True:
                if isDescending == True:
                    query = 'SELECT id,request_id, worker_id,fib_id,fib_value, retry_count, started_date,finished_date FROM workerdata WHERE finished_date IS NULL  ORDER BY id DESC LIMIT %d'%limit
                else:
                    query = 'SELECT id,request_id, worker_id,fib_id,fib_value, retry_count, started_date,finished_date FROM workerdata WHERE finished_date IS NULL  ORDER BY id LIMIT %d'%limit
            else:
                if isDescending == True:
                    query = 'SELECT id,request_id, worker_id,fib_id,fib_value, retry_count, started_date,finished_date FROM workerdata WHERE finished_date IS NOT NULL ORDER BY id DESC LIMIT %d'%limit
                else:
                    query = 'SELECT id,request_id, worker_id,fib_id,fib_value, retry_count, started_date,finished_date FROM workerdata WHERE finished_date IS NOT NULL ORDER BY id LIMIT %d'%limit
            
            cur = db.cursor()
            cur.execute(query)
            rows = cur.fetchall()
            
            for row in rows:
                requests.append(WorkerData(row))
                
            
        except MySQLdb.Error, e:
            self.handleMySQLException(e)
    
        self.disconnectFromDB(db)
        self.log.debug("returning %d request"%len(requests))
        return requests
    

    def dropAllWorkerData(self):
        """
        for testing: truncate the db
        """
        self.log.debug("dropping all workerdata")
        try:
            db = self.connectToDB()
            query = "TRUNCATE TABLE workerdata"
            cur = db.cursor()
            cur.execute(query)
            db.commit()
            
        except MySQLdb.Error, e:
            self.log.error(str(e))
            self.handleMySQLException(e)
        
        self.disconnectFromDB(db)
