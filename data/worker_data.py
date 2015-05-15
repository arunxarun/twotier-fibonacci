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

class WorkerData(object):
    '''
    a row in the DisplayData table, indicating worker ID, status, and last checkin time
    '''


    def __init__(self, row = None, body = None):

        if row != None:
            workerDataLogger.debug("initializing from database")
            self.id = row[0]
            self.workerId = row[1]
            self.workerStatus = row[2]
            self.lastCheckinDate = row[3]
            
        elif body != None:
            workerDataLogger.debug("initializing from JSON")
            self.id = -1
            
            if body.has_key('worker_id') == True:
                self.workerId = body['worker_id']
            else:
                workerDataLogger.error("invalid JSON format, worker_id not found")
                raise 'invalid format'
            
            if body.has_key('worker_status') == True:
                self.workerStatus = body['worker_status']
            else:
                workerDataLogger.error("invalid JSON format, worker_status not found")
                raise 'invalid format'
            
            if body.has_key('last_checkin_date'):
                self.lastCheckinDate = body['last_checkin_date']
            else:
                self.lastCheckinDate = None
    
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
            workerDataTableCreate = 'CREATE TABLE IF NOT EXISTS workerdata( id int not null auto_increment, worker_id char(100) not null, worker_status char(100) not null, last_checkin_date int not null,PRIMARY KEY(id));'
            
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
            
            
            self.log.debug("adding worker entry into database with worker_id = '%s', worker_status = %s, and last_checkin_date = %d"%(workerData.workerId,workerData.workerStatus,workerData.lastCheckinDate))
            query = "insert into workerdata(worker_id, worker_status,last_checkin_date) values('%s','%s', %d)"%(workerData.workerId,workerData.workerStatus,workerData.lastCheckinDate)
            
            cur.execute(query)
            db.commit()
            
            # get generated ID
            
            query = "select max(id) from workerdata where worker_id = '%s'"%(workerData.workerId)
            cur.execute(query)
            
            row = cur.fetchone()
            id = row[0]
            workerData.id = id
            
            self.disconnectFromDB(db)
            
            return workerData
            
        except MySQLdb.Error as e:
            self.log.error(str(e))
            self.handleMySQLException(e)
       
    def  getWorkerData(self,workerId):
        """
        returns a workerData with the specified workerId or None
        """
        try:
            
            db = self.connectToDB()
            query = "select id,worker_id,worker_status,last_checkin_date from workerdata where worker_id = '%s'"%workerId
            
            cur = db.cursor()
            cur.execute(query)
            row = cur.fetchone()
            
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
            
            self.log.debug("update workerData set worker_id = '%s', worker_status='%s',last_checkin_date=%d where id = %d"%(workerData.workerId,workerData.workerStatus,workerData.lastCheckinDate, workerData.id))
            query = "update workerData set worker_id = '%s', worker_status='%s',last_checkin_date=%d where id = %d"%(workerData.workerId,workerData.workerStatus,workerData.lastCheckinDate, workerData.id)
            cur.execute(query)
            db.commit()
            self.disconnectFromDB(db)
                
                
        except MySQLdb.Error as e:
            self.log.error(str(e))
            self.handleMySQLException(e)
    
    
    def getWorkerDatas(self,workerId = None, withinSeconds = -1, isDescending=True,limit = 100):
        """
        returns all workerData with ordering and limit set as specified
        worker = worker ID to filter by
        withinSeconds = filter out all worker data older than now - withinSeconds
        isDescending = True by default, gives most recent timestamps first
        limit = take what is needed to display.
        """
        timeBoundary  = nowInSeconds() - withinSeconds
        requests = []
        self.log.debug("retrieving workerData, limit = %d"%limit)
        try:
            
            db = self.connectToDB()
            
            if isDescending == True:
                if withinSeconds == -1:
                    query = 'select id,worker_id,worker_status, last_checkin_date from workerdata WHERE ORDER BY id DESC LIMIT %d'%limit
                else: 
                    query = "select id,worker_id,worker_status, last_checkin_date from workerdata where last_checkin_date > %d ORDER BY id DESC LIMIT %d"%(timeBoundary,limit)
            else:
                if withinSeconds == -1:
                    query = 'select id,worker_id,worker_status, last_checkin_date from workerdata WHERE ORDER BY id LIMIT %d'%limit
                else:
                    query = "select id,worker_id,worker_status, last_checkin_date from workerdata where last_checkin_date > %d ORDER BY id LIMIT %d"%(timeBoundary,limit)
            
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
    

    def dropAllRequests(self):
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
