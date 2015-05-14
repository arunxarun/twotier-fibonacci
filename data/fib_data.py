'''
Created on Nov 13, 2014

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
    
fibDataLogger = logging.getLogger('fibdata')
fibDataLogger.setLevel(logging.DEBUG)

class FibDataRequest(object):
    '''
    contains fibId, fibValue, created information.
    '''


    def __init__(self, row = None, body = None):
        


        if row != None:
            fibDataLogger.debug("initializing from database")
            
            self.requestId = row[0]
            self.workerId = row[1]
            self.fibId = row[2]
            self.fibValue = row[3]
            self.startedDate = row[4]
            self.finishedDate = row[5]
        elif body != None:
            fibDataLogger.debug("initializing from JSON")
            self.requestId = -1
            
            if body.has_key('worker_id') == True:
                self.workerId = body['worker_id']
            else:
                fibDataLogger.error("invalid JSON format, sequence_id not found")
                raise 'invalid format'
            
            if body.has_key('fib_id') == True:
                self.fibId = body['fib_id']
            else:
                fibDataLogger.error("invalid JSON format, sequence_id not found")
                raise 'invalid format'
            
            if body.has_key('fib_value') == True:
                self.fibValue = body['fib_value']
            else:
                fibDataLogger.error("invalid JSON format, sequence_value  not found")
                raise 'invalid format'    
            
            if body.has_key('started_date'):
                self.startedDate = body['started_date']
            else:
                self.startedDate = None
    
            if body.has_key('finished_date'):
                self.finishedDate = body['finished_date']
            else:
                self.finishedDate = None


    
class WorkerData(object):
    def __init__(self, fibData):
        self.fibData = fibData
        self.formattedStartDate = prettyPrintTime(fibData.startedDate)
        if fibData.finishedDate != None:
            self.formattedFinishDate = prettyPrintTime(fibData.finishedDate)
        self.runTime = nowInSeconds() - fibData.startedDate
        
class FormattedRequest(object):
    def __init__(self,fibData):
        self.fibData = fibData
        self.formattedStartDate = prettyPrintTime(fibData.startedDate)
        if fibData.finishedDate != None:
            self.formattedFinishDate = prettyPrintTime(int(fibData.finishedDate))
        else:
            self.formattedFinishDate = 'in progress'
        
class FibDataDB(object):
    
    def __init__(self,url,dbName,userName,password):
        
        self.log =  logging.getLogger('messageDB')
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
            fibdataTableCreate = 'CREATE TABLE IF NOT EXISTS fibdata( request_id int not null auto_increment, worker_id char(100) not null, fib_id int not null, fib_value bigint not null, started_date int not null, finished_date int,PRIMARY KEY(request_id));'
            
            cur = db.cursor()
    
            self.log.debug('executing fibdata table create')
            
            cur.execute(fibdataTableCreate)
            db.commit()
            self.log.debug('fibdata table created')
            
            try:
                workerIndexDrop = 'drop index worker_idx on fibdata;'
                cur.execute(workerIndexDrop)
                db.commit()
            except:
                self.log.debug('index doesnt exist')
                
            workerIndexCreate = 'CREATE INDEX  worker_idx ON fibdata(worker_id);'
            cur.execute(workerIndexCreate)
            db.commit()
            

            self.disconnectFromDB(db)
        
        except MySQLdb.Error, e:
            self.log.error("error creating table fibdata")
            self.handleMySQLException(e,True)
            return None
        
        
    def dropTable(self): 
        try:
            db = self.connectToDB()
            fibdataTableCreate = 'DROP TABLE fibdata'
            
            cur = db.cursor()
    
            self.log.debug('executing fibdata table drop')
            
            cur.execute(fibdataTableCreate)
            db.commit()
            self.debug('fibdata table removed')
            
            self.disconnectFromDB(db)
        
        except MySQLdb.Error, e:
            self.log.error("error removing table fibdata")
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
    
    def addRequest(self,request):
        """
        inserts a request into the database and timestamps it for readability
        
        """
        try:
            db = self.connectToDB()
            cur = db.cursor()
            if request.startedDate == None:
                request.startedDate = nowInSeconds()
            
            
            
            if request.finishedDate == None:
                self.log.debug("adding request into database with worker_id = '%s', fib_id = %d, fib_value = %d and started_date = %d"%(request.workerId,request.fibId,request.fibValue,request.startedDate))
                query = "insert into fibdata(worker_id, fib_id,fib_value,started_date) values('%s',%d, %d,%d)"%(request.workerId, request.fibId, request.fibValue,request.startedDate)
            else:
                self.log.debug("adding request into database with worker_id = '%s', fib_id = %d, fib_value = %d, started_date = %d, finished_date = %d"%(request.workerId,request.fibId,request.fibValue,request.startedDate,request.finishedDate))
                query = "insert into fibdata(worker_id, fib_id,fib_value,started_date,finished_date) values('%s',%d, %d,%d,%d)"%(request.workerId, request.fibId, request.fibValue,request.startedDate,request.finishedDate)
            
            
            cur.execute(query)
            db.commit()
            
            # get generated ID
            
            query = "select max(request_id) from fibdata where worker_id = '%s'"%(request.workerId)
            cur.execute(query)
            
            row = cur.fetchone()
            request_id = row[0]
            request.requestId = request_id
            
            self.disconnectFromDB(db)
            
            return request
            
        except MySQLdb.Error as e:
            self.log.error(str(e))
            self.handleMySQLException(e)
       
    def  getRequest(self,requestId):
        """
        returns a request with the specified requestID or None
        """
        try:
            
            db = self.connectToDB()
            query = 'select request_id,worker_id,fib_id, fib_value,started_date, finished_date from fibdata where request_id = %d'%requestId
            
            cur = db.cursor()
            cur.execute(query)
            row = cur.fetchone()
            
            fibData = FibDataRequest(row)
         
            return fibData
        
        except MySQLdb.Error as e:
            self.log.error(str(e))
            self.handleMySQLException(e)
            
           
    def updateRequest(self,request):
        """
        updates a specified request, setting workerId, fibValue and finished_time values ONLY.
        (those are the things that can change)
        """
        # explicit fail if the record hasn't been added
        
        if request.requestId == -1:
            raise 'cannot update a request that has not been added already'
        
        try:
            db = self.connectToDB()
            cur = db.cursor()
            finishedDate = nowInSeconds()
            self.log.debug("updating request  with fib_id = %d and fib_value = %d and requestId = %d"%(request.fibId,request.fibValue,request.requestId))
            query = "update fibdata set worker_id = '%s', fib_value=%d,finished_date=%d where request_id = %d"%(request.workerId, request.fibValue,finishedDate, request.requestId)
            cur.execute(query)
            db.commit()
            self.disconnectFromDB(db)
                
                
        except MySQLdb.Error as e:
            self.log.error(str(e))
            self.handleMySQLException(e)
    
    
    def getRequests(self,worker = None, isPending=False, isDescending=True,limit = 100):
        """
        returns all requests with ordering and limit set as specified
        worker = worker ID to filter by
        isPending = fetch requests with a null complete time (meaning they're still in progress
        isDescending = True by default, gives most recent timestamps first
        limit = take what is needed to display.
        TODO: get requests since a specified timestamp.
        """
    
        requests = []
        self.log.debug("retrieving messages, limit = %d"%limit)
        try:
            
            db = self.connectToDB()
            
            if isDescending == True:
                if worker == None:
                    if isPending == False:
                        query = 'select request_id,worker_id,fib_id, fib_value,started_date, finished_date from fibdata WHERE finished_date IS NOT NULL ORDER BY fib_id DESC LIMIT %d'%limit
                    else:
                        query = 'select request_id,worker_id,fib_id, fib_value,started_date, finished_date from fibdata WHERE finished_date IS NULL ORDER BY fib_id DESC LIMIT %d'%limit
                else: 
                    if isPending == False:
                        query = "select request_id,worker_id,fib_id, fib_value,started_date, finished_date from fibdata where worker_id = '%s' and finished_date IS NOT  NULL ORDER BY fib_id DESC LIMIT %d"%(worker,limit)
                    else:
                        query = "select request_id,worker_id,fib_id, fib_value,started_date, finished_date from fibdata where worker_id = '%s' and finished_date IS NULL ORDER BY fib_id DESC LIMIT %d"%(worker,limit)
                        
            else:
                if worker == None:
                    if isPending == False:
                        query = 'select request_id,worker_id,fib_id, fib_value,started_date, finished_date from fibdata WHERE  finished_date IS NOT NULL ORDER BY fib_id LIMIT %d'%limit # will order ASC because message_id is the primary key
                    else:
                        query = 'select request_id,worker_id,fib_id, fib_value,started_date, finished_date from fibdata WHERE  finished_date IS NULL ORDER BY fib_id LIMIT %d'%limit # will order ASC because message_id is the primary key
                else:
                    if isPending == False:
                        query = "select request_id,worker_id,fib_id, fib_value,started_date, finished_date from fibdata where worker_id = '%s' and finished_date IS NOT NULL ORDER BY fib_id  LIMIT %d"%(worker,limit) # will order ASC because message_id is the primary key
                    else:
                        query = "select request_id,worker_id,fib_id, fib_value,started_date, finished_date from fibdata where worker_id = '%s' and finished_date IS NULL ORDER BY fib_id  LIMIT %d"%(worker,limit) # will order ASC because message_id is the primary key
            
            cur = db.cursor()
            cur.execute(query)
            rows = cur.fetchall()
            
            for row in rows:
                requests.append(FibDataRequest(row))
                
            
        except MySQLdb.Error, e:
            self.handleMySQLException(e)
    
        self.disconnectFromDB(db)
        self.log.debug("returning %d request"%len(requests))
        return requests
    

    def dropAllRequests(self):
        """
        for testing: truncate the db
        """
        self.log.debug("dropping all fibdata")
        try:
            db = self.connectToDB()
            query = "TRUNCATE TABLE fibdata"
            cur = db.cursor()
            cur.execute(query)
            db.commit()
            
        except MySQLdb.Error, e:
            self.log.error(str(e))
            self.handleMySQLException(e)
        
        self.disconnectFromDB(db)
            
