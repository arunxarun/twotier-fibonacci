'''
Created on May 13, 2015

@author: arunjacob
'''
import logging
import json
from fib_data import FibDataDB, FibDataRequest, WorkerData,DataEncoder

logging.basicConfig()


class ResponseHandler:
    
    def __init__(self,uuid, fibDataDB):
        self.uuid = uuid
        self.fibDataDB = fibDataDB
        self.log = logging.getLogger('worker')
        self.log.setLevel(logging.DEBUG)
 
    
        
    
    def getInProcess(self):
        self.log.debug("handling /inprocess GET")
    
        allRequestData = self.fibDataDB.getRequests(isPending=True)
        
        workerInfo = []
        for request in allRequestData:
            workerInfo.append(WorkerData(request))
            
            
        return json.dumps(workerInfo,cls=DataEncoder)
    
    def getComplete(self):
        self.log.debug("handling /complete GET")
    
        completeData = self.fibDataDB.getRequests(isPending=False)
        workerInfo = []
        for request in completeData:
            workerInfo.append(WorkerData(request))
            
        return json.dumps(workerInfo,cls=DataEncoder)

    def getInstance(self):
        return "{'instance-id':'%s'}"%self.uuid
        