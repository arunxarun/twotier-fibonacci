'''
Created on May 13, 2015

@author: arunjacob
'''
import logging
import json
from worker_data import WorkerDataDB, WorkerData, WorkerDisplayData,WorkerDataEncoder

logging.basicConfig()


class ResponseHandler:
    
    def __init__(self,uuid, workerDataDB):
        self.uuid = uuid
        self.workerDataDB = workerDataDB
        self.log = logging.getLogger('worker')
        self.log.setLevel(logging.DEBUG)
 
    
    def getInProcess(self):
        self.log.debug("handling /inprocess GET")
    
        allData = self.workerDataDB.getWorkItems(isPending=True)
        
        workerInfo = []
        for workerData in allData:
            workerInfo.append(WorkerDisplayData(workerData))
            
            
        return json.dumps(workerInfo,cls=WorkerDataEncoder)
    
    def getComplete(self):
        self.log.debug("handling /complete GET")
    
        allData = self.workerDataDB.getWorkItems(isPending=False)
        workerInfo = []
        for workerData in allData:
            workerInfo.append(WorkerDisplayData(workerData))
            
        return json.dumps(workerInfo,cls=WorkerDataEncoder)

    def getInstance(self):
        return "{'instance-id':'%s'}"%self.uuid
        