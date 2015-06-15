import urlparse
from fib_data import  FibDataDB
from worker_data import WorkerDataDB

def initializeWorkerDataDB(dbName):
    
    MYSQL_URL = "mysql://dev:devpass@localhost/%s"%dbName
    
    mysql_url = urlparse.urlparse(MYSQL_URL)
    
    
    #rdb = redis.Redis(host=url.hostname, port=url.port, password=url.password)
    
    url = mysql_url.hostname
    password = mysql_url.password
    user = mysql_url.username
    dbname = mysql_url.path[1:] 
    
    workerDataDB = WorkerDataDB(url,dbname,user,password)
    
    return workerDataDB