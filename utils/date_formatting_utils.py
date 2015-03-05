'''
Created on Feb 18, 2015
contains basic date formatting utilities. 
@author: arunjacob
'''
import datetime

def nowInSeconds():
        """
        for instant timestamping
        """
        return int(datetime.datetime.now().strftime("%s")) 
    
def prettyPrintTime(timeInSec):
    """
    for conversion to JSON, readability, etc
    """ 
    return datetime.datetime.fromtimestamp(timeInSec).strftime("%Y-%m-%d %H:%M:%S")