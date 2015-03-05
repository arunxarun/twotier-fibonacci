'''
Created on Nov 14, 2014

@author: arunjacob
'''

from messages import  MessageQueue


def initializeMessageQueue(rabbit_url):
        
    # this assumes a real external rabbit queue. TODO: create an internal version.
    
    #rdb = redis.Redis(host=url.hostname, port=url.port, password=url.password)
    
    
    messageQ = MessageQueue(rabbit_url)
    
    return messageQ