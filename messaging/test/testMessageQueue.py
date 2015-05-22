'''
Created on Nov 14, 2014

@author: arunjacob
'''
import unittest
import urlparse
import os
import pika
import time
import json
import sets

from messages import Message,MessageEncoder, MessageQueue
from test_msgutils import initializeMessageQueue

class Test(unittest.TestCase):


    def test0CreateAndDeleteQueue(self):
        
        rabbit_url = os.environ['RABBITMQ_URL']
        queue_name = os.environ['QUEUE_NAME']

        messageQ1  = initializeMessageQueue(rabbit_url)
        
        messageQ1.createQueue(queue_name)
        messageQ1.deleteQueue(queue_name)
        
    def test1RunMessageQueue(self):
        #10.8.50.235:5671/%2f
        rabbit_url = os.environ['RABBITMQ_URL']
        queue_name = os.environ['QUEUE_NAME']

        testMessages = []
        messageQ1 = initializeMessageQueue(rabbit_url)
        messageQ1.createQueue(queue_name)
        for i in range(0,5):
            message = Message(value = i)
            testMessages.append(message)
            messageQ1.sendMessage(queue_name,message)
        
        messageQ2 = initializeMessageQueue(rabbit_url)
        sentMsgs = messageQ2.getMessages(queue_name,5)
   
        self.assertTrue(len(sentMsgs) == len(testMessages))
        
        messageQ1.deleteQueue(queue_name)
        
    def test2GetAndProcessMessages(self):
        rabbit_url = os.environ['RABBITMQ_URL']
        queue_name = os.environ['QUEUE_NAME']

        testMessages = []
        messageQ1 = initializeMessageQueue(rabbit_url)
        messageQ1.createQueue(queue_name)
        for i in range(0,5):
            message = Message(value = i)
            testMessages.append(message)
            messageQ1.sendMessage(queue_name,message)
            
        messageQ2 = initializeMessageQueue(rabbit_url)
        
        def processMessage(message):
            valid  = sets.Set({0,1,2,3,4})
            
            if message.messageKey not in valid:
                raise Exception('invalid value '%message.value)
            
        messageQ2.getAndProcessMessages(queue_name,processMessage,True)
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()