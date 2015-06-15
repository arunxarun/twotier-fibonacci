'''
Created on Nov 13, 2014

@author: arunjacob
'''
import json
import pika
import threading
import logging
import datetime
import urlparse
import os
import sys
from date_formatting_utils import nowInSeconds

logging.basicConfig()

    
messageLogger = logging.getLogger('message')
messageLogger.setLevel(logging.DEBUG)

class MessageEncoder(json.JSONEncoder):
        
    def default(self,o):
        return o.__dict__


class WorkRequestMessage(object):
    '''
    contains request id, message_key, created_date
    
    '''


    def __init__(self, requestId = None, messageKey = None, body = None):
                
        messageLogger.debug("initializing from JSON")
        
        if body != None:
            if body.has_key('requestId') == True:
                self.requestId = body['requestId']
            else:
                messageLogger.error("invalid JSON format, requestId not found")
                raise 'invalid format'
            
            if body.has_key('messageKey') == True:
                self.messageKey = body['messageKey']
            else:
                messageLogger.error("invalid JSON format, messageKey not found")
                raise 'invalid format'
            
            # created is optional.
            if body.has_key('startedDate'):
                self.starteDate = body['startedDate']
            else: 
                self.startedDate = nowInSeconds()
        else:
            self.requestId = requestId
            self.messageKey = messageKey
            self.createdDate = nowInSeconds()
            
    

            
class WorkResultMessage(object):
    '''
    contains request id, message key, message value, created date
    '''
    
    def __init__(self, requestId = None, messageKey = None, messageValue = None, startedDate = None, finishedDate = None, body = None):
                
        messageLogger.debug("initializing from JSON")
        
        if body != None:
            if body.has_key('requestId') == True:
                self.requestId = body['requestId']
            else:
                messageLogger.error("invalid JSON format, requestId not found")
                raise 'invalid format'
            
            if body.has_key('messageKey') == True:
                self.messageKey = body['messageKey']
            else:
                messageLogger.error("invalid JSON format, messageKey not found")
                raise 'invalid format'
            
            if body.has_key('messageValue') == True:
                self.messageValue = body['messageValue']
            else:
                messageLogger.error("invalid JSON format, messageValue not found")
                raise 'invalid format'
            
            if body.has_key('startedDate'):
                self.startedDate = body['startedDate']
                
            if body.has_key('finishedDate'):
                self.finishedDate = body['finishedDate']
        else:
            self.requestId = requestId
            self.messageKey = messageKey
            self.messageValue = messageValue
            self.startedDate = startedDate
            
            if finishedDate == None:
                self.finishedDate = int(datetime.datetime.now().strftime("%s"))
            else:
                self.finishedDate = finishedDate




  
            
class MessageQueue:
    def __init__(self,amqp_url):
        self.log =  logging.getLogger('messageQueue')
        self.log.setLevel(logging.DEBUG)
        self.amqp_url = amqp_url
        self.channel = None
        self.keepGoing = True
        self.queueName = None
    
     
    def stopProcessing(self):
        self.keepGoing = False
          
    def createQueue(self,queueName):
        self.log.debug("creating queue %s"%queueName)
        parameters = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(parameters)
        self.channel = connection.channel() 
        self.queueName = queueName
        self.channel.queue_declare(queue=self.queueName)
        connection.close()
        
        
    def deleteQueue(self):
        self.log.debug("deleting queue %s"%self.queueName)
        parameters = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel() 
        channel.queue_delete(queue=self.queueName)
        connection.close()
        
    def getMessages(self, messageCount):
        
        """
        pulls messages from the queue, for a specified count of messages
        """
        messages = []
        self.log.debug("attempting to fetch a max of  %d messages from queue"%messageCount)
        parameters = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(parameters)

        channel = connection.channel()
        i = 0
        
        for i in range(0,messageCount):
            
            method_frame, header_frame, body = channel.basic_get(self.queueName)
            if method_frame:
                try:
                    self.log.debug("message %d, method frame = %s, header frame = %s"%(i,method_frame,header_frame))
                    messages.append(self.extractMessage(body)) 
                except:
                    self.log.error ("message %d, invalid format of message, removing message from queue"%i)
                
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    
            else:
                break
        self.log.debug("returning %d messages found"%len(messages))    
        return messages
    
    def getAndProcessMessages(self,processMessage,extractMessage,quitOnEmpty = False):
        """
        pulls messages from the queue, for a specified count of messages
        """
        parameters = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(parameters)

        channel = connection.channel()
        
        while self.keepGoing == True:
            method_frame, header_frame, body = channel.basic_get(self.queueName)
            if method_frame:
                try:
                    self.log.debug("method frame = %s, header frame = %s"%(method_frame,header_frame))
                    processMessage(extractMessage(body))
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag) 
                except:
                    self.log.error ("invalid format of message: method frame = %s, header frame = %s, removing message from queue")
            elif quitOnEmpty == True:
                    break
            
                
   
        
        
        
    def sendMessage(self,  message):
        parameters = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel() 
        
    
        json_body = json.dumps(message,cls=MessageEncoder)
        self.log.debug(json_body) 
        channel.basic_publish(exchange='', routing_key=self.queueName, body=json_body)
        connection.close()
                    
   
        
    
   
        