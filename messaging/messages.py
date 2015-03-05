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

logging.basicConfig()

    
messageLogger = logging.getLogger('message')
messageLogger.setLevel(logging.DEBUG)

class MessageEncoder(json.JSONEncoder):
        
    def default(self,o):
        return o.__dict__

class Message(object):
    '''
    contains message_key, created_date
    '''


    def __init__(self, value = None, body = None):
                
        messageLogger.debug("initializing from JSON")
        
        if body != None:
            if body.has_key('messageKey') == True:
                self.messageKey = body['messageKey']
            else:
                messageLogger.error("invalid JSON format, sequence_id not found")
                raise 'invalid format'
            
            # created is optional. It's always overwritten on insert to db.
            if body.has_key('createdDate'):
                self.createdDate = body['createdDate']
        elif value != None:
            self.messageKey = int(value)
            self.createdDate = int(datetime.datetime.now().strftime("%s"))
            
    
    
    
  
            
class MessageQueue:
    def __init__(self,amqp_url):
        self.log =  logging.getLogger('messageQueue')
        self.log.setLevel(logging.DEBUG)
        self.amqp_url = amqp_url
        self.channel = None
    
       
    def createQueue(self,queueName):
        self.log.debug("creating queue %s"%queueName)
        parameters = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel() 
        channel.queue_declare(queue=queueName)
        connection.close()
        
    def deleteQueue(self,queueName):
        self.log.debug("deleting queue %s"%queueName)
        parameters = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel() 
        channel.queue_delete(queue=queueName)
        connection.close()
        
    def getMessages(self, queue_name,messageCount):
        
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
            
            method_frame, header_frame, body = channel.basic_get(queue_name)
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
            
    def extractMessage(self,messageBody):
        """
        puts the message in a Message classs
        """
        messageContents = json.loads(messageBody)
        try:
            
            message = Message(body=messageContents)
            return message
        except:
            
            self.log.error(str(sys.exc_info()[0]))
        
        
        
    def sendMessage(self, queue_name, message):
        parameters = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel() 
        
    
        json_body = json.dumps(message,cls=MessageEncoder)
         
        channel.basic_publish(exchange='', routing_key=queue_name, body=json_body)
        connection.close()
                    
   
        
    
   
        