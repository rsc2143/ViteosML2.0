# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 16:23:01 2020

@author: consultant138
"""
import os
model_path = os.getcwd() 
import json
#print('Rabbit MQ called')

from src.RabbitMQ_Production import RabbitMQ_Class as rb_mq

with open(model_path + '\\data\\Weiss_125_Production_loop_1_parameters.json') as f:
    ML2_RMQ_Receive_parameters_dict = json.load(f)

ML2_RMQ_Receive_RabbitMQ_parameters_dict = ML2_RMQ_Receive_parameters_dict.get('RabbitMQ_parameters_dict')
ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict = ML2_RMQ_Receive_RabbitMQ_parameters_dict.get('RabbitMQ_parameters_for_ML2_to_read_from')


import pika
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    
rb_mq_obj_new = rb_mq(
        param_RABBITMQ_QUEUEING_PROTOCOL = ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('RABBITMQ_QUEUEING_PROTOCOL'), 
        param_RABBITMQ_USERNAME = ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('RABBITMQ_USERNAME'),
        param_RABBITMQ_PASSWORD = ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('RABBITMQ_PASSWORD'), 
        param_RABBITMQ_HOST_IP = ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('RABBITMQ_HOST_IP'), 
        param_RABBITMQ_PORT = ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('RABBITMQ_PORT'), 
        param_RABBITMQ_VIRTUAL_HOST = ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('RABBITMQ_VIRTUAL_HOST'), 
        param_RABBITMQ_EXCHANGE = ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('RABBITMQ_EXCHANGE'), 
        param_RABBITMQ_QUEUE = ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('RABBITMQ_QUEUE'), 
        param_RABBITMQ_ROUTING_KEY = ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('RABBITMQ_ROUTING_KEY'), 
        param_test_message_publishing = ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('test_message_publishing'), 
        param_timeout = ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('timeout'))


connection = pika.BlockingConnection(pika.connection.URLParameters(rb_mq_obj_new.connection_string))
channel = connection.channel()

def add_callback_threadsafe(ch, method, properties, body):
   print("%r" % body)


timeout = 30

def on_timeout():
  global connection
  connection.close()

connection.add_timeout(timeout, on_timeout)
	
channel.basic_consume(add_callback_threadsafe,
                      queue=ML2_RMQ_Receive_RabbitMQ_parameters_for_ML2_to_read_from_dict.get('RABBITMQ_QUEUE'),
                      no_ack=True)

channel.start_consuming()
