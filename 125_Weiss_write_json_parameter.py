# -*- coding: utf-8 -*-
"""
Created on Tue Apr 13 16:45:24 2021

@author: riteshkumar.patra
"""
import json
#from src.Write import Write_Class
import os
os.chdir('D:\\ViteosModel2.0\\data\\')

#Create json file for model execution
RabbitMQ_parameters_dict = {
                        'RabbitMQ_parameters_for_ML2_to_publish_to' : 
                            {\
                            'RABBITMQ_QUEUEING_PROTOCOL' : 'amqps', 
                            'RABBITMQ_USERNAME' : 'recon2', 
                            'RABBITMQ_PASSWORD' : 'recon2', 
                            'RABBITMQ_HOST_IP' : 'vu-uat', 
                            'RABBITMQ_PORT' : '5671', 
                            'RABBITMQ_VIRTUAL_HOST' : 'viteos', 
                            'RABBITMQ_EXCHANGE' : 'ReconPROD_PARALLELExchange_MLAck', 
                            'RABBITMQ_QUEUE' : 'VNFRecon_ReadFromML_Ack_PROD_PARALLEL', 
                            'RABBITMQ_ROUTING_KEY' : 'Recon2_ReadFromMLAck_PROD_PARALLEL', 
                            'test_message_publishing' : True, 
                            'timeout' : 10        
                            },
                        'RabbitMQ_parameters_for_ML2_to_read_from' : 
                            {\
                            'RABBITMQ_QUEUEING_PROTOCOL' : 'amqps', 
                            'RABBITMQ_USERNAME' : 'recon2', 
                            'RABBITMQ_PASSWORD' : 'recon2', 
                            'RABBITMQ_HOST_IP' : 'vu-uat', 
                            'RABBITMQ_PORT' : '5671', 
                            'RABBITMQ_VIRTUAL_HOST' : 'viteos', 
                            'RABBITMQ_EXCHANGE' : 'ReconPROD_PARALLELExchange_ML', 
                            'RABBITMQ_QUEUE' : 'VNFRecon_WriteToML_PROD_PARALLEL', 
                            'RABBITMQ_ROUTING_KEY' : 'Recon2_WriteToML_PROD_PARALLEL', 
                            'test_message_publishing' : True, 
                            'timeout' : 10        
                            },
                        'RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement' : 
                            {\
                            'RABBITMQ_QUEUEING_PROTOCOL' : 'amqps', 
                            'RABBITMQ_USERNAME' : 'recon2', 
                            'RABBITMQ_PASSWORD' : 'recon2', 
                            'RABBITMQ_HOST_IP' : 'vu-uat', 
                            'RABBITMQ_PORT' : '5671', 
                            'RABBITMQ_VIRTUAL_HOST' : 'viteos', 
                            'RABBITMQ_EXCHANGE' : 'ReconPROD_PARALLELExchange_MLAck', 
                            'RABBITMQ_QUEUE' : 'VNFRecon_WriteToML_Ack_PROD_PARALLEL', 
                            'RABBITMQ_ROUTING_KEY' : 'Recon2_WriteToMLAck_PROD_PARALLEL', 
                            'test_message_publishing' : True, 
                            'timeout' : 10        
                            }
                        }

MongoDB_parameters_dict = {
                        'MongoDB_parameters_for_reading_data_from' : 
                            {\
                             'without_ssh' : True, 
                             'without_RabbitMQ_pipeline' : True,
                             'SSH_HOST' : None, 
                             'SSH_PORT' : None,
                             'SSH_USERNAME' : None, 
                             'SSH_PASSWORD' : None,
                             'MONGO_HOST' : '10.1.79.212', 
                             'MONGO_PORT' : 27017,
                             'MONGO_USERNAME' : '', 
                             'MONGO_PASSWORD' : '',
                             'db' : 'ReconDB'
                             },
                        'MongoDB_parameters_for_writing_data_to' : 
                            {\
                             'without_ssh' : True, 
                             'without_RabbitMQ_pipeline' : True,
                             'SSH_HOST' : None, 
                             'SSH_PORT' : None,
                             'SSH_USERNAME' : None, 
                             'SSH_PASSWORD' : None,
                             'MONGO_HOST' : '10.1.79.212', 
                             'MONGO_PORT' : 27017,
                             'MONGO_USERNAME' : '', 
                             'MONGO_PASSWORD' : '',
                             'db' : 'ReconDB_ML'
                             }
                            }

Weiss_125_parameters_dict = {'RabbitMQ_parameters_dict' : RabbitMQ_parameters_dict,
                             'MongoDB_parameters_dict' : MongoDB_parameters_dict}
with open('Weiss_125_Production_loop_1_parameters.json', 'w') as outfile:
    json.dump(Weiss_125_parameters_dict, outfile, indent=4, sort_keys=True)

#Write_Class.write_dict_at_top(param_filename = '125_Weiss_Production_loop_1_parameters.json', param_dict_to_add = MongoDB_parameters_dict)
