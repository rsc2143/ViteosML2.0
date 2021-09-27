# -*- coding: utf-8 -*-
"""
Created on Tue Jul 27 18:29:45 2021

@author: consultant138
"""

import os
os.chdir('D:\\ViteosModel2.0\\')
from src.ViteosMongoDB_Production import  ViteosMongoDB_Class as mngdb
import json

with open(os.getcwd() + '\\data\\Production_Model_parameters.json') as f:
    parameters_dict = json.load(f)

MongoDB_parameters_dict = parameters_dict.get('MongoDB_parameters_dict')
MongoDB_parameters_for_reading_data_from_dict = MongoDB_parameters_dict.get('MongoDB_parameters_for_reading_data_from')
MongoDB_parameters_for_writing_data_to_dict = MongoDB_parameters_dict.get('MongoDB_parameters_for_writing_data_to')

mngdb_obj_for_reading = mngdb(
        param_without_ssh  = MongoDB_parameters_for_reading_data_from_dict.get('without_ssh'), 
        param_without_RabbitMQ_pipeline = MongoDB_parameters_for_reading_data_from_dict.get('without_RabbitMQ_pipeline'),
        param_SSH_HOST = MongoDB_parameters_for_reading_data_from_dict.get('SSH_HOST'), 
        param_SSH_PORT = MongoDB_parameters_for_reading_data_from_dict.get('SSH_PORT'),
        param_SSH_USERNAME = MongoDB_parameters_for_reading_data_from_dict.get('SSH_USERNAME'), 
        param_SSH_PASSWORD = MongoDB_parameters_for_reading_data_from_dict.get('SSH_PASSWORD'),
        param_MONGO_HOST = MongoDB_parameters_for_reading_data_from_dict.get('MONGO_HOST'), 
        param_MONGO_PORT = MongoDB_parameters_for_reading_data_from_dict.get('MONGO_PORT'),
        param_MONGO_USERNAME = MongoDB_parameters_for_reading_data_from_dict.get('MONGO_USERNAME'), 
        param_MONGO_PASSWORD = MongoDB_parameters_for_reading_data_from_dict.get('MONGO_PASSWORD'))

mngdb_obj_for_reading.connect_with_or_without_ssh()

db_for_reading_MEO_data = mngdb_obj_for_reading.client[MongoDB_parameters_for_reading_data_from_dict.get('db')]

coll_Tasks = db_for_reading_MEO_data['Tasks']

query_1_for_MEO_data = coll_Tasks.find({

        {"InstanceID": 1, "Status": 1}