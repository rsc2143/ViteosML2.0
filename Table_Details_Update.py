# -*- coding: utf-8 -*-
"""
Created on Mon Jul  5 17:15:58 2021

@author: consultant138
"""

import os
os.chdir('D:\\ViteosModel2.0')
from src.ViteosMongoDB_Production import ViteosMongoDB_Class as mngdb
import json
import pandas as pd
from datetime import timedelta
import dateutil.parser



def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d

with open(os.getcwd() + '\\data\\Table_Details_Update_parameters.json') as f:
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

mngdb_obj_for_writing = mngdb(
        param_without_ssh  = MongoDB_parameters_for_writing_data_to_dict.get('without_ssh'), 
        param_without_RabbitMQ_pipeline = MongoDB_parameters_for_writing_data_to_dict.get('without_RabbitMQ_pipeline'),
        param_SSH_HOST = MongoDB_parameters_for_writing_data_to_dict.get('SSH_HOST'), 
        param_SSH_PORT = MongoDB_parameters_for_writing_data_to_dict.get('SSH_PORT'),
        param_SSH_USERNAME = MongoDB_parameters_for_writing_data_to_dict.get('SSH_USERNAME'), 
        param_SSH_PASSWORD = MongoDB_parameters_for_writing_data_to_dict.get('SSH_PASSWORD'),
        param_MONGO_HOST = MongoDB_parameters_for_writing_data_to_dict.get('MONGO_HOST'), 
        param_MONGO_PORT = MongoDB_parameters_for_writing_data_to_dict.get('MONGO_PORT'),
        param_MONGO_USERNAME = MongoDB_parameters_for_writing_data_to_dict.get('MONGO_USERNAME'), 
        param_MONGO_PASSWORD = MongoDB_parameters_for_writing_data_to_dict.get('MONGO_PASSWORD'))

mngdb_obj_for_reading.connect_with_or_without_ssh()
mngdb_obj_for_writing.connect_with_or_without_ssh()

db_for_reading_data = mngdb_obj_for_reading.client[MongoDB_parameters_for_reading_data_from_dict.get('db')]
db_for_writing_data = mngdb_obj_for_writing.client[MongoDB_parameters_for_writing_data_to_dict.get('db')]

date_to_copy_TableDetails_for = parameters_dict.get('date_to_copy_TableDetails_for')
penultimate_date_to_copy_TableDetails_for_ymd_format = str((pd.to_datetime(date_to_copy_TableDetails_for,format='%Y-%m-%d') - timedelta(1)).strftime('%Y-%m-%d'))
penultimate_date_to_copy_TableDetails_for_ymd_iso_18_30_format = penultimate_date_to_copy_TableDetails_for_ymd_format + 'T18:30:00.000+0000'

query_for_data = db_for_reading_data[MongoDB_parameters_for_reading_data_from_dict.get('CollectionName')].find({ 
                                                                      "BusinessDate": getDateTimeFromISO8601String(penultimate_date_to_copy_TableDetails_for_ymd_iso_18_30_format)
                                                             },
                                                             parameters_dict.get("TableDetails_columns_dict")
                                                             )

list_of_dicts_query_for_data = list(query_for_data)

db_for_writing_data[MongoDB_parameters_for_writing_data_to_dict.get('CollectionName')].insert_many(list_of_dicts_query_for_data)

