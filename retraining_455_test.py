# -*- coding: utf-8 -*-
"""
Created on Wed Sep  1 13:42:18 2021

@author: consultant138
"""

import os
os.chdir('D:\\ViteosModel2.0')
import json
from src.ViteosMongoDB_Production import  ViteosMongoDB_Class as mngdb
from dateutil import parser
from pandas.io.json import json_normalize

import datetime as dt
import datetime
from datetime import datetime, timedelta
now = datetime.now()
current_date = now.strftime('%d-%m-%Y')
import pandas as pd
import logging
import pprint

setup_code = '455'

try:
    with open(os.getcwd() + '\\data\\Production_Lombard_retraining_parameters.json') as f:
        parameters_dict = json.load(f)
    MongoDB_parameters_dict = parameters_dict.get('MongoDB_parameters_dict')
    MongoDB_parameters_for_reading_data_from_dict = MongoDB_parameters_dict.get('MongoDB_parameters_for_reading_data_from')
    projection_columns_dict = parameters_dict.get('projection_columns_dict')
    Number_of_days_to_go_behind = parameters_dict.get('Number_of_days_to_go_behind')

    mngdb_obj_for_reading = mngdb(
        param_without_ssh=MongoDB_parameters_for_reading_data_from_dict.get('without_ssh'),
        param_without_RabbitMQ_pipeline=MongoDB_parameters_for_reading_data_from_dict.get('without_RabbitMQ_pipeline'),
        param_SSH_HOST=MongoDB_parameters_for_reading_data_from_dict.get('SSH_HOST'),
        param_SSH_PORT=MongoDB_parameters_for_reading_data_from_dict.get('SSH_PORT'),
        param_SSH_USERNAME=MongoDB_parameters_for_reading_data_from_dict.get('SSH_USERNAME'),
        param_SSH_PASSWORD=MongoDB_parameters_for_reading_data_from_dict.get('SSH_PASSWORD'),
        param_MONGO_HOST=MongoDB_parameters_for_reading_data_from_dict.get('MONGO_HOST'),
        param_MONGO_PORT=MongoDB_parameters_for_reading_data_from_dict.get('MONGO_PORT'),
        param_MONGO_USERNAME=MongoDB_parameters_for_reading_data_from_dict.get('MONGO_USERNAME'),
        param_MONGO_PASSWORD=MongoDB_parameters_for_reading_data_from_dict.get('MONGO_PASSWORD'))
    mngdb_obj_for_reading.connect_with_or_without_ssh()
    db_for_reading_MEO_data = mngdb_obj_for_reading.client[MongoDB_parameters_for_reading_data_from_dict.get('db')]

    query = db_for_reading_MEO_data['HST_RecData_' + setup_code].find({
        "ViewData": {"$ne": None},
        "ViewData.Status": {"$eq": "OB"},
        "ViewData.InternalComment2": {"$ne": None},

         "ViewData.Business Date": { "$gte": (parser.parse('16-08-2021') - timedelta(days=Number_of_days_to_go_behind)).strftime("%m-%d-%Y"), 
                                     "$lte": parser.parse('16-08-2021').strftime("%m-%d-%Y")} 
        },
        projection_columns_dict
        )
    list_of_dicts_query = list(query)

    data = json_normalize(list_of_dicts_query)
#        HST_audit_trail_data_just_date['ViewData.Task Business Date'] = HST_audit_trail_data['ViewData.Task Business Date'].apply(dt.datetime.isoformat)
except Exception:
    logging.error('Exception occured', exc_info=True)


#df_455 = pd.read_csv("D:\\HST_RecData_455")
#df_455_ob = df_455[df_455['ViewData.Status'] == 'OB']
#df_455_ob_with_comment = df_455_ob[~df_455_ob['ViewData.InternalComment2'].isna()]



#df_455_ob_with_comment['ViewData.InternalComment2'].value_counts()
