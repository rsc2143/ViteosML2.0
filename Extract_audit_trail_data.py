# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 14:02:48 2021

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
try:
    with open(os.getcwd() + '\\data\\Extract_audit_trail_data_parameters.json') as f:
        parameters_dict = json.load(f)
    MongoDB_parameters_dict = parameters_dict.get('MongoDB_parameters_dict')
    MongoDB_parameters_for_reading_data_from_dict = MongoDB_parameters_dict.get(
        'MongoDB_parameters_for_reading_data_from')

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
    
    Setup_code_to_extract_data_for_list = parameters_dict.get('Setup_code_to_extract_data_for_list')
    Number_of_days_to_go_behind = parameters_dict.get('Number_of_days_to_go_behind')
    Data_path_for_dumping_csv_file =  parameters_dict.get('Data_path_for_dumping_csv_file')
    for setup_code in Setup_code_to_extract_data_for_list:
        query_for_HST_audit_trail_data_for_past_n_days = db_for_reading_MEO_data['HST_RecData_' + setup_code].find({
            "ViewData": {"$ne": None},
             "ViewData.Business Date": { "$gte": (parser.parse(current_date) - timedelta(days=Number_of_days_to_go_behind)).strftime("%m-%d-%Y"), 
                                         "$lte": parser.parse(current_date).strftime("%m-%d-%Y")} 
            },
            {
                "BreakID": 1,
                "LastPerformedAction": 1,
                "TaskInstanceID": 1,
                "SourceCombinationCode": 1,
                "ViewData": 1
            })
        
        list_of_dicts_query_result_for_HST_audit_trail_data_for_past_n_days = list(query_for_HST_audit_trail_data_for_past_n_days)
        
        if (len(list_of_dicts_query_result_for_HST_audit_trail_data_for_past_n_days) != 0):
            HST_audit_trail_data = json_normalize(list_of_dicts_query_result_for_HST_audit_trail_data_for_past_n_days)
            HST_audit_trail_data = HST_audit_trail_data.loc[:, HST_audit_trail_data.columns.str.startswith(('ViewData', '_createdAt'))]
            HST_audit_trail_data['ViewData.Task Business Date'] = HST_audit_trail_data['ViewData.Task Business Date'].apply(dt.datetime.isoformat)
            HST_audit_trail_data.drop_duplicates(keep=False, inplace=True)
            HST_audit_trail_data.to_csv(Data_path_for_dumping_csv_file + 'HST_RecData_' +  str(setup_code))
            # Change added on 14-12-2020 to remove records with multiple values of Side0 and Side1 UniqueIds for statuses like OB,UOB,SDB,CNF and CMF. Typically, these statuses should have single values in Side0 and Side1 UniqueIds. So records not following expected behviour are removed
        else:
            HST_audit_trail_data = pd.DataFrame()
            print('No Data found for ' +  str(setup_code))
        

except Exception:
    logging.error('Exception occured', exc_info=True)
