#!/usr/bin/env python
# coding: utf-8

import os
os.chdir('D:\\ViteosModel')

import pandas as pd
import numpy as np
import datetime as dt
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from pandas.io.json import json_normalize
import datetime
from datetime import datetime,timedelta
import math
# ### Reading comments file
import dateutil.parser
from dateutil.parser import parse
import ViteosMongoDB_Query
import Viteos_Miscellaneous_Functions
import datetime
import pandas as pd
import win32com.client
import os
import getpass

import dateutil
from dateutil import parser

client = 'Schonfeld'
setup = '897'
setup_code = '897'
number_of_days_to_go_behind = 25

Schonfeld_897_number_of_days_to_go_behind = 25
Schonfeld_897_output_folder_path = "\\data\\Schonfeld\\output_files"
Schonfeld_897_position_file_path = "\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\Files and Rules for Lookup\\Rules delivery from Ronson\\Schonfeld\\Schonfeld ML final\\57 Position Recon\\"
Schonfeld_897_position_file_first_sheet_name = "FIFTY_SEVEN"
Schonfeld_897_position_file_second_sheet_name = "FIFTY_SEVENCAY"
Schonfeld_897_position_file_first_sheet_origin_row_number = 4
Schonfeld_897_position_file_first_sheet_origin_col_number = 1
Schonfeld_897_position_file_second_sheet_origin_row_number = 4
Schonfeld_897_position_file_second_sheet_origin_col_number = 1
Schonfeld_897_position_file_password = "SSA2020"
Schonfeld_897_position_file_columns_list = ["Fund","Geneva Account","Investment ID","Security Type","Currency","Description","SSA vs PB Qty Difference","SSA vs PB Price Difference","SSA vs PB Local MV Difference"]

mngdb_137_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_137_server.connect_with_or_without_ssh()
#ReconDB_ML_137_server = mngdb_137_server.client['ReconDB']
#Changes asked by Shitanshu for db on 24-03-2021
ReconDB_ML_137_server_for_reading = mngdb_137_server.client['ReconDB']
ReconDB_ML_137_server_for_writing = mngdb_137_server.client['ReconDB_ML']

ReconDB_ML_Testing_137_server = mngdb_137_server.client['ReconDB_ML_Testing']

mngdb_prod_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = 'vitblrrecdb05.viteos.com', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_prod_server.connect_with_or_without_ssh()
ReconDB_prod_server = mngdb_prod_server.client['ReconDB']
#Change added on 19-02-2021 as per Rohit. For Abhijeet, he wanted to get last 10 days data to solve the accumulation issue

ViteosQuery_obj = ViteosMongoDB_Query.ViteosMongoDB_Query_Class(param_setup_code = setup_code, param_TaskID_server = ReconDB_ML_137_server_for_reading, param_Meo_data_server = ReconDB_ML_137_server_for_reading) 

base_dir_containing_client = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\'

os.chdir('D:\\ViteosModel')

print('Starting predictions for ' + str(client) + ', setup_code = ')
print(setup_code)


#meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Schonfeld/meo_df_setup_897_date_' + date_to_analyze_ymd_format + '.csv'
#meo_df = pd.read_csv(meo_filename)

#meo_df = pd.read_csv(base_dir_containing_client + 'meo_df_setup_897_date_2021-01-04.csv')

def normalize_bp_acct_col_names(fun_df):
    bp_acct_col_names_mapping_dict = {
                                      'ViewData.Cust Net Amount' : 'ViewData.B-P Net Amount',
                                      'ViewData.Cust Net Amount Difference' : 'ViewData.B-P Net Amount Difference',
                                      'ViewData.Cust Net Amount Difference Absolute' : 'ViewData.B-P Net Amount Difference Absolute',
                                      'ViewData.CP Net Amount' : 'ViewData.B-P Net Amount',
                                      'ViewData.CP Net Amount Difference' : 'ViewData.B-P Net Amount Difference',
                                      'ViewData.CP Net Amount Difference Absolute' : 'ViewData.B-P Net Amount Difference Absolute',
                                      'ViewData.PMSVendor Net Amount' : 'ViewData.Accounting Net Amount'
                                        }
    fun_df.rename(columns = bp_acct_col_names_mapping_dict, inplace = True)
    return(fun_df)

def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d

def contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(fun_row):
    
    if(',' in str(fun_row['ViewData.Side0_UniqueIds'])):
        Side_0_contains_comma = 1
    else:
        Side_0_contains_comma = 0

    if(',' in str(fun_row['ViewData.Side1_UniqueIds'])):
        Side_1_contains_comma = 1
    else:
        Side_1_contains_comma = 0
    
    if((str(fun_row['ViewData.Status']) in ['OB','SDB','UOB','CNF','CMF']) and ((Side_0_contains_comma == 1) or (Side_1_contains_comma == 1))):
        return('remove')
    else:
        return('keep')

def dmy_value_with_or_without_zero(param_dmy_value): 
    if((param_dmy_value > 0) & (param_dmy_value < 10)):
        return("0" + str(param_dmy_value))
    else:
        return(str(param_dmy_value))

#meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/meo_df_setup_' + setup_code + '_date_' + date_to_analyze_ymd_format + '.csv'
#meo_df = pd.read_csv(meo_filename)

query_1_for_MEO_data = ReconDB_ML_137_server_for_reading['RecData_' + setup_code].find({ "LastPerformedAction": 31},ViteosMongoDB_Query.data_projection_meo_columns)
list_of_dicts_query_result_1 = list(query_1_for_MEO_data)

meo_df = json_normalize(list_of_dicts_query_result_1)
meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
meo_df.drop_duplicates(keep=False, inplace = True)

#Change added on 01-03-2021 to remove records with multiple values of Side0 and Side1 UniqueIds for statuses like OB,UOB,SDB,CNF and CMF. Typically, these statuses should have single values in Side0 and Side1 UniqueIds. So records not following expected behviour are removed
meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] = meo_df.apply(lambda row : contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(fun_row = row), axis = 1,result_type="expand")
meo_df = meo_df[~(meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] == 'remove')]

print('meo size')
print(meo_df.shape[0])


date_i = pd.to_datetime(pd.to_datetime(meo_df['ViewData.Task Business Date'])).dt.date.astype(str).mode()[0]
print(str(date_i))

date_to_analyze_ymd_format = date_i
penultimate_date_to_analyze_ymd_format = str((pd.to_datetime(date_i) - timedelta(1)).strftime('%Y-%m-%d'))
penultimate_date_to_analyze_ymd_iso_18_30_format = penultimate_date_to_analyze_ymd_format + 'T18:30:00.000+0000'
date_to_analyze_ymd_iso_00_00_format = date_to_analyze_ymd_format + 'T00:00:00.000+0000'

str_date_in_ddmmyyyy_format = pd.to_datetime(date_i,format='%Y-%m-%d').strftime('%d-%m-%Y')

#meo_appended_data = ViteosQuery_obj.get_past_n_days_meo_df(param_collection_to_get_taskid_from='Tasks',param_number_of_days_to_go_behind = number_of_days_to_go_behind,param_str_date_in_ddmmyyyy_format=str_date_in_ddmmyyyy_format)

#days = meo_df['ViewData.Task Business Date'].value_counts().reset_index()
#date = list(days[days['ViewData.Task Business Date']>50]['index'])[0]
#
#
#date1 = pd.to_datetime(date)
#
#day = date1.day
#mon = date1.month
#yr = date1.year
#
##path = 'Schonfield\daily files\\'
#path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\Files and Rules for Lookup\\Rules delivery from Ronson\\Schonfeld\\Schonfeld ML final\\57 Position Recon\\'
#if((day > 0) & (day < 10)):
#    filename = '57 Position recon - '+ str(mon) + '0' + str(day) + str(yr)+'_copy.xlsx'
#else:
#    filename = '57 Position recon - '+ str(mon) + str(day) + str(yr)+'_copy.xlsx'
#filename_pos_file = path + filename
#
#def read_pos_file_and_concat_to_single_pos_df(fun_filepath):
#    xlsx_obj = pd.ExcelFile(fun_filepath)
#    xlsx_sheet_names_list = xlsx_obj.sheet_names
#    True_False_list_for_Document_Map_substring_in_xlsx_sheet_names_list = ['Document Map' in x for x in xlsx_sheet_names_list]
#    Document_Map_substring_in_xlsx_sheet_names_list = [x for x,y in zip(xlsx_sheet_names_list,True_False_list_for_Document_Map_substring_in_xlsx_sheet_names_list) if y == True]
#    string_name_for_Document_Map_substring_in_xlsx_sheet_names_list = Document_Map_substring_in_xlsx_sheet_names_list[0]
#    
#    xlsx_sheet_names_list_without_Document_Map = [x for x in xlsx_sheet_names_list if x != string_name_for_Document_Map_substring_in_xlsx_sheet_names_list]
#    df_sheet_list = []
#    for sheet_name in xlsx_sheet_names_list_without_Document_Map:
#        df_sheet = xlsx_obj.parse(sheet_name,skiprows = 3)
#        df_sheet_list.append(df_sheet)
#    pos_df = pd.concat(df_sheet_list)
#    return(pos_df)
#
#
#    
#def read_passowrd_protected_pos_file_and_concat_to_single_pos_df(param_filepath, param_passwrod, param_sheet_names_list, param_start_row_num, param_start_col_num):
#    xl_app = win32com.client.Dispatch('Excel.Application')
#    #pwd = getpass.getpass('Enter file password: ')
#    xl_wb = xl_app.Workbooks.Open(param_filepath, False, True, None, param_passwrod)
#    xl_app.Visible = False
#    df_sheet_list = []
#    for sheet_name in param_sheet_names_list:
#        xl_sh = xl_wb.Worksheets(sheet_name)
#
#    # Get last row    
#        row_num = param_start_row_num
#        cell_val = ''
#        while cell_val != None:
#            cell_val = xl_sh.Cells(row_num, param_start_col_num).Value
#            row_num = row_num + 1
#        last_row = row_num - 2
#
#    # Get last_column
#        col_num = param_start_col_num
#        cell_val = ''
#        while cell_val != None:
#            cell_val = xl_sh.Cells(param_start_row_num, col_num).Value
#            col_num = col_num + 1
#        last_col = col_num - 2
#    
#        content = xl_sh.Range(xl_sh.Cells(param_start_row_num, param_start_col_num), xl_sh.Cells(last_row, last_col)).Value
#        df_sheet = pd.DataFrame(list(content[1:]), columns=content[0])
#        df_sheet_list.append(df_sheet)
#    pos_df = pd.concat(df_sheet_list)
#    return(pos_df)
#
#    
#
#pos = read_pos_file_and_concat_to_single_pos_df(fun_filepath = filename_pos_file)
#

query_for_MEO_appended_data = ReconDB_ML_137_server_for_reading['RecData_' + setup_code + '_Historic'].find({
    "ViewData": {"$ne": None},
     "ViewData.Business Date": { "$gte": (parser.parse(date_i) - timedelta(days=Schonfeld_897_number_of_days_to_go_behind)).strftime("%m-%d-%Y"), 
                                 "$lte": parser.parse(date_i).strftime("%m-%d-%Y")} 
    },
    {
        "DataSides": 1,
        "BreakID": 1,
        "LastPerformedAction": 1,
        "TaskInstanceID": 1,
        "SourceCombinationCode": 1,
        "MetaData": 1,
        "ViewData": 1
    })

list_of_dicts_query_result_MEO_appended_data = list(query_for_MEO_appended_data)

if (len(list_of_dicts_query_result_MEO_appended_data) != 0):
    meo_appended_data = json_normalize(list_of_dicts_query_result_MEO_appended_data)
    meo_appended_data = meo_appended_data.loc[:, meo_appended_data.columns.str.startswith(('ViewData', '_createdAt'))]
    meo_appended_data['ViewData.Task Business Date'] = meo_appended_data['ViewData.Task Business Date'].apply(dt.datetime.isoformat)
    meo_appended_data.drop_duplicates(keep=False, inplace=True)
    meo_appended_data = normalize_bp_acct_col_names(fun_df=meo_appended_data)

    # Change added on 14-12-2020 to remove records with multiple values of Side0 and Side1 UniqueIds for statuses like OB,UOB,SDB,CNF and CMF. Typically, these statuses should have single values in Side0 and Side1 UniqueIds. So records not following expected behviour are removed
    meo_appended_data['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] = meo_appended_data.apply(lambda row: contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(row), axis=1, result_type="expand")
    meo_appended_data = meo_appended_data[~(meo_appended_data['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] == 'remove')]
else:
    meo_appended_data = pd.DataFrame()


days = meo_df['ViewData.Task Business Date'].value_counts().reset_index()
#                    date_gt_50 = list(days[days['ViewData.Task Business Date']>50]['index'])[0]
date_with_highest_frq = days.sort_values(by="ViewData.Task Business Date", ascending = False)['index'].iloc[0]
#                    date_from_to_extract_dmy = pd.to_datetime(date_gt_50)
date_from_to_extract_dmy = pd.to_datetime(date_with_highest_frq)
day = date_from_to_extract_dmy.day
mon = date_from_to_extract_dmy.month
yr = date_from_to_extract_dmy.year

Schonfeld_897_position_file_path = "\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\Files and Rules for Lookup\\Rules delivery from Ronson\\Schonfeld\\Schonfeld ML final\\57 Position Recon\\"
filename = '57 Position recon - '+ dmy_value_with_or_without_zero(param_dmy_value = mon) + dmy_value_with_or_without_zero(param_dmy_value = day) + str(yr)+ '.xlsx'
filename_pos_file = Schonfeld_897_position_file_path + filename

def read_passowrd_protected_pos_file_and_concat_to_single_pos_df(param_filepath, param_password, param_sheet_names_list, param_start_row_num, param_start_col_num, param_pos_cols_list):
    xl_app = win32com.client.Dispatch('Excel.Application')
    #pwd = getpass.getpass('Enter file password: ')
    xl_wb = xl_app.Workbooks.Open(param_filepath, False, True, None, param_password)
    xl_app.Visible = False
    df_sheet_list = []
    for sheet_name in param_sheet_names_list:
        print(sheet_name)
        xl_sh = xl_wb.Worksheets(sheet_name)

    # Get last row    
        row_num = param_start_row_num
        cell_val = ''
        while cell_val != None:
            cell_val = xl_sh.Cells(row_num, param_start_col_num).Value
            row_num = row_num + 1
        last_row = row_num - 2

    # Get last_column
        col_num = param_start_col_num
        cell_val = ''
        while cell_val != None:
            cell_val = xl_sh.Cells(param_start_row_num, col_num).Value
            col_num = col_num + 1
        last_col = col_num - 2
    
        content = xl_sh.Range(xl_sh.Cells(param_start_row_num, param_start_col_num), xl_sh.Cells(last_row, last_col)).Value
        df_sheet = pd.DataFrame(list(content[1:]), columns=content[0])
        df_sheet = df_sheet[param_pos_cols_list]
        df_sheet_list.append(df_sheet)
    pos_df = pd.concat(df_sheet_list)
    return(pos_df)

pos = read_passowrd_protected_pos_file_and_concat_to_single_pos_df(param_filepath = filename_pos_file, 
                                                                   param_password = Schonfeld_897_position_file_password, 
                                                                   param_sheet_names_list = [Schonfeld_897_position_file_first_sheet_name, Schonfeld_897_position_file_second_sheet_name], 
                                                                   param_start_row_num = Schonfeld_897_position_file_first_sheet_origin_row_number, 
                                                                   param_start_col_num = Schonfeld_897_position_file_first_sheet_origin_col_number,
																   param_pos_cols_list = Schonfeld_897_position_file_columns_list)

os.chdir('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) + '\\output_files')
#uni2 = pd.read_csv('Lombard/249/ReconDB.HST_RecData_249_01_10.csv')

#Change made by Rohit on 09-12-2020 to make dynamic directories
# base dir
base_dir = os.getcwd()       

# create dynamic name with date as folder
base_dir = os.path.join(base_dir + '\\Setup_' + setup_code)
# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir):
    os.makedirs(base_dir)
os.chdir(base_dir)

recon_done_for_dates_folder_names = [name for name in os.listdir(".") if os.path.isdir(name)]

def get_date_subfolder_suffix(param_date, param_subfolder_list):
	greatest_element_number_suffix = 0
	for element in param_subfolder_list:
		if(param_date in element):
			if(element.split('_')[-1].isnumeric() == True):
				if(int(element.split('_')[-1]) > greatest_element_number_suffix):
					greatest_element_number_suffix = int(element.split('_')[-1])
				else:
					greatest_element_number_suffix = greatest_element_number_suffix
			else:
				greatest_element_number_suffix = greatest_element_number_suffix
		else:
			greatest_element_number_suffix = greatest_element_number_suffix
	
	return(greatest_element_number_suffix + 1)

suffix_for_BD_folder = get_date_subfolder_suffix(param_date = date_to_analyze_ymd_format, param_subfolder_list = recon_done_for_dates_folder_names)
base_dir = os.path.join(base_dir + '\\BD_of_' + str(date_i) + '_' + str(suffix_for_BD_folder))
if not os.path.exists(base_dir):
    os.makedirs(base_dir)

os.chdir(base_dir)

df1 = meo_appended_data.copy()

dff = meo_df.copy()
#days = df1['ViewData.Task Business Date'].value_counts().reset_index()
#date = list(days[days['ViewData.Task Business Date']>50]['index'])[0]
#
#
#date1 = pd.to_datetime(date)
#
#day = date1.day
#mon = date1.month
#yr = date1.year

frames = [df1,dff]

df1 = pd.concat(frames)

df1 = df1.reset_index()
df1 = df1.drop('index', axis = 1)

output_col = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','Custodian Account','Currency','Ticker1','Net Amount Difference1','Description','Settle Date','Trade Date','ViewData.Age','predicted action','predicted category','predicted status','predicted comment','ViewData.Task Business Date','ViewData.Task ID','ViewData.Source Combination Code']

#imp_col = ['ViewData.Task Business Date', 'ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Mapped Custodian Account', 'ViewData.Currency','ViewData.Ticker', 'ViewData.Settle Date','ViewData.Trade Date','ViewData.Description','ViewData.Net Amount Difference']

#df2 = df1[imp_col]

#df2.to_csv('Schonfiled combined file for accumulation inspection.csv')


# #### Examination of Df1 for duplication
# ### Start of operation

# #### Standardization of ticker of both position file

rem_word = ['comdty','index','indx','elec']
def tickerclean(x):
    if ((x!= None) & (type(x)== str)):
        x = x.lower()
        x1 = x.split()
        k = []
        for item in x1:
            if item not in rem_word:
                k.append(item)
        return ' '.join(k)

# Change made by Rohit as per Abhijeet on 07-04-2021. Position file was changed, and following columns were not found with this error : "['Local Price Diff', 'Price dif %', 'Custodian Account', 'Quantity Diff', 'Local MV Diff', 'Strategy'] not in index"
#Below table was given by Ronson after talking to User about column mappings
'''
Column	           Status	        Changed Header	                 Moved to 
Fund	           No Change	      NA	                         Column A
Custodian Account  Header Changed	Geneva Account	                 Column B
Investment ID	   No Change	      NA	                         Column I
Strategy	       Header Changed	Geneva Strategy	                 Column H
Security Type	   No Change	      NA	                         Column D
Currency	       No Change	      NA	                         Column F
Description	       No Change	      NA	                         Column J
Quantity Diff	   Header Changed	SSA vs PB Qty Difference	     Column O
Local Price Diff   Header Changed	SSA vs PB Price Difference	     Column Y
Price dif %	       Column Removed	Column Removed	                 NA
Local MV Diff	   Header Changed	SSA vs PB Local MV Difference	 Column AG
'''

#Removed Strategy column as it was not used in code		
#Removed Price dif % column as it was not used in code		

pos = pos[['Fund',
		    
#		   'Custodian Account', -> changed to 'Geneva Account'
           'Geneva Account',
		   
		   'Investment ID',

#           'Strategy', 

		   'Security Type', 
		   'Currency', 
		   'Description', 

#		   'Quantity Diff', -> changed to 'SSA vs PB Qty Difference'
		   'SSA vs PB Qty Difference',
		   
#		   'Local Price Diff', -> changed to 'SSA vs PB Price Difference'
		   'SSA vs PB Price Difference',
		   
#		   'Price dif %',

#		   'Local MV Diff'-> changed to 'SSA vs PB Local MV Difference'
		   'SSA vs PB Local MV Difference'
		   
		   ]]

pos = pos.rename(columns = {'Geneva Account' : 'Custodian Account',
							'SSA vs PB Qty Difference' : 'Quantity Diff',
							'SSA vs PB Price Difference' : 'Local Price Diff', 
							'SSA vs PB Local MV Difference' : 'Local MV Diff'
							})

pos = pos.rename(columns = {'Description':'Pos_Desc',
							'Security Type':'Pos_security',
							
#		   'Quantity Diff', -> changed to 'SSA vs PB Qty Difference'
							'Quantity Diff':'pos_qnt_diff',
							
							'Investment ID':'Ticker'})

pos = pos[~pos['Ticker'].isna()]
pos = pos[~pos['Ticker'].isnull()]

pos['Ticker1'] = pos['Ticker'].apply(lambda x : tickerclean(x) )

pos.drop('Ticker', axis =1 , inplace = True)

# #### Accumulated files standardization

df1 = df1.rename(columns = {'ViewData.Mapped Custodian Account':'Custodian Account',
                           'ViewData.Currency':'Currency',
                            'ViewData.Ticker':'Ticker',
                          'ViewData.Net Amount Difference':'Net Amount Difference',
                           'ViewData.Settle Date':'Settle Date',
                           'ViewData.Trade Date':'Trade Date',
                           'ViewData.Description':'Description'})

df1['Ticker1'] = df1['Ticker'].apply(lambda x : tickerclean(x))

df1.drop('Ticker', axis =1 , inplace = True)

# #### Accumulated files standardization
dff = dff.rename(columns = {'ViewData.Mapped Custodian Account':'Custodian Account',
                           'ViewData.Currency':'Currency',
                            'ViewData.Ticker':'Ticker',
                          'ViewData.Net Amount Difference':'Net Amount Difference',
                           'ViewData.Settle Date':'Settle Date',
                           'ViewData.Trade Date':'Trade Date',
                           'ViewData.Description':'Description'})

dff['Ticker1'] = dff['Ticker'].apply(lambda x : tickerclean(x))

dff.drop('Ticker', axis =1 , inplace = True)

#df1 = pd.merge(df1, pos, on = ['Custodian Account','Currency','Ticker1'], how = 'left')

#df1 = df1.reset_index()
#df1 =df1.drop('index',1)

# ### Cleaning of Variables and feature Engineering

# - cleaning of dates

df2= df1.copy()


df2['Settle Date'] = pd.to_datetime(df2['Settle Date'])
df2['Trade Date'] = pd.to_datetime(df2['Trade Date'])
#df2['ViewData.Task Business Date'] = pd.to_datetime(df2['ViewData.Task Business Date'])

#df2['ViewData.Task Business Date1'] = df2['ViewData.Task Business Date'].dt.date

df2['Settle Date1'] = df2['Settle Date'].dt.date
df2['Trade Date1'] = df2['Trade Date'].dt.date

dff['Settle Date'] = pd.to_datetime(dff['Settle Date'])
dff['Trade Date'] = pd.to_datetime(dff['Trade Date'])

dff['Settle Date1'] = dff['Settle Date'].dt.date
dff['Trade Date1'] = dff['Trade Date'].dt.date

# - Taking care of the amount variable
def amountcleaning(x):
    if type(x)==str:
        
        if x.startswith('('):
            x1 = x.strip('(,)')
            x2 = x1.split(',')
            x3 = []
            for item in x2:
                if item!=',':
                    x3.append(item)
            x4 = ''.join(x3)
                
            x4 = 0 - float(x4)
            return x4
        else:
            x2 = x.split(',')
            x3 = []
            for item in x2:
                if item!=',':
                    x3.append(item)
            x4 = ''.join(x3)
            return float(x4)
    else:
        return 1234567

df2['Net Amount Difference1'] = df2['Net Amount Difference']
abhijeet_comment_viteos_folder_filepath = 'D:\\ViteosModel\\Abhijeet - Comment\\'
conv = pd.read_csv(abhijeet_comment_viteos_folder_filepath + 'currency conversion.csv')

df2 = pd.merge(df2, conv, on = 'Currency', how = 'left')

df2['Net Amount Difference1'] = round(df2['Net Amount Difference1']* df2['conversion'],2)

# #### Conversion of amount of cash file

dff['Net Amount Difference1'] = dff['Net Amount Difference']
dff = pd.merge(dff, conv, on = 'Currency', how = 'left')
dff['Net Amount Difference1'] = round(dff['Net Amount Difference1']* dff['conversion'],2)

#df2['Net Amount Difference1'] = df2['Net Amount Difference'].apply(lambda x : amountcleaning(x))
#df2['Local MV Diff'] = df2['Local MV Diff'].apply(lambda x :amountcleaning(x) )

#df2['Local MV Diff'] = round(df2['Local MV Diff']* df2['conversion'],2)

# #### Conversion of file due to normal issues

def remove_mark_notolerance(y,k,a):
    if ((abs(k)<5.1) &  (y==0) &  (a>1)):
        return 1
     
    else:
        return 0


'''
dummyk = dff.groupby(['Custodian Account','Currency','Ticker1'])['Net Amount Difference1'].apply(list).reset_index()
dummyk['Cash Standing'] = dummyk['Net Amount Difference1'].apply(lambda x : sum(x))
dummyk['len_cash'] = dummyk['Net Amount Difference1'].apply(lambda x :len(x))

dummyk = dummyk[['Custodian Account', 'Currency', 'Ticker1','Cash Standing','len_cash']]
dummyk = pd.merge(dummyk, pos, on = ['Custodian Account','Currency','Ticker1'], how = 'left')

dummyk['pos_qnt_diff'] = dummyk['pos_qnt_diff'].fillna(0)

dummyk['remove_mark_fin'] = dummyk.apply(lambda x :remove_mark_notolerance(x['pos_qnt_diff'] ,x['Cash Standing'],x['len_cash']),axis = 1)

dummyk1 = dummyk[['Custodian Account', 'Currency', 'Ticker1','remove_mark_fin']]
dff = pd.merge(dff,dummyk1, on = ['Custodian Account','Currency','Ticker1'], how = 'left')

dfn = dff[dff['remove_mark_fin']==1]
'''

dff['Custodian Account'] = dff['Custodian Account'].apply(lambda x : str(x).strip())
dff['Currency'] = dff['Currency'].apply(lambda x : str(x).strip())
dff['Ticker1'] = dff['Ticker1'].apply(lambda x : str(x).strip())

dummyk = dff.groupby(['Custodian Account','Currency','Ticker1'])['Net Amount Difference1'].apply(list).reset_index()
dummyk['Cash Standing'] = dummyk['Net Amount Difference1'].apply(lambda x : sum(x))
dummyk['len_cash'] = dummyk['Net Amount Difference1'].apply(lambda x :len(x))
                    
dummyk = dummyk[['Custodian Account', 'Currency', 'Ticker1','Cash Standing','len_cash']]
                    
pos['Custodian Account'] = pos['Custodian Account'].apply(lambda x : str(x).strip())
pos['Currency'] = pos['Currency'].apply(lambda x : str(x).strip())
pos['Ticker1'] = pos['Ticker1'].apply(lambda x : str(x).strip())
                    
dummyk = pd.merge(dummyk, pos, on = ['Custodian Account','Currency','Ticker1'], how = 'left')
                    
dummyk['pos_qnt_diff'] = dummyk['pos_qnt_diff'].fillna(0.0)
                    
dummyk['remove_mark_fin'] = dummyk.apply(lambda x :remove_mark_notolerance(x['pos_qnt_diff'] ,x['Cash Standing'],x['len_cash']),axis = 1)
                    
dummyk1 = dummyk[['Custodian Account', 'Currency', 'Ticker1','remove_mark_fin']]
dff = pd.merge(dff,dummyk1, on = ['Custodian Account','Currency','Ticker1'], how = 'left')
                    
dfn = dff[dff['remove_mark_fin']==1]
 


if dfn.shape[0]!=0:
    dfn['predicted action'] = 'pair'
    dfn['predicted category'] = 'match'
    dfn['predicted comment'] = 'Match'
    dfn['predicted status'] = dfn['ViewData.Status'].apply(lambda x : 'UMF' if x =='SMB' else 'UCB')
    dfn1 = dfn[output_col]
#    dfn1.to_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p01.csv')
    dfn1.to_csv('Schonfield ' + setup_code +' Meo Prediction P0.csv')
    dff = dff[dff['remove_mark_fin'] !=1]
    
else:
    
    dff = dff.copy()

dff.drop('remove_mark_fin', axis =1, inplace = True)
# #### Sub sum before accumulation

def subSumnotol(numbers,total):
    length = len(numbers)
    
    if length <20:
        
        for index,number in enumerate(numbers):
            if np.isclose(number, total, atol=0.005).any():
                return [number]
                print(34567)
            subset = subSumnotol(numbers[index+1:],total-number)
            if subset:
                #print(12345)
                return [number] + subset
        return []
    else:
        return numbers


def remove_mark_notol(x,y,k):
    if ((x>1) & (x<20)):
        if ((abs(k)<0.005) &  (y==0)):
            return 1
        else:
            return 0
        
    else:
        return 0

def remover(x,y,z):
    if x==1:
        if y in z:
            return 1
        else:
            return 0
    else:
        return 0

def date_remover_notol(x,y,z):
    if isinstance(x,list):
        for item in x:
            if ((item>y) & (item<z)):
                return 0
            else:
                return 1
    else:
        return 1

dummy = dff.groupby(['Custodian Account','Currency','Ticker1'])['Net Amount Difference1'].apply(list).reset_index()
dummy['Net Amount Difference1'] = dummy['Net Amount Difference1'].apply(lambda x : list(set(x)))
dummy['len_amount'] = dummy['Net Amount Difference1'].apply(lambda x : len(x))
dummy['zero_list'] = dummy['Net Amount Difference1'].apply(lambda x : subSumnotol(x,0))
dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
dummy['pos_qnt_diff'] = dummy['pos_qnt_diff'].fillna(0)

dummy['remove_mark'] = dummy.apply(lambda x :remove_mark_notol(x['zero_list_len'],x['pos_qnt_diff'],x['zero_list_sum']),axis = 1)
dummy = dummy[['Custodian Account', 'Currency', 'Ticker1', 'zero_list',  'diff_len', 'remove_mark']]
dff = pd.merge(dff, dummy, on = ['Custodian Account','Currency','Ticker1'], how = 'left')

dff['remove_mark_fin'] = dff.apply(lambda x : remover(x['remove_mark'],x['Net Amount Difference1'],x['zero_list']),axis =1)

dfn = dff[dff['remove_mark_fin']==1]
dfn1 = dff[dff['remove_mark_fin']!=1]

if dfn.shape[0] !=0:
    dfn['Trade Date for extracting day'] = pd.to_datetime(dfn['Trade Date'])
    dfn1['Trade Date for extracting day'] = pd.to_datetime(dfn1['Trade Date'])
    dfn['day'] = dfn['Trade Date for extracting day'].dt.day
    dfn1['day'] = dfn1['Trade Date for extracting day'].dt.day

#    dfn['day'] = dfn['Trade Date'].dt.day
#    dfn1['day'] = dfn1['Trade Date'].dt.day

    agg = dfn.groupby(['Custodian Account','Currency','Ticker1'])['day'].apply(list).reset_index()
    agg = agg.rename(columns = {'day':'zero_day'})
    agg1 = dfn1.groupby(['Custodian Account','Currency','Ticker1'])['day'].apply(list).reset_index()
    agg2 = pd.merge(agg, agg1, on = ['Custodian Account','Currency','Ticker1'], how = 'left' )
    agg2['zero_max'] = agg2['zero_day'].apply(lambda x : max(x))
    agg2['zero_min'] = agg2['zero_day'].apply(lambda x : min(x))
    agg2['remove_mark_new'] = agg2.apply(lambda x : date_remover_notol(x['zero_day'],x['zero_min'],x['zero_max']), axis = 1)
    agg2 = agg2[['Custodian Account','Currency','Ticker1','remove_mark_new']]
    dfn = pd.merge(dfn, agg2, on = ['Custodian Account','Currency','Ticker1'], how = 'left' )
    if dfn[dfn['remove_mark_new']==1].shape[0]!=0:
        df4_new = dfn[dfn['remove_mark_new']==1]
        df4_new['predicted action'] = 'pair'
        df4_new['predicted category'] = 'match'
        df4_new['predicted comment'] = 'Match'
        df4_new['predicted status'] = df4_new['ViewData.Status'].apply(lambda x : 'UMF' if x =='SMB' else 'UCB')
        df4_new1 = df4_new[output_col]
#        df4_new1.to_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p02.csv')
        df4_new1.to_csv('Schonfield ' + setup_code +' Meo Prediction P1.csv')
        df5_new = dfn[dfn['remove_mark_new']!=1]
                               
        df5_new.drop(['remove_mark_new'], axis = 1, inplace = True)
        
        dff = pd.concat([df5_new,dfn1], axis = 0)
        dff = dff.reset_index()
        dff.drop(['index','day'], axis = 1, inplace = True)
    else:
        dff = dff.copy()
#         df2 = df2.copy()
else:
    
    dff = dff.copy()
#     df2 = df2.copy()


# #### Conversion of file using accumulation

dff.drop(['zero_list','diff_len','remove_mark','remove_mark_fin'], axis = 1, inplace = True)

dummyk = df2.groupby(['Custodian Account','Currency','Ticker1'])['Net Amount Difference1'].apply(list).reset_index()

dummyk['Cash Standing'] = dummyk['Net Amount Difference1'].apply(lambda x : sum(set(x)))

def remove_mark_pre(y,k):
    
    if ((abs(k)<5.0) &  (y==0)):
            return 1
     
    else:
        return 0

dummyk = dummyk.rename(columns = {'Net Amount Difference1': 'list_amount'})
dummyk['len_amt'] = dummyk['list_amount'].apply(lambda x : len(x))

dummyk = dummyk[['Custodian Account', 'Currency', 'Ticker1','Cash Standing','len_amt','list_amount']]

dummyk = pd.merge(dummyk, pos, on = ['Custodian Account','Currency','Ticker1'], how = 'left')

dummyk['Local MV Diff'] = dummyk['Local MV Diff'].apply(lambda x :amountcleaning(x) )

dummyk['pos_qnt_diff'] = dummyk['pos_qnt_diff'].fillna(0)

dummyk['remove_mark_fin'] = dummyk.apply(lambda x :remove_mark_pre(x['pos_qnt_diff'] ,x['Cash Standing']),axis = 1)

dummyk1 = dummyk[['Custodian Account', 'Currency', 'Ticker1','remove_mark_fin']]

dff = pd.merge(dff,dummyk1, on = ['Custodian Account','Currency','Ticker1'], how = 'left')

df2 = pd.merge(df2,dummyk1, on = ['Custodian Account','Currency','Ticker1'], how = 'left')

#df2['remove_mark_fin'] = df2.apply(lambda x :remove_mark_pre(x['pos_qnt_diff'] ,x['Cash Standing']),axis = 1)

#df2['cash difference'] = df2.apply(lambda x : x['Cash Standing']+x['Local MV Diff'] if x['Local MV Diff']!=1234567.0 else 1234567, axis =1)

dff['zero_list'] = 'am'
dff['diff_len'] = 10
dff['remove_mark'] = 1
dff['remove_mark_new'] = 10
dff['day'] = 10

dfn = dff[dff['remove_mark_fin'] ==1]

if dfn.shape[0]!=0:
    dfn['predicted action'] = 'pair'
    dfn['predicted category'] = 'match'
    dfn['predicted comment'] = 'Match'
    dfn['predicted status'] = dfn['ViewData.Status'].apply(lambda x : 'UMF' if x =='SMB' else 'UCB')
    dfn1 = dfn[output_col]
#    dfn1.to_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p0.csv')
    dfn1.to_csv('Schonfield ' + setup_code +' Meo Prediction P2.csv')
    dff = dff[dff['remove_mark_fin'] !=1]
    df2 = df2[df2['remove_mark_fin'] !=1]
else:
    df2 = df2.copy()
    dff = dff.copy()

dff = dff.drop(['zero_list', 'diff_len', 'remove_mark',
       'remove_mark_fin','remove_mark_new','day'], axis = 1)
dff = dff.reset_index()
dff.drop('index', axis = 1,inplace = True)

df2 = df2.drop(['conversion','remove_mark_fin'], axis =1 )

# ### Elimination of Matched

# #### Absolute Zero : No tolerance

dummy = df2.groupby(['Custodian Account','Currency','Ticker1'])['Net Amount Difference1'].apply(list).reset_index()

dummy['Net Amount Difference1'] = dummy['Net Amount Difference1'].apply(lambda x : list(set(x)))

def subSum(numbers,total):
    length = len(numbers)
    
    if length <20:
      
        for index,number in enumerate(numbers):
            if np.isclose(number, total, atol=5).any():
                return [number]
                print(34567)
            subset = subSum(numbers[index+1:],total-number)
            if subset:
                #print(12345)
                return [number] + subset
        return []
    else:
        return numbers

dummy['len_amount'] = dummy['Net Amount Difference1'].apply(lambda x : len(x))

dummy['zero_list'] = dummy['Net Amount Difference1'].apply(lambda x : subSum(x,0))

dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))

dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']

dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))

#dummy['remain_amt'] = dummy.apply(lambda x : list(set(x['Net Amount Difference1'])-set(x['zero_list'])) if x['remove_mark'] == 1 else "AA", axis =1)

dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
dummy['pos_qnt_diff'] = dummy['pos_qnt_diff'].fillna(0)

def remove_mark(x,y,z,k):
   
  
   if ((x>1) & (x<20)):
       if ((abs(k)<0.5) &  (y==0)):
           return 1
       elif ((abs(k)<5.1) & (z==0) & (y==0)):
           return 1
       elif ((abs(k)<5.1) & (z<3) & (y==0)):
           return 1
       else:
           return 0
   else:
       return 0

dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['pos_qnt_diff'],x['diff_len'],x['zero_list_sum']),axis = 1)

dummy = dummy[['Custodian Account', 'Currency', 'Ticker1', 'zero_list',  'diff_len', 'remove_mark']]

df3 = pd.merge(dff, dummy, on = ['Custodian Account','Currency','Ticker1'], how = 'left')
df2 = pd.merge(df2, dummy, on = ['Custodian Account','Currency','Ticker1'], how = 'left')

def remover(x,y,z):
    if x==1:
        if y in z:
            return 1
        else:
            return 0
    else:
        return 0
   
df3['remove_mark_fin'] = df3.apply(lambda x : remover(x['remove_mark'],x['Net Amount Difference1'],x['zero_list']),axis =1)

df2['remove_mark_fin'] = df2.apply(lambda x : remover(x['remove_mark'],x['Net Amount Difference1'],x['zero_list']),axis =1)

df4 = df3[df3['remove_mark_fin']==1]
df5 = df3[df3['remove_mark_fin']!=1]

def date_remover(x,y,z):
    if isinstance(x,list):
        for item in x:
            if ((item>y) & (item<z)):
                return 0
            else:
                return 1
    else:
        return 1

if df4.shape[0] !=0:
    df4['Trade Date for extracting day'] = pd.to_datetime(df4['Trade Date'])
    df5['Trade Date for extracting day'] = pd.to_datetime(df5['Trade Date'])
    df4['day'] = df4['Trade Date for extracting day'].dt.day
    df5['day'] = df5['Trade Date for extracting day'].dt.day
#    df4['day'] = df4['Trade Date'].dt.day
#    df5['day'] = df5['Trade Date'].dt.day
    agg = df4.groupby(['Custodian Account','Currency','Ticker1'])['day'].apply(list).reset_index()
    agg = agg.rename(columns = {'day':'zero_day'})
    agg1 = df5.groupby(['Custodian Account','Currency','Ticker1'])['day'].apply(list).reset_index()
    agg2 = pd.merge(agg, agg1, on = ['Custodian Account','Currency','Ticker1'], how = 'left' )
    agg2['zero_max'] = agg2['zero_day'].apply(lambda x : max(x))
    agg2['zero_min'] = agg2['zero_day'].apply(lambda x : min(x))
    agg2['remove_mark_new'] = agg2.apply(lambda x : date_remover(x['zero_day'],x['zero_min'],x['zero_max']), axis = 1)
    agg2 = agg2[['Custodian Account','Currency','Ticker1','remove_mark_new']]
    df4 = pd.merge(df4, agg2, on = ['Custodian Account','Currency','Ticker1'], how = 'left' )
    if df4[df4['remove_mark_new']==1].shape[0]!=0:
        df4_new = df4[df4['remove_mark_new']==1]
        df4_new['predicted action'] = 'pair'
        df4_new['predicted category'] = 'match'
        df4_new['predicted comment'] = 'Match'
        df4_new['predicted status'] = df4['ViewData.Status'].apply(lambda x : 'UMF' if x =='SMB' else 'UCB')
        df4_new1 = df4_new[output_col]
#        df4_new1.to_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p1.csv')
        df4_new1.to_csv('Schonfield ' + setup_code +' Meo Prediction P3.csv')
        df5_new = df4[df4['remove_mark_new']!=1]
#        df2 = df2[df2['remove_mark_new']!=1]
        df5_new.drop(['remove_mark_new'], axis = 1, inplace = True)
        
        df5 = pd.concat([df5_new,df5], axis = 0)
        df5 = df5.reset_index()
        df5.drop(['index','day'], axis = 1, inplace = True)
    else:
        df5 = df3.copy()
        df2 = df2.copy()
else:
    
    df5 = df5.copy()
    df2 = df2.copy()
    

# #### Without Ticker

df5.drop(['zero_list', 'diff_len', 'remove_mark',
       'remove_mark_fin'], axis =1 , inplace = True)

#df2.drop(['remove_mark_fin'], axis =1 , inplace = True)
#
#dummy = df2.groupby(['Custodian Account','Currency'])['Net Amount Difference1'].apply(list).reset_index()
#
#dummy['Net Amount Difference1'] = dummy['Net Amount Difference1'].apply(lambda x : list(set(x)))
#
#dummy['len_amount'] = dummy['Net Amount Difference1'].apply(lambda x : len(x))
#
#dummy['zero_list'] = dummy['Net Amount Difference1'].apply(lambda x : subSum(x,0))
#
#dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
#
#dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
#
#dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
#
#def remove_mark2(x,z,k):
#    if ((x>1) & (x<20)):
#        
#        if ((abs(k)<5.1) & (z==0)):
#            return 1
#       
#        else:
#            return 0
#    else:
#        return 0
#
#dummy['remove_mark'] = dummy.apply(lambda x :remove_mark2(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)
#
#dummy = dummy[['Custodian Account', 'Currency', 'zero_list',  'diff_len', 'remove_mark']]
#
#df3 = pd.merge(df5, dummy, on = ['Custodian Account','Currency'], how = 'left')
#
#df3['remove_mark_fin'] = df3.apply(lambda x : remover(x['remove_mark'],x['Net Amount Difference1'],x['zero_list']),axis =1)
#
#df4 = df3[df3['remove_mark_fin']==1]
#df5 = df3[df3['remove_mark_fin']!=1]
#
#if df4.shape[0] !=0:
#    df4['day'] = df4['Trade Date'].dt.day
#    df5['day'] = df5['Trade Date'].dt.day
#    agg = df4.groupby(['Custodian Account','Currency'])['day'].apply(list).reset_index()
#    agg = agg.rename(columns = {'day':'zero_day'})
#    agg1 = df5.groupby(['Custodian Account','Currency'])['day'].apply(list).reset_index()
#    agg2 = pd.merge(agg, agg1, on = ['Custodian Account','Currency'], how = 'left' )
#    agg2['zero_max'] = agg2['zero_day'].apply(lambda x : max(x))
#    agg2['zero_min'] = agg2['zero_day'].apply(lambda x : min(x))
#    agg2['remove_mark_new'] = agg2.apply(lambda x : date_remover(x['zero_day'],x['zero_min'],x['zero_max']), axis = 1)
#    agg2 = agg2[['Custodian Account','Currency','remove_mark_new']]
#    df4 = pd.merge(df4, agg2, on = ['Custodian Account','Currency'], how = 'left' )
#    if df4[df4['remove_mark_new']==1].shape[0]!=0:
#        df4_new = df4[df4['remove_mark_new']==1]
#        df4_new['predicted action'] = 'pair'
#        df4_new['predicted category'] = 'match'
#        df4_new['predicted comment'] = 'Match'
#        df4_new['predicted status'] = df4['ViewData.Status'].apply(lambda x : 'UMF' if x =='SMB' else 'UCB')
#        df4_new1 = df4_new[output_col]
#        df4_new1.to_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p2.csv')
#        df5_new = df4[df4['remove_mark_new']!=1]
#        df5_new.drop(['remove_mark_new'], axis = 1, inplace = True)
#        df5 = pd.concat([df5_new,df5], axis = 0)
#        df5 = df5.reset_index()
#        df5.drop(['index','day'], axis = 1, inplace = True)
#    else:
#        df5 = df3.copy()
#else:
#    
#    df5 = df5.copy()
#
## ### Cases of Currency conversion - New Stuff
#
#df5.drop(['zero_list', 'diff_len', 'remove_mark','remove_mark_fin'], axis =1 , inplace = True)

# ### Final Commenting Section
df5 = pd.merge(df5, pos , on = ['Custodian Account', 'Currency', 'Ticker1'],how = 'left')

df5['Local MV Diff'] = df5['Local MV Diff'].apply(lambda x :amountcleaning(x) )

dummyk2 = df5.groupby(['Custodian Account', 'Currency', 'Ticker1'])['Net Amount Difference1'].sum().reset_index()

dummyk2 = dummyk2.rename(columns = {'Net Amount Difference1':'Cash Standing'})

df5 = pd.merge(df5, dummyk2 , on = ['Custodian Account', 'Currency', 'Ticker1'], how = 'left')

df5['cash difference'] = df5.apply(lambda x : x['Cash Standing']+x['Local MV Diff'] if x['Local MV Diff']!=1234567.0 else 1234567, axis =1)

df6 = df5[df5['ViewData.InternalComment2'].isna()]
df7 = df5[~df5['ViewData.InternalComment2'].isna()]

if df7.shape[0]!=0:
    df7['predicted action'] = 'No-pair'
    df7['predicted category'] = 'OB'
    df7['predicted comment'] = df7['ViewData.InternalComment2']
    df7['predicted status'] = df7['ViewData.Status']
else:
    df6 = df6.copy()

df6['Local Price Diff'] = df6['Local Price Diff'].fillna(0)

def commentschon(pos,amt,accamt, pbamt,cash_diff):
    if ((pos==0) & (amt==0)):
        if((cash_diff<6.0) & (cash_diff>-6.0)):
            com = 'MV Swing'
        else:
            
            com = 'Commission & fee difference,SFA to advise'
    elif(pos!=0):
        if ((accamt==None) | (math.isnan(accamt))):
            com = 'GVA missing the trade, viteos to check and book'
        else:
            com = 'PB to report missing the trade.'
    else:
        com = 'MV Swing'
        
    return com
df6.rename(columns = {'ViewData.B-P Net Amount':'ViewData.Cust Net Amount'}, inplace = True)            
df6['predicted comment'] = df6.apply(lambda x : commentschon(x['pos_qnt_diff'],x['Local Price Diff'],x['ViewData.Accounting Net Amount'],x['ViewData.Cust Net Amount'],x['cash difference']),axis = 1)

df6['predicted status'] = df6['ViewData.Status']
df6['predicted action'] = 'No-pair'
df6['predicted category'] = 'OB'

df6 = df6[output_col]
df7 = df7[output_col]

#df7.to_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p5.csv')
#df6.to_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p6.csv')
df7.to_csv('Schonfield ' + setup_code +' Meo Prediction P4.csv')
df6.to_csv('Schonfield ' + setup_code +' Meo Prediction P5.csv')

# #### Combining all the files

#a0 = pd.read_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p0.csv')
#a = pd.read_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p1.csv')
#b = pd.read_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p2.csv')
##c = pd.read_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p3.csv')
##d = pd.read_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p4.csv')
#e = pd.read_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p5.csv')
#f = pd.read_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p6.csv')
#
#
## In[110]:
#
#
#frames = [a0,a,b,e,f]
#
#final_df = pd.concat(frames)
##### While renaming ticker1 will be viewdata.ticker and 
#
#final_df = final_df.rename(columns = {'Custodian Account':'ViewData.Mapped Custodian Account',
#                           'Currency':'ViewData.Currency',
#                            'Ticker1':'ViewData.Ticker',
#                          'Net Amount Difference1':'ViewData.Net Amount Difference',
#                          'Settle Date': 'ViewData.Settle Date',
#                           'Trade Date':'ViewData.Trade Date',
#                           'Description':'ViewData.Description'})
#
#final_df.to_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction final.csv')
def check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(fun_only_filename_with_csv_list):
    frames = []
    current_folder = os.getcwd()
    full_filepath_list = [current_folder + '\\' + x for x in fun_only_filename_with_csv_list]
    for full_filepath in full_filepath_list :
        if os.path.isfile(full_filepath) == True:
            frames.append(pd.read_csv(full_filepath))
    return pd.concat(frames)


# #### Combining all the files
final_df_filename_list = ['Schonfield 897 Meo Prediction P' + str(x) + '.csv' for x in [0,1,2,3,4,5,6]]
final_df = check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(final_df_filename_list)
final_df = final_df.reset_index()
final_df = final_df.drop('index', axis = 1)

#### While renaming ticker1 will be viewdata.ticker and 

# final_df = final_df.rename(columns = {'Custodian Account':'ViewData.Mapped Custodian Account',
#                            'Currency':'ViewData.Currency',
#                             'Ticker1':'ViewData.Ticker',
#                           'Net Amount Difference1':'ViewData.Net Amount Difference',
#                           'Settle Date': 'ViewData.Settle Date',
#                            'Trade Date':'ViewData.Trade Date',
#                            'Description':'ViewData.Description'})

#### While renaming ticker1 will be viewdata.ticker and 

final_df = final_df.rename(columns = {'Custodian Account':'ViewData.Mapped Custodian Account',
                           'Currency':'ViewData.Currency',
                            'Ticker1':'ViewData.Ticker',
                          'Net Amount Difference1':'ViewData.Net Amount Difference',
                          'Settle Date': 'ViewData.Settle Date',
                           'Trade Date':'ViewData.Trade Date',
                           'Description':'ViewData.Description',
                           'ViewData.Task Business Date' : 'BusinessDate',
                           'ViewData.BreakID' : 'BreakID',
#                           'final_BreakID_to_insert_in_db' : 'BreakID',
#                           'Predicted_BreakID_to_insert_in_db' : 'Final_predicted_break',
#                           'final_Status_to_insert_in_db' : 'Predicted_Status',
#                           'Predicted_action_to_insert_in_db' : 'Predicted_action',
                           
                           'ViewData.Source Combination Code' : 'SourceCombinationCode',
                           'ViewData.Task ID' : 'TaskID',
                           'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                           'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                           'predicted action' : 'Predicted_action',
                           'predicted comment' : 'PredictedComment',
                           'predicted category' : 'PredictedCategory',
                           'predicted status' : 'Predicted_Status'})
#As per Abhijeet, Ticker will be Ticker1 and Net Amount Difference will be Net Amount Difference1 now

final_df.to_csv('Schonfield concatenated file for all predictions.csv')

final_df['BreakID'] = final_df['BreakID'].astype(str)

final_df['BusinessDate'] = pd.to_datetime(final_df['BusinessDate'])
final_df['BusinessDate'] = final_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df['BusinessDate'] = pd.to_datetime(final_df['BusinessDate'])

final_df['Final_predicted_break'] = ''
final_df['Final_predicted_break'] = final_df['Final_predicted_break'].astype(str)
final_df['BreakID'] = final_df['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df['Final_predicted_break'] = final_df['Final_predicted_break'].map(lambda x:x.lstrip('[').rstrip(']'))

final_df['ML_flag'] = 'ML'

final_df['SetupID'] = setup_code

final_df['probability_No_pair'] = ''
final_df['probability_UMB'] = ''
final_df['probability_UMR'] = ''
final_df['probability_UMT'] = ''


    
    
cols_for_database = ['BreakID', 'BusinessDate', 'Final_predicted_break', 'ML_flag',
       'Predicted_Status', 'Predicted_action', 'SetupID',
       'SourceCombinationCode', 'TaskID', 'probability_No_pair',
       'probability_UMB', 'probability_UMR', 'probability_UMT',
       'Side1_UniqueIds', 'PredictedComment', 'PredictedCategory',
       'Side0_UniqueIds']    
    
    

final_df_2 = final_df[cols_for_database]

#    Added more checks for database

final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].astype(str)
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].astype(str)
final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].astype(str)
final_df_2['probability_UMT'] = final_df_2['probability_UMT'].astype(str)
final_df_2['probability_UMR'] = final_df_2['probability_UMR'].astype(str)
final_df_2['probability_UMB'] = final_df_2['probability_UMB'].astype(str)
final_df_2['probability_No_pair'] = final_df_2['probability_No_pair'].astype(str)

final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2['BreakID'] = final_df_2['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].map(lambda x:x.lstrip('[').rstrip(']'))

cols_to_remove_newline_char_from = ['Side1_UniqueIds','Side0_UniqueIds','BreakID','Final_predicted_break']
final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].replace('\\n','',regex = True)
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].replace('\\n','',regex = True)
final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].replace('BB','')
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].replace('AA','')
final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].replace('None','')
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].replace('None','')
final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].replace('nan','')
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].replace('nan','')

final_df_2['probability_No_pair'] = final_df_2['probability_No_pair'].replace('None','')
final_df_2['probability_No_pair'] = final_df_2['probability_No_pair'].replace('nan','')

final_df_2['probability_UMT'] = final_df_2['probability_UMT'].replace('None','')
final_df_2['probability_UMT'] = final_df_2['probability_UMT'].replace('nan','')

final_df_2['probability_UMR'] = final_df_2['probability_UMR'].replace('None','')
final_df_2['probability_UMR'] = final_df_2['probability_UMR'].replace('nan','')

final_df_2['probability_UMB'] = final_df_2['probability_UMB'].replace('None','')
final_df_2['probability_UMB'] = final_df_2['probability_UMB'].replace('nan','')

final_df_2['BreakID'] = final_df_2['BreakID'].replace('\\n','',regex = True)

final_df_2['PredictedComment'] = final_df_2['PredictedComment'].astype(str)
final_df_2['PredictedComment'] = final_df_2['PredictedComment'].replace('nan','')
final_df_2['PredictedComment'] = final_df_2['PredictedComment'].replace('None','')
final_df_2['PredictedComment'] = final_df_2['PredictedComment'].replace('NA','')

final_df_2['BreakID'] = final_df_2['BreakID'].replace('\.0','',regex = True)

#final_df_2_UMR_record_with_predicted_comment = final_df_2[((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMR'))]
#if(final_df_2_UMR_record_with_predicted_comment.shape[0] != 0):
#    final_df_2 = final_df_2[~((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMR'))]
#
#    Side0_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment = final_df_2_UMR_record_with_predicted_comment['Side0_UniqueIds']
#    Side1_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment = final_df_2_UMR_record_with_predicted_comment['Side1_UniqueIds']
#
#    final_df_2 = final_df_2[~((final_df_2['Side0_UniqueIds'].isin(Side0_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#    final_df_2 = final_df_2[~((final_df_2['Side1_UniqueIds'].isin(Side1_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#
#    final_df_2_UMR_record_with_predicted_comment['PredictedComment'] = ''       
#    final_df_2 = final_df_2.append(final_df_2_UMR_record_with_predicted_comment)
#
#
#final_df_2_UMT_record_with_predicted_comment = final_df_2[((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMT'))]
#if(final_df_2_UMT_record_with_predicted_comment.shape[0] != 0):
#    final_df_2 = final_df_2[~((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMT'))]
#    
#    Side0_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment = final_df_2_UMT_record_with_predicted_comment['Side0_UniqueIds']
#    Side1_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment = final_df_2_UMT_record_with_predicted_comment['Side1_UniqueIds']
#    
#    final_df_2 = final_df_2[~((final_df_2['Side0_UniqueIds'].isin(Side0_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#    final_df_2 = final_df_2[~((final_df_2['Side1_UniqueIds'].isin(Side1_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#
#    final_df_2_UMT_record_with_predicted_comment['PredictedComment'] = ''
#    final_df_2 = final_df_2.append(final_df_2_UMT_record_with_predicted_comment)
#
#final_df_2['BusinessDate'] = final_df_2.apply(lambda x: get_BusinessDate_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
#final_df_2['TaskID'] = final_df_2.apply(lambda x: get_TaskID_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
#final_df_2['SourceCombinationCode'] = final_df_2.apply(lambda x: get_SourceCombinationCode_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)


final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].astype(str)
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x:x.lstrip('[').rstrip(']'))

final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])


final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)

final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(float)
final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(np.int64)

final_df_2[['SourceCombinationCode']] = final_df_2[['SourceCombinationCode']].astype(str)
final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].map(lambda x:x.lstrip('\'').rstrip('\''))

final_df_2[['Predicted_Status']] = final_df_2[['Predicted_Status']].astype(str)
final_df_2[['Predicted_action']] = final_df_2[['Predicted_action']].astype(str)

def apply_ui_action_column_897(fun_row):
    if(fun_row['ML_flag'] == 'Not_Covered_by_ML'):
        ActionType = 'No Prediction'
    else:
        if((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'No Action'
        elif((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'COMMENT'
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'No Action'
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'COMMENT'

        elif((fun_row['Predicted_Status'] == 'UCB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'CLOSE'
        elif((fun_row['Predicted_Status'] == 'UCB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'CLOSE WITH COMMENT'
        elif((fun_row['Predicted_Status'] == 'UMF') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'FORCE MATCH'
        elif((fun_row['Predicted_Status'] == 'UMF') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'FORCE MATCH WITH COMMENT'
        else:
            ActionType = 'Status not covered'
    return ActionType

def apply_ui_action_column_897_final(fun_row):
    if(fun_row['ML_flag'] == 'Not_Covered_by_ML'):
        ActionType = 'No Prediction'
    else:
        if((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'No Action'
        elif((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'COMMENT'
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'No Action'
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'COMMENT'
        elif((fun_row['Predicted_Status'] == 'UCB') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'CLOSE'
        elif((fun_row['Predicted_Status'] == 'UMF') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'FORCE MATCH'
        else:
            ActionType = 'Status not covered'
    return ActionType

def apply_ui_action_column_897_final_ActionTypeCode(fun_row):
    if(fun_row['ML_flag'] == 'Not_Covered_by_ML'):
        ActionTypeCode = 7
    else:
        if((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 6
        elif((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 3
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 6
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 3
        elif((fun_row['Predicted_Status'] == 'UCB') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 2
        elif((fun_row['Predicted_Status'] == 'UMF') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 5
        else:
            ActionTypeCode = 0
    return ActionTypeCode

final_df_2['ActionType'] = final_df_2.apply(lambda row : apply_ui_action_column_897_final(fun_row = row), axis = 1,result_type="expand")            

final_df_2['ActionTypeCode'] = final_df_2.apply(lambda row : apply_ui_action_column_897_final_ActionTypeCode(fun_row = row), axis = 1,result_type="expand")            
final_df_2['ActionTypeCode'] = final_df_2['ActionTypeCode'].astype(int)
final_df_2.loc[final_df_2['Predicted_Status']=='UMF','PredictedComment'] = ''
final_df_2.loc[final_df_2['Predicted_Status']=='UCB','PredictedComment'] = ''

final_df_2['BreakID'] = final_df_2['BreakID'].replace(', ',',',regex = True)
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace(', ',',',regex = True)
final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].astype(str)

final_df_2['ReconSetupName'] = 'Schonfeld Cash - 57'
final_df_2['ClientShortCode'] = 'Schonfeld'

from datetime import datetime,date,timedelta
today = date.today()
today_Y_m_d = today.strftime("%Y-%m-%d")

final_df_2['CreatedDate'] = today_Y_m_d
final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])
final_df_2['CreatedDate'] = final_df_2['CreatedDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])

filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '_for_10_day_predictions.csv'
final_df_2.to_csv(filepaths_final_df_2)

filepaths_meo_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\meo_df_setup_' + setup_code + '_date_' + str(date_i) + '_for_10_day_predictions.csv'
meo_df.to_csv(filepaths_meo_df)

#data_dict = final_table_to_write.to_dict("records")
#coll_1_for_writing_prediction_data = ReconDB_ML_137_server_for_writing['MLPrediction_Cash']
coll_1_for_writing_prediction_data = ReconDB_ML_137_server_for_writing['MLPrediction_' + setup_code]
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing = ReconDB_ML_Testing_137_server['MLPrediction_Cash']

coll_1_for_writing_prediction_data.remove({ "BusinessDate": getDateTimeFromISO8601String(date_to_analyze_ymd_iso_00_00_format), "SetupID": int(setup_code)})
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.remove({ "BusinessDate": getDateTimeFromISO8601String(date_to_analyze_ymd_iso_00_00_format), "SetupID": int(setup_code)})

data_dict = final_df_2.to_dict("records_final")
coll_1_for_writing_prediction_data.insert_many(data_dict) 

data_dict_for_testingdb = final_df_2.to_dict("records_final_for_testingdb")
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.insert_many(data_dict_for_testingdb) 


print(setup_code)
print(date_i)
'''    
os.chdir('D:\\ViteosModel')

#1. Drop the following collections
## i. Tasks in ReconDB_ML_Testing db
Tasks_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['Tasks']
Tasks_ReconDB_ML_Testing_137_server.drop()

## ii. RecData_<setup_code> in ReconDB_ML_Testing db
RecData_Setup_Code_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code]
RecData_Setup_Code_ReconDB_ML_Testing_137_server.drop()

## iii. RecData_<setup_code>_Audit in ReconDB_ML_Testing_db
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code + '_Audit']
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server.drop()

#2. Fixing Tasks collection in 137 Testing so as to reflect Tasks corresponding to client,setup_code and date
query_for_Task_collection = ReconDB_ML_137_server['Tasks'].find({ "BusinessDate": getDateTimeFromISO8601String(penultimate_date_to_analyze_ymd_iso_18_30_format), 
                                                                  "ReconSetupCode": setup_code },ViteosMongoDB_Query.task_projection_all_columns)
list_of_dicts_query_for_Task_collection = list(query_for_Task_collection)
list_instance_ids = [list_of_dicts_query_for_Task_collection[i].get('InstanceID',{}) for i in range(0,len(list_of_dicts_query_for_Task_collection))]
list_version = [list_of_dicts_query_for_Task_collection[i].get('_version',{}) for i in range(0,len(list_of_dicts_query_for_Task_collection))]

#Update all values of '_version' to 2 as the comparison report requires the value of '_version' column to be greater than 0. We have randomly chosen 2 to keep uniformity.     
for d in list_of_dicts_query_for_Task_collection:
    d.update((k, int(2)) for k, v in d.items() if k == '_version')

Tasks_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['Tasks']
Tasks_ReconDB_ML_Testing_137_server.insert_many(list_of_dicts_query_for_Task_collection) 

RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code + '_Audit']
RecData_Setup_Code_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code]

print(list_instance_ids)
#3. Getting AUA data from prod and dumping results into RecData_<setup_code>_Audit collection in 137 Testing corresponding to client,setup_code and date

query_for_AUA_data = ReconDB_prod_server['HST_RecData_' + setup_code].find({ 'TaskInstanceID': { '$in': list_instance_ids }, 'ViewData' : { '$ne': None}, 'LastPerformedAction' : { '$ne' : 31 }, 'MatchStatus': { '$nin' : [1,2,18,19,20,21] } },ViteosMongoDB_Query.data_projection_all_columns)

list_of_dicts_query_for_AUA_data = list(query_for_AUA_data)
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code + '_Audit']
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server.insert_many(list_of_dicts_query_for_AUA_data)

#3. Getting AUA data from prod and dumping results into RecData_<setup_code>_Audit collection in 137 Testing corresponding to client,setup_code and date

query_for_MEO_data = ReconDB_ML_137_server_for_reading['RecData_' + setup_code + '_Historic'].find({ 'TaskInstanceID': { '$in': list_instance_ids }},ViteosMongoDB_Query.data_projection_all_columns)

list_of_dicts_query_for_MEO_data = list(query_for_MEO_data)
RecData_Setup_Code_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code]
RecData_Setup_Code_ReconDB_ML_Testing_137_server.insert_many(list_of_dicts_query_for_MEO_data)

#meo_df = json_normalize(list_of_dicts_query_for_MEO_data)
#meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
#meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
#meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + str(client) + '/meo_df_setup_' + str(setup_code) +'_date_' + date_to_analyze_ymd_format + '.csv'
#meo_df.to_csv(meo_filename)

aua_df = json_normalize(list_of_dicts_query_for_AUA_data)
#aua_df = aua_df.loc[:,aua_df.columns.str.startswith('ViewData')]
aua_df['ViewData.Task Business Date'] = aua_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
aua_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + str(client) + '/aua_df_setup_' + str(setup_code) +'_date_' + date_to_analyze_ymd_format + '.csv'
aua_df.to_csv(aua_filename)
 
count_docs = ReconDB_ML_Testing_137_server['RecData_' + setup_code].count_documents({'PredictionInfo': {'$exists' : True}})
print(count_docs)
    
#Local MV difference
#Pos_qnt_diff
#Local Price Difference
#
#All three in string for Abhijeet, he converts them to float
#Uses Amount cleaning function for Local MV Diff. I dont have to use amount cleaning function if mine is already in integer. Have to check this.
#Other 2, he uses as string
#
#For pos qnt diff, its used as string in functions as well. Will have to change the functions for them to be able to use pos_qnt_diff as float instead of string. This will make the change as '0.00' in function to be replaced by 0 as it will be used as float
#
#Local Price Diff

'''
