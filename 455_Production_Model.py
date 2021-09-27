# -*- coding: utf-8 -*-
"""
Created on Mon May 24 06:10:52 2021

@author: riteshkumar.patra
"""
 
client = 'Lombard'

setup = '455'
setup_code = '455'

ReconSetupName = 'Lombard Caceis Cash Recon'

import logging
import timeit

start = timeit.default_timer()
import memory_profiler

import numpy as np
import pandas as pd
# from imblearn.over_sampling import SMOTE


import os

os.chdir('D:\\ViteosModel2.0\\')

import pickle
import datetime as dt
import sys
from datetime import datetime,date,timedelta
from pandas.io.json import json_normalize
import dateutil.parser
import re
from dateutil.parser import parse
pd.set_option('mode.chained_assignment', None)

from src.ViteosMongoDB_Production import ViteosMongoDB_Class as mngdb
import json
from src.RabbitMQ_Production import RabbitMQ_Class as rb_mq
import pika
from src.ViteosLogger_Production import ViteosLogger_Class
import argparse
from pandas.io.json import json_normalize
import dateutil.parser
from difflib import SequenceMatcher
import pprint
import json
from pandas import merge, DataFrame

now = datetime.now()
current_date_and_time = now.strftime('%d-%b-%Y_%I%p-%M-%S')

Logger_obj = ViteosLogger_Class()
log_folder = os.getcwd() + '\\logs\\'
model_files_folder = os.getcwd() + '\\data\\model_files\\' + str(setup_code) + '\\'

try:
    with open(os.getcwd() + '\\data\\Production_Model_parameters.json') as f:
        parameters_dict = json.load(f)

    def assign_PB_Acct_side_row_apply(fun_row):
        if ((fun_row['flag_side1'] >= 1) & (fun_row['flag_side0'] == 0)):
            PB_or_Acct_Side_Value = 'PB_Side'
        elif ((fun_row['flag_side1'] == 0) & (fun_row['flag_side0'] >= 1)):
            PB_or_Acct_Side_Value = 'Acct_Side'
        else:
            PB_or_Acct_Side_Value = 'Non OB'

        return (PB_or_Acct_Side_Value)

    def assign_geneva_or_nongeneva_based_on_PB_Acct_side(fun_row):
        if (fun_row['PB_or_Acct_Side'] == 'PB_side'):
            return 'geneva'
        else:
            return 'non_geneva'
        
    def make_mm_dd_yyyy_from_string_date_format_yyyy_mm_dd(param_str_date):
        return(param_str_date[5:7] + '-' + param_str_date[8:10] + '-'+ param_str_date[0:4])

    def getDateTimeFromISO8601String(s):
        d = dateutil.parser.parse(s)
        return d
    
    def clean_mapping(item):
        item1 = item.split()
        
        
        ttype = []
        
        
        for x in item1:
            ttype1 = []
            for key, value in clean_map_dict.items():
                
        
            
            
                if x in key:
                    a = value
                    ttype1.append(a)
               
            if len(ttype1)==0:
                ttype1.append(x)
            ttype = ttype + ttype1
            
        return ' '.join(ttype)
    
    def is_num(item):
        try:
            float(item)
            return True
        except ValueError:
            return False
    
    def is_date_format(item):
        try:
            parse(item, fuzzy=False)
            return True
        
        except ValueError:
            return False
        
    def date_edge_cases(item):
        if len(item) == 5 and item[2] =='/' and is_num(item[:2]) and is_num(item[3:]):
            return True
        return False
    
    def comb_clean(x):
        k = []
        for item in x.split():
            if ((is_num(item)==False) and (is_date_format(item)==False) and (date_edge_cases(item)==False)):
                k.append(item)
        return ' '.join(k)
    
    def descclean(com,cat_list):
        cat_all1 = []
        list1 = cat_list
        m = 0
        if (type(com) == str):
            com = com.lower()
            com1 =  re.split("[,/. \-!?:]+", com)
            
            for item in list1:
                if (type(item) == str):
                    item = item.lower()
                    item1 = item.split(' ')
                    lst3 = [value for value in item1 if value in com1] 
                    if len(lst3) == len(item1):
                        cat_all1.append(item)
                        m = m+1
                
                    else:
                        m = m
                else:
                        m = 0
        else:
            m = 0
        
        if m >0 :
            return list(set(cat_all1))
        else:
            if ((type(com)==str)):
                if (len(com1)<4):
                    if ((len(com1)==1) & com1[0].startswith('20')== True):
                        return 'swap id'
                    else:
                        return com
                else:
                    return 'NA'
            else:
                return 'NA'
    
    def currcln(x):
        if (type(x)==list):
            return x
        else:
            if x == 'NA':
                return "NA"
            elif (('dollar' in x) | ('dollars' in x )):
                return 'dollar'
            elif (('pound' in x) | ('pounds' in x)):
                return 'pound'
            elif ('yen' in x):
                return 'yen'
            elif ('euro' in x) :
                return 'euro'
            else:
                return x
    
    def desccat(x):
        if isinstance(x, list):
            
            if 'equity swap settlement' in x:
                return 'swap settlement'
            elif 'collateral transfer' in x:
                return 'collateral transfer'
            elif 'dividend' in x:
                return 'dividend'
            elif (('loan' in x) & ('option' in x)):
                return 'option loan'
            
            elif (('interest' in x) & ('corp' in x) ):
                return 'corp loan'
            elif (('interest' in x) & ('loan' in x) ):
                return 'interest'
            else:
                return x[0]
        else:
            if x == 'db_int':
                return 'interest'
            else:
                return x

    def desc_cat(x):
        if x == 'NA':
            return 'NA'
        elif 'froc' in x:
            return 'froc'
        elif 'facturation fees' in x:
            return 'facturation fees'
        elif 'external management fees' in x:
            return 'emf'
        elif 'external distribution fees' in x:
            return 'edf'
        elif 'transfer of' in x:
            return 'transfer'
        else:
            if type(x)== str:
                return x
            else:
                return x[0]
    
    #def comgen(x,y,z,k):
    #    if x == 'geneva':
    #        
    #        com = str(k) + ' ' + str(y) + ' ' + str(z)
    #    else:
    #        com = "Integrata" + ' ' + str(y) + ' ' + str(z)
    #        
    #    return com
    
    def comgen(x,y,z,k,m,a,b,c,d):
        trade_ttype = ['buy','sell','sell short','cover short','spot fx','forward','forward fx','spotfx','forwardfx']
        pos_break = ['settlement amount , no pos break']
        x = x.lower()
        d = d.lower()
    
        if('aum' in d):
            y = 'booked the AUM plug for SD'
        elif('net sub/red' in d):
            y = 'booked the Net Sub/Red for SD'	
        elif('miscellaneous' in d):
            y = 'booked the Miscellaneous for SD'
        elif('subscription' in d):
            y = 'booked the subscription for SD'
        else:
            y = y
    
        if m in trade_ttype:
            if x != 'geneva':
                if ((a!= 0) & (b!= 0)):
                    com = k + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '. Integrata yet to book' 
                elif  ((a==0) & (b!=0)):
                    com = k + ' ' +y + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) +  '. Integrata yet to book'
                elif  ((a!=0) & (b==0)):
                    com = k + ' ' +y + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) +  '. Integrata yet to book'
                else:
                    com = k + ' ' +y + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) +  '. Integrata yet to book'
            else:
                if ((a!= 0) & (b!= 0)):
                    com = 'Integrata' + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '.'+ k+ ' yet to book' 
                elif  ((a==0) & (b!=0)):
                    com = 'Integrata' + ' ' +y + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ k+ ' yet to book'
                elif  ((a!=0) & (b==0)):
                    com = 'Integrata' + ' ' +y + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ k+ ' yet to book'
                else:
                    com = 'Integrata' + ' ' +y + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ k+ ' yet to book'
        
        elif m in pos_break:
            
            if ((a!= 0) & (b!= 0)):
                com ='No position break, Geneva to reflect jpm trade on ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b)
            elif  ((a==0) & (b!=0)):
                com = 'No position break, Geneva to reflect jpm trade on ' + str(z) + " " + 'for quantity' +' ' + str(b) 
            elif  ((a!=0) & (b==0)):
                com = 'No position break, Geneva to reflect jpm trade on ' + str(z) + " " + 'for price' +' ' + str(a) 
            else:
                com = 'No position break, Geneva to reflect jpm trade on ' + str(z)
            
        else:
            if x != 'geneva':
            
                com = k + ' ' +y + ' ' + str(z) + ". Integrata yet to book"
            else:
                com = 'Integrata' + ' ' +y + ' ' + str(z)+ '.' + k + ' yet to book.'
            
        return com
    	
    
    
    def catcln1(cat,df):
        ret = []
        if (type(cat)==list):
            
            if 'equity swap settlement' in cat:
                ret.append('equity swap settlement')
            #return 'equity swap settlement'
            elif 'equity swap' in cat:
                ret.append('equity swap settlement')
            #return 'equity swap settlement'
            elif 'swap settlement' in cat:
                ret.append('equity swap settlement')
            #return 'equity swap settlement'
            elif 'swap unwind' in cat:
                ret.append('swap unwind')
            #return 'swap unwind'
            else:
                for item in cat:
                
                    a = df[df['Pairing']==item]['replace'].values[0]
                    if a not in ret:
                        ret.append(a)
            return list(set(ret))
          
        else:
            return cat
    
    # Function1
    def subSum(numbers,total):
        length = len(numbers)
        
        
        
        if length <16:
          
                
          
            
            for index,number in enumerate(numbers):
                if np.isclose(number, total, atol=0.05).any():
                    return [number]
                    print(34567)
                subset = subSum(numbers[index+1:],total-number)
                if subset:
                    #print(12345)
                    return [number] + subset
            return []
        else:
            return numbers
    
    def subSum1(numbers,total):
        length = len(numbers)
        if length <16:
            for index,number in enumerate(numbers):
                if np.isclose(number, total, atol=5.0).any():
                    return [number]
                    print(34567)
                subset = subSum(numbers[index+1:],total-number)
                if subset:
                    #print(12345)
                    return [number] + subset
            return []
        else:
            return numbers
    
    def amt_marker(x,y,z):
        if type(y)==list:
            if ((x in y) & ((z<16) & (z>=2))) :
                return 1
            else:
                return 0
        else:
            return 0
    
    def remove_mark(x,z,k):
        
       
        if ((x>1) & (x<16)):
            if ((k<6.0)):
                return 1
    #         elif ((k==0.0) & (z!=0)):
    #             return 1
            else:
                return 0
        else:
            return 0
    
    def mtm(x,y):
        if ((pd.isnull(x)==False) & (pd.isnull(y)==False)):
            y1 = y.split(',')
            x1 = x.split(',')
            return pd.Series([len(x1),len(y1)], index=['len_0', 'len_1'])
        elif ((pd.isnull(x)==False) & (pd.isnull(y)==True)):
            x1 = x.split(',')
            
            return pd.Series([len(x1),0], index=['len_0', 'len_1'])
        elif ((pd.isnull(x)==True) & (pd.isnull(y)==False)):
            y1 = y.split(',')
            
            return pd.Series([0,len(y1)], index=['len_0', 'len_1'])
            
        else:
            
            
            return pd.Series([0,0], index=['len_0', 'len_1'])
    
    def mtm_mark(x,y):
        if ((x>1) &(y>1)):
            return 'MTM'
        elif((x==1) &(y==1)):
            return 'OTO'
        elif((x>1) &(y==1)):
            return 'MTO'
        elif((x==1) &(y>1)):
            return 'OTM'
        else:
            return 'OB'
    
    def make_dict(row):
        keys_l = str(row['Keys']).lower()
        keys_s = keys_l.split(', ')
        keys = tuple(keys_s)
        return keys
    
    def common_matching_engine_single1(df,filters,columns_to_output, amount_column, dummy_filter,serial_num, param_client = client, param_setup_code = setup_code):
        dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
        dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
        dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
        dummy['sel_mark'] = dummy.apply(lambda x : 1 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 0, axis =1 )
        if dummy[dummy['sel_mark']==1].shape[0]!=0:
        
            dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
        
            dummy['zero_sum_list'] = dummy[amount_column].apply(lambda x : len(set(x)))
            #dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
    
            #dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
            #dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
        
        #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
            final_cols = filters + dummy_filter
        
            dummy['remove_mark'] = dummy.apply(lambda x :1 if ((x['len_amount'])/(x['zero_sum_list'])==2) else 0, axis =1)
    
            dummy = dummy[final_cols]
            df3 = pd.merge(df, dummy, on = filters, how = 'left')
            #print(df3.columns)
        
            df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
        #print(df4.columns)
        
       
            if df4.shape[0]!=0:
    #            k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
    #            k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
    #            k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
    #            k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
    #            k = pd.merge(k1, k2 , on = filters, how = 'left')
    #            k = pd.merge(k, k3 , on = filters, how = 'left')
    #            k = pd.merge(k, k4 , on = filters, how = 'left')
            
                df4['predicted status'] = 'UCB'
                df4['predicted action'] = 'Close'
                df4['predicted category'] = 'Close'
                df4['predicted comment'] = ''
                df4 = df4[columns_to_output]
            
            
                string_name = 'p'+str(serial_num) + ' ' + str(item)
    #            filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
                filename = str(param_client) +' setup ' + str(param_setup_code) + ' ' + string_name + '.csv'
    #            filename = 'Lombard setup 249 ' + string_name + '.csv'
    
                df4.to_csv(filename)
            
                df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
                #print(df5.columns)
                df5.drop(dummy_filter, axis = 1, inplace = True)
                df5 = df5.reset_index()
                df5.drop('index', axis = 1, inplace = True)
            else:    
                df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
                df5.drop(dummy_filter, axis = 1, inplace = True)
                df5 = df5.reset_index()
                df5.drop('index', axis = 1, inplace = True)
                print(df5.columns)
        else:
            df5 = df.copy()
            
        return df5
    ### Change 1:
    
    def common_matching_engine_single2(df,filters,columns_to_output, amount_column, dummy_filter,serial_num,param_client = client, param_setup_code = setup_code):
        dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
        dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
        dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
        dummy['sel_mark'] = dummy.apply(lambda x : 1 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 0, axis =1 )
        if dummy[dummy['sel_mark']==1].shape[0]!=0:
        
            dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
        
            dummy['zero_list'] = dummy[amount_column].apply(lambda x : subSum(x,0))
            dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
    
            dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
            dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
        
        #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
            final_cols = filters + dummy_filter
        
            dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)
    
            dummy = dummy[final_cols]
            df3 = pd.merge(df, dummy, on = filters, how = 'left')
            #print(df3.columns)
        
            df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
        #print(df4.columns)
        
       
            if df4.shape[0]!=0:
    #             k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
    #             k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
    #             k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
    #             k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
    #             k = pd.merge(k1, k2 , on = filters, how = 'left')
    #             k = pd.merge(k, k3 , on = filters, how = 'left')
    #             k = pd.merge(k, k4 , on = filters, how = 'left')
            
                df4['predicted status'] = 'UCB'
                df4['predicted action'] = 'Close'
                df4['predicted category'] = 'Close'
                df4['predicted comment'] = ''
                df4 = df4[columns_to_output]
            
            
                string_name = 'p'+str(serial_num) + ' ' + str(item)
    #            filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
                filename = str(param_client) +' setup ' + str(param_setup_code) + ' ' + string_name + '.csv'
                df4.to_csv(filename)
            
                df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
                #print(df5.columns)
                df5.drop(dummy_filter, axis = 1, inplace = True)
                df5 = df5.reset_index()
                df5.drop('index', axis = 1, inplace = True)
            else:    
                df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
                df5.drop(dummy_filter, axis = 1, inplace = True)
                df5 = df5.reset_index()
                df5.drop('index', axis = 1, inplace = True)
                print(df5.columns)
        else:
            df5 = df.copy()
            
        return df5
    
    
    # In[2285]:
    
    
    def common_matching_engine_single3(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
        dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
        dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
        dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
        dummy['sel_mark'] = dummy.apply(lambda x : 1 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 0, axis =1 )
        if dummy[dummy['sel_mark']==1].shape[0]!=0:
        
            dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
        
            dummy['zero_list'] = dummy[amount_column].apply(lambda x : subSum1(x,0))
            dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
    
            dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
            dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
        
        #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
            final_cols = filters + dummy_filter
        
            dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)
    
            dummy = dummy[final_cols]
            df3 = pd.merge(df, dummy, on = filters, how = 'left')
            #print(df3.columns)
        
            df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
        #print(df4.columns)
        
       
            if df4.shape[0]!=0:
    #             k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
    #             k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
    #             k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
    #             k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
    #             k = pd.merge(k1, k2 , on = filters, how = 'left')
    #             k = pd.merge(k, k3 , on = filters, how = 'left')
    #             k = pd.merge(k, k4 , on = filters, how = 'left')
            
                df4['predicted status'] = 'pair'
                df4['predicted action'] = 'UMR'
                df4['predicted category'] = 'match'
                df4['predicted comment'] = 'match'
                df4 = df4[columns_to_output]
            
            
                string_name = 'p'+str(serial_num)
                filename = 'Schonfield/pair result/setup 85 ' + string_name + '.csv'
                df4.to_csv(filename)
            
                df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
                #print(df5.columns)
                df5.drop(dummy_filter, axis = 1, inplace = True)
                df5 = df5.reset_index()
                df5.drop('index', axis = 1, inplace = True)
            else:    
                df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
                df5.drop(dummy_filter, axis = 1, inplace = True)
                df5 = df5.reset_index()
                df5.drop('index', axis = 1, inplace = True)
                print(df5.columns)
        else:
            df5 = df.copy()
            
        return df5
    
    
    # In[2402]:
    
    
    
    
    def common_matching_engine_double1(df,filters,columns_to_output, amount_column, dummy_filter,serial_num, param_client = client, param_setup_code = setup_code):
        dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
        dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
        dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
        dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
        if dummy[dummy['sel_mark']==1].shape[0]!=0:
        
            dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
        
            dummy['zero_sum_list'] = dummy[amount_column].apply(lambda x : sum(x))
            #dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
    
            #dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
            #dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
        
        #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
            final_cols = filters + dummy_filter
        
            dummy['remove_mark'] = dummy.apply(lambda x :1 if ((abs(x['zero_sum_list'])<=0.05) & (x['len_amount']>1)) else 0, axis =1)
    
            dummy = dummy[final_cols]
            df3 = pd.merge(df, dummy, on = filters, how = 'left')
            #print(df3.columns)
        
            df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
        #print(df4.columns)
        
       
            if df4.shape[0]!=0:
                k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
                k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
                k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
                k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
                k = pd.merge(k1, k2 , on = filters, how = 'left')
                k = pd.merge(k, k3 , on = filters, how = 'left')
                k = pd.merge(k, k4 , on = filters, how = 'left')
            
                k['predicted status'] = 'UMR'
                k['predicted action'] = 'UMR'
                k['predicted category'] = 'match'
                k['predicted comment'] = 'match'
                k = k[columns_to_output]
            
            
                string_name = 'p'+str(serial_num) + ' ' + str(item)
                filename = str(param_client) +' setup ' + str(param_setup_code) + ' ' + string_name + '.csv'
    #            filename = 'Lombard setup 249 ' + string_name + '.csv'
                k.to_csv(filename)
            
                df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
                #print(df5.columns)
                df5.drop(dummy_filter, axis = 1, inplace = True)
                df5 = df5.reset_index()
                df5.drop('index', axis = 1, inplace = True)
            else:    
                df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
                df5.drop(dummy_filter, axis = 1, inplace = True)
                df5 = df5.reset_index()
                df5.drop('index', axis = 1, inplace = True)
                print(df5.columns)
        else:
            df5 = df.copy()
            
        return df5
    
    def common_matching_engine_double2(df,filters,columns_to_output, amount_column, dummy_filter,serial_num, param_client =  client, param_setup_code = setup_code):
        dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
        dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
        dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
        dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
        if dummy[dummy['sel_mark']==1].shape[0]!=0:
        
            dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
        
            dummy['zero_list'] = dummy[amount_column].apply(lambda x : subSum(x,0))
            dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
    
            dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
            dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
        
        #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
            final_cols = filters + dummy_filter
        
            dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)
    
            dummy = dummy[final_cols]
            df3 = pd.merge(df, dummy, on = filters, how = 'left')
            #print(df3.columns)
        
            df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
        #print(df4.columns)
        
       
            if df4.shape[0]!=0:
                k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
                k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
                k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
                k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
                k = pd.merge(k1, k2 , on = filters, how = 'left')
                k = pd.merge(k, k3 , on = filters, how = 'left')
                k = pd.merge(k, k4 , on = filters, how = 'left')
            
                k['predicted status'] = 'pair'
                k['predicted action'] = 'UMR'
                k['predicted category'] = 'match'
                k['predicted comment'] = 'match'
                k = k[columns_to_output]
            
            
                string_name = 'p'+str(serial_num) + ' ' + str(item)
                filename = str(param_client) +' setup ' + str(param_setup_code) + ' ' + string_name + '.csv'
    #            filename = 'Lombard setup 249 ' + string_name + '.csv'
                k.to_csv(filename)
            
                df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
                #print(df5.columns)
                df5.drop(dummy_filter, axis = 1, inplace = True)
                df5 = df5.reset_index()
                df5.drop('index', axis = 1, inplace = True)
            else:    
                df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
                df5.drop(dummy_filter, axis = 1, inplace = True)
                df5 = df5.reset_index()
                df5.drop('index', axis = 1, inplace = True)
                print(df5.columns)
        else:
            df5 = df.copy()
            
        return df5
    
    
    
    def new_pf_mapping(x):
        if x=='GSIL':
            return 'GS'
        elif x == 'CITIGM':
            return 'CITI'
        elif x == 'JPMNA':
            return 'JPM'
        else:
            return x
    
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
    
    def apply_cancel_in_transaction_type(param_transaction_type_val):
    	if('cancel' in str(param_transaction_type_val).lower()):
    		return 1
    	else:
    		return 0
    vec_apply_cancel_in_transaction_type = np.vectorize(apply_cancel_in_transaction_type)
    
    def apply_closed_mark(param_col_val_to_extract_closed_mark_from, param_col_val_for_existing_closed_mark):
    	if(param_col_val_to_extract_closed_mark_from == 1):
    		return 1
    	else:
    		return param_col_val_for_existing_closed_mark
    	
    vec_apply_closed_mark = np.vectorize(apply_closed_mark)
    
    def apply_Forward_swap_mark(param_transaction_type_val, param_currency_val, param_swap_transaction_types_list):
    	if(str(param_transaction_type_val) in param_swap_transaction_types_list):
    		if(str(param_currency_val) not in ['USD','']):
    			return 1
    		else:
    			return 0
    	else:
    		return 0
    	
    vec_apply_Forward_swap_mark = np.vectorize(apply_Forward_swap_mark)
    # ### Reading of MEO files
    today = date.today()
    # filepath_to_read_ReconDF_from = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) + '\\ReconDF_messages\\ReconDF_setup_' + str(setup_code) + '_date_' + str(today) + '_' + str(200) + '.csv'
    # ReconDF = pd.read_csv(filepath_to_read_ReconDF_from)
    MongoDB_parameters_dict = parameters_dict.get('MongoDB_parameters_dict')
    MongoDB_parameters_for_reading_data_from_dict = MongoDB_parameters_dict.get(
        'MongoDB_parameters_for_reading_data_from')
    MongoDB_parameters_for_writing_data_to_dict = MongoDB_parameters_dict.get('MongoDB_parameters_for_writing_data_to')

    RabbitMQ_parameters_dict = parameters_dict.get('RabbitMQ_parameters_dict')
    RabbitMQ_parameters_for_ML2_to_publish_to_dict = RabbitMQ_parameters_dict.get(
        'RabbitMQ_parameters_for_ML2_to_publish_to')
    RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict = RabbitMQ_parameters_dict.get(
        'RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement')
    RabbitMQ_parameters_for_ML2_to_read_from_dict = RabbitMQ_parameters_dict.get(
        'RabbitMQ_parameters_for_ML2_to_read_from')

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

    mngdb_obj_for_writing = mngdb(
        param_without_ssh=MongoDB_parameters_for_writing_data_to_dict.get('without_ssh'),
        param_without_RabbitMQ_pipeline=MongoDB_parameters_for_writing_data_to_dict.get('without_RabbitMQ_pipeline'),
        param_SSH_HOST=MongoDB_parameters_for_writing_data_to_dict.get('SSH_HOST'),
        param_SSH_PORT=MongoDB_parameters_for_writing_data_to_dict.get('SSH_PORT'),
        param_SSH_USERNAME=MongoDB_parameters_for_writing_data_to_dict.get('SSH_USERNAME'),
        param_SSH_PASSWORD=MongoDB_parameters_for_writing_data_to_dict.get('SSH_PASSWORD'),
        param_MONGO_HOST=MongoDB_parameters_for_writing_data_to_dict.get('MONGO_HOST'),
        param_MONGO_PORT=MongoDB_parameters_for_writing_data_to_dict.get('MONGO_PORT'),
        param_MONGO_USERNAME=MongoDB_parameters_for_writing_data_to_dict.get('MONGO_USERNAME'),
        param_MONGO_PASSWORD=MongoDB_parameters_for_writing_data_to_dict.get('MONGO_PASSWORD'))

    mngdb_obj_for_reading.connect_with_or_without_ssh()
    mngdb_obj_for_writing.connect_with_or_without_ssh()

    db_for_reading_MEO_data = mngdb_obj_for_reading.client[MongoDB_parameters_for_reading_data_from_dict.get('db')]
    db_for_writing_MEO_data = mngdb_obj_for_writing.client[MongoDB_parameters_for_writing_data_to_dict.get('db')]

    rb_mq_obj_new_for_publish = rb_mq(
        param_RABBITMQ_QUEUEING_PROTOCOL=RabbitMQ_parameters_for_ML2_to_publish_to_dict.get(
            'RABBITMQ_QUEUEING_PROTOCOL'),
        param_RABBITMQ_USERNAME=RabbitMQ_parameters_for_ML2_to_publish_to_dict.get('RABBITMQ_USERNAME'),
        param_RABBITMQ_PASSWORD=RabbitMQ_parameters_for_ML2_to_publish_to_dict.get('RABBITMQ_PASSWORD'),
        param_RABBITMQ_HOST_IP=RabbitMQ_parameters_for_ML2_to_publish_to_dict.get('RABBITMQ_HOST_IP'),
        param_RABBITMQ_PORT=RabbitMQ_parameters_for_ML2_to_publish_to_dict.get('RABBITMQ_PORT'),
        param_RABBITMQ_VIRTUAL_HOST=RabbitMQ_parameters_for_ML2_to_publish_to_dict.get('RABBITMQ_VIRTUAL_HOST'),
        param_RABBITMQ_EXCHANGE=RabbitMQ_parameters_for_ML2_to_publish_to_dict.get('RABBITMQ_EXCHANGE'),
        param_RABBITMQ_QUEUE=RabbitMQ_parameters_for_ML2_to_publish_to_dict.get('RABBITMQ_QUEUE'),
        param_RABBITMQ_ROUTING_KEY=RabbitMQ_parameters_for_ML2_to_publish_to_dict.get('RABBITMQ_ROUTING_KEY'),
        param_test_message_publishing=RabbitMQ_parameters_for_ML2_to_publish_to_dict.get('test_message_publishing'),
        param_timeout=RabbitMQ_parameters_for_ML2_to_publish_to_dict.get('timeout'))

#    Logger_obj.log_to_file(param_filename=log_filepath, param_log_str='RabbitMQ Publish object created')

    rb_mq_obj_new_for_acknowledgement = rb_mq(
        param_RABBITMQ_QUEUEING_PROTOCOL=RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict.get(
            'RABBITMQ_QUEUEING_PROTOCOL'),
        param_RABBITMQ_USERNAME=RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict.get(
            'RABBITMQ_USERNAME'),
        param_RABBITMQ_PASSWORD=RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict.get(
            'RABBITMQ_PASSWORD'),
        param_RABBITMQ_HOST_IP=RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict.get(
            'RABBITMQ_HOST_IP'),
        param_RABBITMQ_PORT=RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict.get('RABBITMQ_PORT'),
        param_RABBITMQ_VIRTUAL_HOST=RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict.get(
            'RABBITMQ_VIRTUAL_HOST'),
        param_RABBITMQ_EXCHANGE=RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict.get(
            'RABBITMQ_EXCHANGE'),
        param_RABBITMQ_QUEUE=RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict.get('RABBITMQ_QUEUE'),
        param_RABBITMQ_ROUTING_KEY=RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict.get(
            'RABBITMQ_ROUTING_KEY'),
        param_test_message_publishing=RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict.get(
            'test_message_publishing'),
        param_timeout=RabbitMQ_parameters_for_ML2_to_publish_to_for_acknowledgement_dict.get('timeout'))

    while_loop_iterator = 0
    outer_while_loop_iterator = 0
    # while True:
    while outer_while_loop_iterator == 0:
        #    try:
        #      s2_out = subprocess.check_output([sys.executable, os.getcwd() + '\\ML2_RMQ_Receive_Production.py'])
        #    except Exception:
        #        data = None
        s2_out = sys.argv[1]
#        s2_out = '4551470464|Schonfeld Cash - 57|Cash|RecData_897|132120|Recon Run Completed|455|609a34b91e9c9c19c0cbc1e3'
        stout_list = s2_out.split("|")
        print(stout_list)
        if len(stout_list) > 1:
            outer_while_loop_iterator = outer_while_loop_iterator + 1
            while_loop_iterator = while_loop_iterator + 1

            smallerlist = [l.split(',') for l in ','.join(stout_list).split('\n')]
#            Logger_obj.log_to_file(param_filename=log_filepath, param_log_str='split happened')

            ReconDF = DataFrame.from_records(smallerlist)
#            Logger_obj.log_to_file(param_filename=log_filepath, param_log_str='ReconDF created')
            ReconDF = ReconDF.dropna(how='any', axis=0)
            #        ReconDF.columns = ['TaskID', 'csc','ReconPurpose','collection_meo','RequestId']
            ReconDF.columns = ['TaskID', 'csc', 'ReconPurpose', 'collection_meo', 'ProcessID', 'Completed_Status',
                               'Setup_Code', 'MongoDB_TaskID']

            ReconDF['TaskID'] = ReconDF['TaskID'].str.lstrip("b'")
            ReconDF['ProcessID'] = ReconDF['ProcessID'].str.replace(r"[^0-9]", " ")
            ReconDF['Setup_Code'] = ReconDF['Setup_Code'].str.rstrip("'\r")
            ReconDF['MongoDB_TaskID'] = ReconDF['MongoDB_TaskID'].str.rstrip("'\r")

#            Logger_obj.log_to_file(param_filename=log_filepath, param_log_str='ReconDF all columns created')
            print('ReconDF')
            print(ReconDF)

            for z in range(ReconDF.shape[0]):
                # for setup_code in setup_code_list:
#                Logger_obj.log_to_file(param_filename=log_filepath, param_log_str='Started for loop')

                TaskID_z = ReconDF['TaskID'].iloc[z]
                print('Starting predictions for ' + str(ReconSetupName) + ', setup_code = ' + str(setup_code) + ' and Task ID = ',
                      str(TaskID_z))
                print(setup_code)

                csc_z = ReconDF['csc'].iloc[z]
                ReconPurpose_z = ReconDF['ReconPurpose'].iloc[z]
                collection_meo_z = ReconDF['collection_meo'].iloc[z]
                ProcessID_z = ReconDF['ProcessID'].iloc[z]
                Completed_Status_z = ReconDF['Completed_Status'].iloc[z]
                Setup_Code_z = ReconDF['Setup_Code'].iloc[z]
                MongoDB_TaskID_z = ReconDF['MongoDB_TaskID'].iloc[z]
#                log_filename = 'log_datetime_' + str(current_date_and_time) + '.txt'
                log_filename = 'log' + '_' + str(TaskID_z) + '_' + str(Setup_Code_z) + '_' + current_date_and_time + '.txt'
#                error_filename = 'error_datetime_' + str(current_date_and_time) + '.txt'
                error_filename = 'error' + '_' + str(TaskID_z) + '_' + str(Setup_Code_z) + '_' + current_date_and_time + '.txt'
#                stdout_filename = 'stdout_datetime' +  str(current_date_and_time) + '.txt'
                stdout_filename = 'stdout' + '_' + str(TaskID_z) + '_' + str(Setup_Code_z) + '_' + current_date_and_time + '.txt'
                
                #log_filename = 'log_datetime_21-04-14_13-07-19.txt'
                log_filepath = log_folder + log_filename
                error_filepath = log_folder + error_filename
                stdout_filepath = log_folder + stdout_filename
                Logger_obj.log_to_file(param_filename=log_filepath,
                       param_log_str='Log started for datettime = ' + str(current_date_and_time))
                logging.basicConfig(filename=error_filepath, filemode='a')
                sys.stdout = open(stdout_filepath, 'w')
                print('stdout started')


                AckMessage_z = 'Prediction Message Received for : ' + str(TaskID_z)
                #            rb_mq_obj_new_for_acknowledgement.fun_publish_single_message(param_message_body = AckMessage_z)
                Logger_obj.log_to_file(param_filename=log_filepath, param_log_str=AckMessage_z)

                query_1_for_MEO_data = db_for_reading_MEO_data['RecData_' + setup_code].find({
                    "LastPerformedAction": 31,
                    "TaskInstanceID": int(TaskID_z),
                    "MatchStatus": {"$nin": [1, 2, 18, 19, 20, 21]},
                    "ViewData": {"$ne": None}
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

                list_of_dicts_query_result_1 = list(query_1_for_MEO_data)

                if (len(list_of_dicts_query_result_1) != 0):
                    meo_df = json_normalize(list_of_dicts_query_result_1)
                    meo_df = meo_df.loc[:, meo_df.columns.str.startswith(('ViewData', '_createdAt'))]
                    meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat)
                    meo_df.drop_duplicates(keep=False, inplace=True)
                    meo_df = normalize_bp_acct_col_names(fun_df=meo_df)

                    # Change added on 14-12-2020 to remove records with multiple values of Side0 and Side1 UniqueIds for statuses like OB,UOB,SDB,CNF and CMF. Typically, these statuses should have single values in Side0 and Side1 UniqueIds. So records not following expected behviour are removed
                    meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] = meo_df.apply(lambda row: contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(fun_row=row), axis=1, result_type="expand")
                    meo_df = meo_df[~(meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] == 'remove')]
                else:
                    meo_df = pd.DataFrame()

                print('meo size')
                print(meo_df.shape[0])
                Logger_obj.log_to_file(param_filename=log_filepath, param_log_str='meo created')

                if (meo_df.shape[0] == 0):

                    Message_z = str(
                        TaskID_z) + '|' + csc_z + '|' + ReconPurpose_z + '|' + collection_meo_z + '|' + ProcessID_z + '|' + 'FAILURE' + '|' + Setup_Code_z + '|' + MongoDB_TaskID_z
                    rb_mq_obj_new_for_publish.fun_publish_single_message(param_message_body=Message_z)
                    print(Message_z)
                    print('meo has size = ' + str(meo_df.shape[0]) + ' therefore it failed')
                    Logger_obj.log_to_file(param_filename=log_filepath, param_log_str=Message_z)

                #            meo_df_for_sending_message = meo_df[~meo_df['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
                elif (meo_df[~meo_df['ViewData.Status'].isin(['SMT', 'HST', 'OC', 'CT', 'Archive', 'SMR'])].shape[
                          0] == 0):

                    Message_z = str(
                        TaskID_z) + '|' + csc_z + '|' + ReconPurpose_z + '|' + collection_meo_z + '|' + ProcessID_z + '|' + 'FAILURE' + '|' + Setup_Code_z + '|' + MongoDB_TaskID_z
                    rb_mq_obj_new_for_publish.fun_publish_single_message(param_message_body=Message_z)
                    print(Message_z)
                    print('meo has size = ' + str(meo_df.shape[0]) + ' therefore it failed')
                    Logger_obj.log_to_file(param_filename=log_filepath, param_log_str=Message_z)

                else:
                    Logger_obj.log_to_file(param_filename=log_filepath, param_log_str='meo df is not empty, initiating calculations')
                    Logger_obj.log_to_file(param_filename=log_filepath, param_log_str='meo df shape is' + str(meo_df.shape[0]))
                    columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']
                    meo_df['Date'] = pd.to_datetime(meo_df['ViewData.Task Business Date'])
                    meo_df = meo_df.reset_index()
                    meo_df = meo_df.drop('index', 1)

                    meo_df['Date'] = pd.to_datetime(meo_df['Date']).dt.date
                    
                    meo_df['Date'] = meo_df['Date'].astype(str)
                    date_i = meo_df['Date'].mode()[0]

                    print('Choosing the date : ' + date_i)
                    Lombard_parameters = parameters_dict.get(str(client) + '_parameters_dict')
                    Lombard_455_output_files_path_from_dict = str(os.getcwd()) + Lombard_parameters.get(str(client) + '_' + str(setup_code) + '_output_folder_path')
                    print(str(os.getcwd()))
                    os.chdir(str(Lombard_455_output_files_path_from_dict))

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
                    
#                    suffix_for_BD_folder = get_date_subfolder_suffix(param_date = date_to_analyze_ymd_format, param_subfolder_list = recon_done_for_dates_folder_names)
                    suffix_for_BD_folder = get_date_subfolder_suffix(param_date = date_i, param_subfolder_list = recon_done_for_dates_folder_names)
                    base_dir = os.path.join(base_dir + '\\BD_of_' + str(date_i) + '_' + str(suffix_for_BD_folder))
                    if not os.path.exists(base_dir):
                        os.makedirs(base_dir)
                    
                    os.chdir(base_dir)
                    



                    df = meo_df.copy()
                    meo_df_copy = meo_df.copy()
                    del(meo_df)

                    df = df[df['ViewData.Status']!='Archive']

                    df[['len_0','len_1']] = df.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']), axis = 1)

                    df1 = df[(df['len_0']==0) | (df['len_1']==0) ]



                    side0 = df['ViewData.Side0_UniqueIds'].value_counts().reset_index()
                    side1 = df['ViewData.Side1_UniqueIds'].value_counts().reset_index()

                    side0_id = list(set(side0[side0['ViewData.Side0_UniqueIds']==1]['index']))
                    side1_id = list(set(side1[side1['ViewData.Side1_UniqueIds']==1]['index']))

                    df11 = df1[(df1['ViewData.Side0_UniqueIds'].isin(side0_id)) |(df1['ViewData.Side1_UniqueIds'].isin(side1_id)) ]
                    df2_umb = df[((df['ViewData.Status'] == 'UMB') & (df['len_0']>1) & (df['len_1']>1))]

                    df2_umb['predicted status'] = 'UMB'
                    df2_umb['predicted action'] = 'UMB_Carry_Forward'
                    df2_umb['predicted category'] = 'UMB'
                    #df2_umb['predicted comment'] = 'difference in amount'
                    df2_umb.rename(columns = {'ViewData.InternalComment2' : 'predicted comment'}, inplace =  True)
                    df2_umb = df2_umb[columns_to_output]
        
                    serial_num_umb = 0        
                    string_name_umb = 'p'+str(serial_num_umb) 
                    filename_umb = str(client) +' setup ' + str(setup_code) + ' ' + string_name_umb + '.csv'
                    df2_umb.to_csv(filename_umb)

                    df2 = df[(df['len_0']==1) & (df['len_1']==1)]

                    meo_df = pd.concat([df11,df2], axis = 0)
                    meo_df = meo_df.reset_index()
                    meo_df.drop('index', inplace = True, axis = 1)

                    # ### Coding approach to find UMR and UMT

                    # meo_df1= meo_df[meo_df['ViewData.Source Combination']=='Integrata,Goldman Sachs']

                    # dummy_filter = ['remove_mark','sel_mark']
                    # columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.ISIN','ViewData.Investment Type','ViewData.Investment ID','ViewData.Transaction Type','ViewData.Description','ViewData.Settle Date','ViewData.Trade Date','ViewData.Net Amount Difference','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']
                    # amount_column = 'ViewData.Net Amount Difference
                    
                    # filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
                    # serial_num = 1
                    # df1 = common_matching_engine_single1(meo_df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                    # filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
                    # serial_num = 2
                    # df2 = common_matching_engine_double1(df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                    #meo_df1 = meo_df[meo_df['ViewData.Source Combination']=='Integrata,Goldman Sachs']


                    src_comb_code = list(set(meo_df['ViewData.Source Combination']))

                    for item in src_comb_code:
                        meo_df1 = meo_df[meo_df['ViewData.Source Combination']==item]
                    
                    #vital_cols = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.ISIN','ViewData.Investment Type','ViewData.Investment ID','ViewData.Transaction Type','ViewData.Description','ViewData.Settle Date','ViewData.Trade Date','ViewData.Net Amount Difference','ViewData.Status']
                    
                    
                        dummy_filter = ['remove_mark','sel_mark']
                        columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']
                        amount_column = 'ViewData.Net Amount Difference'
                    
                    
                    # In[2425]:
                    
                    
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
                        serial_num = 1
                        df1 = common_matching_engine_single1(meo_df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        
                    #     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
                    #     serial_num = 2
                    #     df2 = common_matching_engine_single2(df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                         
                    
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
                        serial_num = 2
                        df2 = common_matching_engine_double1(df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        
                    #     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
                    #     serial_num = 4
                    #     df4 = common_matching_engine_double2(df3,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        
                        
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID','ViewData.Settle Date']
                        serial_num = 3
                        df3 = common_matching_engine_single1(df2,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        
                    #     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID','ViewData.Settle Date']
                    #     serial_num = 6
                    #     df6 = common_matching_engine_single2(df5,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                         
                    
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID','ViewData.Settle Date']
                        serial_num = 4
                        df4 = common_matching_engine_double1(df3,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        
                    #     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID','ViewData.Settle Date']
                    #     serial_num = 8
                    #     df8 = common_matching_engine_double2(df7,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                    
                    # In[2427]:
                    
                    
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
                        serial_num = 5
                        df5 = common_matching_engine_single1(df4,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        
                    #     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
                    #     serial_num = 10
                    #     df10 = common_matching_engine_single2(df9,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                    
                    # In[2428]:
                    
                    
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
                        serial_num = 6
                        df6 = common_matching_engine_double1(df5,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        
                    #     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
                    #     serial_num = 12
                    #     df12 = common_matching_engine_double2(df11,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID']
                        serial_num = 7
                        df7 = common_matching_engine_single1(df6,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        
                    #     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID']
                    #     serial_num = 14
                    #     df14 = common_matching_engine_single2(df13,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                    
                    # In[2428]:
                    
                    
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID']
                        serial_num = 8
                        df8 = common_matching_engine_double1(df7,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        
                    #     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID']
                    #     serial_num = 16
                    #     df16 = common_matching_engine_double2(df15,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                    
                    # In[2429]:
                    
                    
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description','ViewData.Settle Date']
                        serial_num = 9
                        df9 = common_matching_engine_single1(df8,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    #     serial_num = 18
                    #     df19 = common_matching_engine_single2(df18,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        serial_num = 10
                        df10 = common_matching_engine_double1(df9,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    #     serial_num = 20
                    #     df21 = common_matching_engine_double2(df20,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                    
                    # In[2430]:
                    
                    
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']
                        serial_num = 11
                        df11 = common_matching_engine_single1(df10,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    #     serial_num = 22
                    #     df23 = common_matching_engine_single2(df22,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']
                        serial_num = 12
                        df12 = common_matching_engine_double1(df11,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    #     serial_num = 24
                    #     df25 = common_matching_engine_double2(df24,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                    
                    # In[2431]:
                    
                    
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.Settle Date']
                        serial_num = 13
                        df13 = common_matching_engine_single1(df12,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        serial_num = 14
                        df14 = common_matching_engine_double1(df13,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                    
                    # In[2432]:
                    
                    
                        filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker']
                        serial_num = 15
                        df15 = common_matching_engine_single1(df14,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                        serial_num = 16
                        df16 = common_matching_engine_double1(df15,filters,columns_to_output, amount_column, dummy_filter,serial_num)
                    
                        df3 = df16.copy()
                    	
#                        df = pd.read_excel('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) +'\\output_files\\Setup_' + str(setup_code) + '\\Mapping variables for variable cleaning.xlsx', sheet_name='General')
                        """
                        df = pd.read_excel(model_files_folder + 'Lombard_455_mapping_variables_for_variable_cleaning.xlsx', sheet_name='General')
                    #    df = pd.read_excel('Mapping variables for variable cleaning.xlsx', sheet_name='General')
                    
                        df['tuple'] = df.apply(make_dict, axis=1)
                        clean_map_dict = df.set_index('tuple')['Value'].to_dict()
                    
                        df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
                        df3['ViewData.Asset Type Category'] = df3['ViewData.Asset Type Category'].apply(lambda x : x.lower() if type(x)==str else x)
                        df3['ViewData.Investment Type'] = df3['ViewData.Investment Type'].apply(lambda x : x.lower() if type(x)==str else x)
                        df3['ViewData.Prime Broker'] = df3['ViewData.Prime Broker'].apply(lambda x : x.lower() if type(x)==str else x)
                    
                    
                        df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
                        df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
                        df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
                        df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
                    
                    
                        df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
                        df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
                        df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
                        df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
                    
#                        com = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) +'\\output_files\\Setup_' + str(setup_code) + '\\desc cat with naveen oaktree.csv')
                        com = pd.read_csv(model_files_folder + 'Lombard_455_description_category_comment.csv')
                    #    com = pd.read_csv('desc cat with naveen oaktree.csv')
                        cat_list = list(set(com['Pairing']))
                    
                    
                        df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))
                    
                    
                        df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))
                        com = com.drop(['var','Catogery'], axis = 1)
                        com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
                        com['replace'] = com['replace'].apply(lambda x : x.lower())
                    
                        
                        df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))
                    
                        comp = ['inc','stk','corp ','llc','pvt','plc']
                        df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)
                            
                        df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))
                    
                        df3['new_pb'] = df3['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)
                        new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}
                    
                        df3['new_pb'] = df3['new_pb'].apply(lambda x : new_pf_mapping(x))
                        df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].fillna('kkk')
                        df3['new_pb1'] = df3.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)
                        df3['new_pb1'] = df3['new_pb1'].apply(lambda x : x.lower())
                    
                        df3['ViewData.Settle Date'] = pd.to_datetime(df3['ViewData.Settle Date'])
                        days = [1,30,31,29]
                        df3['monthend marker'] = df3['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)
                    
                        df3['comm_marker'] = 'zero'
                    
                        df3['ViewData.Side0_UniqueIds'] = df3['ViewData.Side0_UniqueIds'].fillna('AA')
                        df3['ViewData.Side1_UniqueIds'] = df3['ViewData.Side1_UniqueIds'].fillna('BB')
                    
                        df3['new_pb2'] = df3.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)
                        df3['new_pb2'] = df3['new_pb2'].apply(lambda x : x.lower())
                    
                        cols = ['ViewData.Transaction Type1','ViewData.Asset Type Category1','ViewData.Investment Type1','new_desc_cat','new_pb2','new_pb1','comm_marker','monthend marker']
                    
                        df4 = df3[cols]
                    
                        df4['ViewData.Transaction Type1'] = df4['ViewData.Transaction Type1'].fillna('aa')
                        df4['ViewData.Asset Type Category1'] = df4['ViewData.Asset Type Category1'].fillna('aa')
                        df4['ViewData.Investment Type1'] = df4['ViewData.Investment Type1'].fillna('aa')
                        df4['new_desc_cat'] = df4['new_desc_cat'].fillna('aa')
                        df4['new_pb2'] = df4['new_pb2'].fillna('aa')
                        df4['new_pb1'] = df4['new_pb1'].fillna('aa')
                        df4['comm_marker'] = df4['comm_marker'].fillna('aa')
                        df4['monthend marker'] = df4['monthend marker'].fillna('aa')
                        """
                        com = pd.read_csv(model_files_folder + 'Lombard_455_description_category_comment.csv')
                    #    com = pd.read_csv('desc cat with naveen oaktree.csv')
                        cat_list = list(set(com['Pairing']))
                        df3['ViewData.Settle Date'] = pd.to_datetime(df3['ViewData.Settle Date'])

                        df3['ViewData.Side0_UniqueIds'] = df3['ViewData.Side0_UniqueIds'].fillna('AA')
                        df3['ViewData.Side1_UniqueIds'] = df3['ViewData.Side1_UniqueIds'].fillna('BB')
                    
                        df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))

                        df3['description'] = df3['desc_cat'].apply(lambda x : desc_cat(x))
                        
                        col_sel = ['ViewData.Transaction Type','ViewData.Asset Type Category', 'ViewData.Mapped Custodian Account','ViewData.Prime Broker','description','broker']
                        
                        
                        df3['broker'] = df3['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('-')[-1])
                        data2 = df3[col_sel]
                        
                        data2['ViewData.Transaction Type'] = data2['ViewData.Transaction Type'].fillna('AAA')
                        data2['ViewData.Asset Type Category'] = data2['ViewData.Asset Type Category'].fillna("HHH")
                        data2['broker'] = data2['broker'].fillna('BBB')
                        data2['description'] = data2['description'].fillna("NNN")
                        
                        cols = ['ViewData.Transaction Type','ViewData.Asset Type Category', 'broker','description']
                        
                        data_to_model = data2[cols]

                    
                    #    filename = 'finalized_model_lombard_249_v1.sav'
#                        filename = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) +'\\output_files\\Setup_' + str(setup_code) + '\\finalized_model_' + str.lower(str('Lombard')) + '_' + str(setup_code) + '_v1.sav'
                        filename = model_files_folder + 'Lombard_455_model.sav'
                        clf = pickle.load(open(filename, 'rb'))
                    
#                        cb_predictions = clf.predict(df4)
                        cb_predictions = clf.predict(data_to_model)
                    #    demo = []
                    #    for item_cb_predict in cb_predictions:
                    #        demo.append(item_cb_predict[0])
                    #    df3['predicted category'] = pd.Series(demo)
                        df3['predicted category'] = np.concatenate(cb_predictions)
                    #    com_temp = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) +'\\output_files\\Setup_' + str(setup_code) + '\\Lombard comment template for delivery.csv')
#                        com_temp = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) +'\\output_files\\Setup_' + str(setup_code) + '\\Lombard 455 comment template for delivery.csv')
                        com_temp = pd.read_csv(model_files_folder + 'Lombard_455_comment_template.csv')
                        com_temp = com_temp.rename(columns = {'Category':'predicted category','template':'predicted template'})
                    #Change made on 19-02-2021 as per Pratik
                    #Begin change made on 19-02-2021
                    #    result_non_trade = df3.copy()
                    #    result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')
                    #
                    #    result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
                    #    result_non_trade['predicted status'] = 'OB'
                    #    result_non_trade['predicted action'] = 'No-pair'
                    #    result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']]
                    #    path = 'Comment file for ' + str(client) + ' ' + str(setup_code) + ' ' + str(item) + '.csv'
                    #
                    ##    path = 'Lombard/249/' + str(item) + 'Comment file for lombard 249.csv'
                    #   
                    #    result_non_trade.to_csv(path)
                    #Begin change made on 19-02-2021
                        result_non_trade = df3.copy()
                        result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')
                    
                        result_non_trade['new_pb1'] = result_non_trade['broker'].apply(lambda x : x.split(' ')[0] if type(x)== str else x)
                        result_non_trade['new_pb1'] = result_non_trade['new_pb1'].fillna('CACEIS')
                    
                    #Change made on 17-02-2021 as per Abhijeet. 
                        result_non_trade['flag_side0'] = result_non_trade.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
                        result_non_trade['flag_side1'] = result_non_trade.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)
                
                        result_non_trade.loc[result_non_trade['ViewData.Side0_UniqueIds'] == 'nan', 'flag_side0'] = 0
                        result_non_trade.loc[result_non_trade['ViewData.Side1_UniqueIds'] == 'nan', 'flag_side1'] = 0
                
                        result_non_trade.loc[result_non_trade['ViewData.Side0_UniqueIds'] == 'None', 'flag_side0'] = 0
                        result_non_trade.loc[result_non_trade['ViewData.Side1_UniqueIds'] == 'None', 'flag_side1'] = 0
                
                        result_non_trade.loc[result_non_trade['ViewData.Side0_UniqueIds'] == '', 'flag_side0'] = 0
                        result_non_trade.loc[result_non_trade['ViewData.Side1_UniqueIds'] == '', 'flag_side1'] = 0
                        
                        result_non_trade['PB_or_Acct_Side'] = result_non_trade.apply(lambda row: assign_PB_Acct_side_row_apply(fun_row=row), axis=1,result_type="expand")
                        
                        result_non_trade['new_pb2'] = result_non_trade.apply(lambda row: assign_geneva_or_nongeneva_based_on_PB_Acct_side(fun_row=row), axis=1,result_type="expand")
                        result_non_trade['new_pb2'] = result_non_trade['new_pb2'].astype(str)
                        result_non_trade['predicted template'] = result_non_trade['predicted template'].astype(str)
                        result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date'].dt.date
                        result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date2'].astype(str)
                        result_non_trade['ViewData.Settle Date_mm_dd_yyyy'] = result_non_trade['ViewData.Settle Date2'].apply(lambda x : make_mm_dd_yyyy_from_string_date_format_yyyy_mm_dd(x))
                    #    result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date'].dt.date
                        result_non_trade['ViewData.Trade Date2'] = pd.to_datetime(result_non_trade['ViewData.Trade Date']).dt.date
                        result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date2'].astype(str)
                        result_non_trade['ViewData.Trade Date_mm_dd_yyyy'] = result_non_trade['ViewData.Trade Date'].apply(lambda x : make_mm_dd_yyyy_from_string_date_format_yyyy_mm_dd(x))
                        result_non_trade['new_pb1'] = result_non_trade['new_pb1'].astype(str)

                        result_non_trade['new_pb1'] = result_non_trade['new_pb1'].replace('CACEIS','Caceis')                    	
                        result_non_trade['new_pb1'] = result_non_trade['new_pb1'].replace('caceis','Caceis')
                        result_non_trade['new_pb1'] = result_non_trade['new_pb1'].replace('','Caceis')
                        #result_non_trade['new_pb1'] = result_non_trade['new_pb1'].apply(lambda x : brokermap(x))
                    	
                    	#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
                    	#Change made on 24-12-2020 as per Abhijeet. The comgen function below was commented out and a new, more elaborate comgen function was coded in. Also, corresponding to the comgen function, predicted_comment apply function was also changed.
                    	#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['ViewData.Side0_UniqueIds'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
#                        result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date2'],x['new_pb1'],x['predicted category'],x['ViewData.Price'],x['ViewData.Quantity'],x['ViewData.Trade Date'],x['ViewData.Description']), axis = 1)
                        result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date_mm_dd_yyyy'],x['new_pb1'],x['predicted category'],x['ViewData.Price'],x['ViewData.Quantity'],x['ViewData.Trade Date_mm_dd_yyyy'],x['ViewData.Description']), axis = 1)
                        result_non_trade['predicted status'] = 'OB'
                        result_non_trade['predicted action'] = 'No-pair'
                        result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']]
                        path = 'Comment file for ' + str(client) + ' ' + str(setup_code) + ' ' + str(item) + '.csv'
                        result_non_trade.to_csv(path)
                    	
                    #    result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]
                    
                    
                    
                    #    result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
                        result_non_trade['predicted status'] = 'OB'
                        result_non_trade['predicted action'] = 'No-pair'
                        result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']]
                        path = 'Comment file for ' + str(client) + ' ' + str(setup_code) + ' ' + str(item) + '.csv'
                    	
                    	
                    serial_num_list = [1,2,3,4,5,6,7,8,9,10,11,12,13,14] 
                    path_serial_num_list = [str(client) +' setup ' + str(setup_code) + ' p' + str(x) for x in serial_num_list]
                    
                    all_paths_list = []
                    for path_serial_num_list_element in path_serial_num_list:
                    	path_serial_num_Source_Combination_list = [path_serial_num_list_element + ' ' + str(src_comb_code_element) + '.csv' for src_comb_code_element in src_comb_code]
                    	all_paths_list.append(path_serial_num_Source_Combination_list)
                    comment_filepath_list = ['Comment file for ' + str(client) + ' ' + str(setup_code) + ' ' + str(src_comb_code_element) + '.csv' for src_comb_code_element in src_comb_code]
                    all_paths_list.append(comment_filepath_list)
                    
                    flatten=lambda l: sum(map(flatten,l),[]) if isinstance(l,list) else [l]	
                    all_paths_list_flattened = flatten(all_paths_list)
                    all_paths_list_flattened.append(filename_umb)
                    
                    
                    def check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(fun_only_filename_with_csv_list):
                        frames = []
                        current_folder = os.getcwd()
                        full_filepath_list = [current_folder + '\\' + x for x in fun_only_filename_with_csv_list]
                        for full_filepath in full_filepath_list :
                            if os.path.isfile(full_filepath) == True:
                                frames.append(pd.read_csv(full_filepath))
                        return pd.concat(frames)
                    
                    final_df = check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(all_paths_list_flattened)
                    
                    stop = timeit.default_timer()
                    
                    print('Time: ', stop - start)
                    
                    final_df = final_df.reset_index()
                    final_df = final_df.drop('index', axis = 1)
                    
                    if('Unnamed: 0' in list(final_df.columns)):
                        final_df.drop(['Unnamed: 0'], axis = 1, inplace = True)
                    
                    final_df = final_df.rename(columns = {'ViewData.BreakID' : 'BreakID',
                                               'ViewData.Source Combination Code' : 'SourceCombinationCode',
                                               'ViewData.Task ID' : 'TaskID',
                                               'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                               'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                               'Predicted Comment' : 'PredictedComment',
                                               'Predicted Category' : 'PredictedCategory'})
                    
                    final_df.to_csv('final_df.csv')
                    
                    def remove_duplicate_commas_from_string(param_str):
                        if(param_str == ''):
                            return('')
                        else:
                            result = (re.findall(r"([\w ]+)",param_str))
                            expected_param_str = result[0]
                            for i in range(1, len(result)):
                                expected_param_str = expected_param_str + ',' + result[i]
                            return(expected_param_str) 
                    		
                    	
                    def replace_leading_trailing_comma_if_str_startswith_or_endswith_comma(param_str):
                    	if(param_str.startswith(',')):
                    		param_str = param_str[1:]
                    	if(param_str.startswith('\',')):
                    		param_str = param_str[2:]
                    	if(param_str.endswith(',')):
                    		param_str = param_str[:-1]
                    	if(param_str.endswith(',\'')):
                    		param_str = param_str[:-2]
                    	if(set(param_str) == set(',')):
                    		param_str = ''
                    	return(param_str)
                    
                    	
                    def clean_list_columns(param_df, param_list_col_name):
                    	param_df[param_list_col_name] = param_df[param_list_col_name].apply(lambda x : str(x).replace('[','').replace(']','').replace('\'','').replace(', ',',').replace('None','').replace('nan','').replace('Nan','').replace('NaN','').replace('AA','').replace('BB','').replace('\\n','').replace('.0',''))
                    	param_df[param_list_col_name] = param_df[param_list_col_name].apply(lambda x : str(x).replace(',,',',').replace('\',','').replace(',\'',''))
                    	param_df[param_list_col_name] = param_df[param_list_col_name].apply(lambda x : replace_leading_trailing_comma_if_str_startswith_or_endswith_comma(str(x)))
                    	param_df[param_list_col_name] = param_df[param_list_col_name].apply(lambda x : remove_duplicate_commas_from_string(str(x)))
                    	return(param_df)	
                    		
                    #final_df['Side0_UniqueIds'].apply(lambda x : str(x).replace('[','').replace(']','').replace('\'','').replace(', ',',').replace('None',''))
                    final_df = clean_list_columns(param_df = final_df, param_list_col_name = 'Side0_UniqueIds')
                    final_df = clean_list_columns(param_df = final_df, param_list_col_name = 'Side1_UniqueIds')
                    final_df = clean_list_columns(param_df = final_df, param_list_col_name = 'BreakID')
                    
                    def unlist_comma_separated_single_quote_string_lst(list_obj):
                        new_list = []
                        for i in list_obj:
                            list_i = list(i.replace('\'','').split(', '))
                            for j in list_i:
                                new_list.append(j)
                        return new_list
                    
                    
                    #BreakId_final_df_2 =  unlist_comma_separated_single_quote_string_lst(fun_final_df_2['BreakID'].astype(str).unique().tolist())
                    
                    def get_first_non_null_value(string_of_values_separated_by_comma):
                        if(string_of_values_separated_by_comma != '' and string_of_values_separated_by_comma != 'nan' and string_of_values_separated_by_comma != 'None' ):
                            if(string_of_values_separated_by_comma.partition(',')[0] != '' and string_of_values_separated_by_comma.partition(',')[0] != 'nan' and string_of_values_separated_by_comma.partition(',')[0] != 'None'):
                                return(string_of_values_separated_by_comma.partition(',')[0])
                            else:
                                return(get_first_non_null_value(string_of_values_separated_by_comma.partition(',')[2]))
                        else:
                            return('Blank value')        
                    
                    
                    final_df['first_non_null_breakid_in_breakid_columns'] = final_df['BreakID'].apply(lambda x : get_first_non_null_value(str(x)))
                    final_df['first_non_null_breakid_in_breakid_columns'] = final_df['first_non_null_breakid_in_breakid_columns'].map(lambda x:x.lstrip('').rstrip(''))
                    final_df.to_csv('final_df_v2.csv')
                    
                    meo_df_copy['ViewData.BreakID'] = meo_df_copy['ViewData.BreakID'].astype(str)
                    final_df_copy = pd.merge(final_df, meo_df_copy[['ViewData.BreakID','ViewData.Task ID','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'first_non_null_breakid_in_breakid_columns', right_on = 'ViewData.BreakID', how='left')
                    
                    final_df_copy['BusinessDate'] = date_i
                    final_df_copy.to_csv('final_df_copy.csv')
                    
                    final_df_copy['BusinessDate'] = pd.to_datetime(final_df_copy['BusinessDate'])
                    final_df_copy['BusinessDate'] = final_df_copy['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
                    final_df_copy['BusinessDate'] = pd.to_datetime(final_df_copy['BusinessDate'])
                    
                    final_df_copy['BreakID_list'] = final_df_copy['BreakID'].apply(lambda x : x.split(','))
                    
                    final_df_copy['BreakID_to_insert_in_db'],final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['BreakID_list'].apply(lambda x : x[0]),final_df_copy['BreakID_list'].apply(lambda x : x[1:])
                    
                    final_df_copy['BreakID_to_insert_in_db'] = final_df_copy['BreakID_to_insert_in_db'].astype(str)
                    final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].astype(str)
                    
                    final_df_copy['BreakID_to_insert_in_db'] = final_df_copy['BreakID_to_insert_in_db'].map(lambda x:x.lstrip('[').rstrip(']'))
                    final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].map(lambda x:x.lstrip('[').rstrip(']'))
                    final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].replace('\' ','\'', regex = True)
                    final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].replace(', ',',', regex = True)
                    final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].replace('\'','', regex = True)
                    
                    final_df_copy['ML_flag'] = 'ML'
                    
                    final_df_copy['SetupID'] = setup_code
                    
                    final_df_copy['probability_No_pair'] = ''
                    final_df_copy['probability_UMB'] = ''
                    final_df_copy['probability_UMR'] = ''
                    final_df_copy['probability_UMT'] = ''
                    
                    #cols_for_database = ['BreakID', 'BusinessDate', 'Final_predicted_break', 'ML_flag',
                    #       'Predicted_Status', 'Predicted_action', 'SetupID',
                    #       'SourceCombinationCode', 'TaskID', 'probability_No_pair',
                    #       'probability_UMB', 'probability_UMR', 'probability_UMT',
                    #       'Side1_UniqueIds', 'PredictedComment', 'PredictedCategory',
                    #       'Side0_UniqueIds']    
                    
                    filepaths_final_df_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_copy_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
#                    final_df_copy.to_csv(filepaths_final_df_copy)
                    
                    cols_for_database = ['Side1_UniqueIds','Side0_UniqueIds',
                    'predicted status', 
                    #'Predicted_Status',
                    'predicted action', 'predicted category',
                    'predicted comment',
                    'ViewData.Task ID',
                    'ViewData.Source Combination Code', 'BusinessDate',
                    'BreakID_to_insert_in_db', 'Predicted_BreakID_to_insert_in_db',
                    'ML_flag', 'SetupID', 'probability_No_pair', 'probability_UMB',
                    'probability_UMR', 'probability_UMT']    
                    
                    
                    final_df_2 = final_df_copy[cols_for_database]
                    
                    cols_for_database_rename_dict = {'predicted status' : 'Predicted_Status',
                                                     'predicted action' : 'Predicted_action',
                                                     'predicted category' : 'PredictedCategory',
                                                     'predicted comment' : 'PredictedComment',
                                                     'ViewData.Task ID' : 'TaskID',
                                                     'ViewData.Source Combination Code' : 'SourceCombinationCode',
                                                     'BreakID_to_insert_in_db' : 'BreakID',
                                                     'Predicted_BreakID_to_insert_in_db' : 'Final_predicted_break'}
                    
                    final_df_2 = final_df_2.rename(columns = cols_for_database_rename_dict)
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
                    
                    final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
                    final_df_2['BusinessDate'] = final_df_2['BusinessDate'].astype(str)
                    final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x:x.lstrip('[').rstrip(']'))
                    
                    final_df_2_copy_2 = final_df_2.copy()
                    final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])
                    final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
                    final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])
                    
                    final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)
                    #def find_taskid_for_breakid_value_apply_function(fun_breakid_string_value,fun_meo_df):
                    #    return(fun_meo_df[fun_meo_df['ViewData.BreakID'] == int(fun_breakid_string_value)]['ViewData.Task ID'].unique())
                    
                    meo_df['ViewData.BreakID'] = meo_df['ViewData.BreakID'].astype(str)
                    single_TaskID_value_for_455 = meo_df['ViewData.Task ID'].mode()[0]
                    if(setup_code == '455'):
                    	final_df_2['TaskID'] = single_TaskID_value_for_455 
                    	#final_df_2['TaskID'] = final_df_2['BreakID'].apply(lambda x : meo_df[meo_df['ViewData.BreakID'] == x]['ViewData.Task ID'].unique())
                    	#
                    	#final_df_2['TaskID'] = final_df_2['TaskID'].astype(str)
                    	#final_df_2['TaskID'] = final_df_2['TaskID'].map(lambda x:x.lstrip('[').rstrip(']'))
                    	#
                    	##final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : np.int64(x)[0])
                    	##
                    	##final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : np.int64(x)[0])
                    	#
                    	##final_df_2 =  final_df_2[~(final_df_2['TaskID'] == '')]
                    	#final_df_2 = final_df_2[final_df_2['TaskID'] != '']
                    	#final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : float(x))
                    	
                    	final_df_2['TaskID'] = final_df_2['TaskID'].astype(np.int64)
                    elif(setup_code == '249'):
                    	final_df_2['TaskID'] = single_TaskID_value_for_455 
                    	final_df_2['TaskID'] = final_df_2['BreakID'].apply(lambda x : meo_df[meo_df['ViewData.BreakID'] == x]['ViewData.Task ID'].unique())
                    	
                    	final_df_2['TaskID'] = final_df_2['TaskID'].astype(str)
                    	final_df_2['TaskID'] = final_df_2['TaskID'].map(lambda x:x.lstrip('[').rstrip(']'))
                    	
                    	#final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : np.int64(x)[0])
                    	#
                    	#final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : np.int64(x)[0])
                    	
                    	#final_df_2 =  final_df_2[~(final_df_2['TaskID'] == '')]
                    	final_df_2 = final_df_2[final_df_2['TaskID'] != '']
                    	final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : float(x))
                    	
                    	final_df_2['TaskID'] = final_df_2['TaskID'].astype(np.int64)
                    	
                    
                    
                    final_df_2[['SourceCombinationCode']] = final_df_2[['SourceCombinationCode']].astype(str)
                    final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].map(lambda x:x.lstrip('[').rstrip(']'))
                    final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].map(lambda x:x.lstrip('\'').rstrip('\''))
                    
                    final_df_2[['Predicted_Status']] = final_df_2[['Predicted_Status']].astype(str)
                    final_df_2[['Predicted_action']] = final_df_2[['Predicted_action']].astype(str)
                    
                    final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace('\'','',regex = True)
                    final_df_2['BreakID'] = final_df_2['BreakID'].replace('\'','',regex = True)
                    final_df_2['BreakID'] = final_df_2['BreakID'].replace(', ',',',regex = True)
                    final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace(', ',',',regex = True)
                    
                    final_df_2['ReconSetupName'] = ReconSetupName
                    final_df_2['ClientShortCode'] = client
                    
                    today = date.today()
                    today_Y_m_d = today.strftime("%Y-%m-%d")
                    
                    final_df_2['CreatedDate'] = today_Y_m_d
                    final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])
                    final_df_2['CreatedDate'] = final_df_2['CreatedDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
                    final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])
                    
                    final_df_2 = final_df_2[final_df_2['TaskID'] != '']
                    final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : float(x))
                    
                    final_df_2['TaskID'] = final_df_2['TaskID'].astype(np.int64)
                    
                    final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)
                    #final_df_2['PredictedComment'] = final_df_2['PredictedComment'].apply(lambda x : x.replace('nan','test'))
                    final_df_2[['Predicted_Status']] = final_df_2[['Predicted_Status']].astype(str)
                    final_df_2[['Predicted_action']] = final_df_2[['Predicted_action']].astype(str)
                    
                    def apply_ui_action_column_Lombard(fun_row):
                        if(fun_row['ML_flag'] == 'Not_Covered_by_ML'):
                            ActionType = 'No Prediction'
                            ActionTypeCode = 7
                        else:
                            if((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
                                ActionType = 'No Action'
                                ActionTypeCode = 6 
                            elif((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
                                ActionType = 'COMMENT'
                                ActionTypeCode = 3
                            elif((fun_row['Predicted_Status'] == 'UCB') & (fun_row['ML_flag'] == 'ML')):
                                ActionType = 'CLOSE'
                                ActionTypeCode = 2 
                            elif(((fun_row['Predicted_Status'] == 'UMB many to many') or (fun_row['Predicted_action'] == 'UMB one to many') or (fun_row['Predicted_action'] == 'UMB one to one') or (fun_row['Predicted_action'] == 'UMR') or (fun_row['Predicted_action'] == 'UMB')) & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
                                ActionType = 'PAIR'
                                ActionTypeCode = 1 
                            elif(((fun_row['Predicted_action'] == 'UMB many to many') or (fun_row['Predicted_action'] == 'UMB one to many') or (fun_row['Predicted_action'] == 'UMB one to one') or (fun_row['Predicted_action'] == 'UMR') or (fun_row['Predicted_action'] == 'UMB')) & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
                                ActionType = 'PAIR WITH COMMENT'
                                ActionTypeCode = 4 
                            else:
                                ActionType = 'Status not covered'
                                ActionTypeCode = 0
                        return ActionType,int(ActionTypeCode)
                    
                    final_df_2[['ActionType','ActionTypeCode']] = final_df_2.apply(lambda row : apply_ui_action_column_Lombard(fun_row = row), axis = 1,result_type="expand")            
                    final_df_2['ActionTypeCode'] = final_df_2['ActionTypeCode'].astype(int)
                    filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '2.csv'
#                    final_df_2.to_csv(filepaths_final_df_2)
                    
                    #coll_1_for_writing_prediction_data = db_for_MEO_data['MLPrediction_Cash']
                    #Changes asked by Shitanshu for db on 24-03-2021
                    coll_1_for_writing_prediction_data = db_for_writing_MEO_data['MLPrediction_' + setup_code]
                    
                    coll_1_for_writing_prediction_data.delete_many({ "BusinessDate": getDateTimeFromISO8601String(date_i), "SetupID": int(setup_code)})
                    #coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.delete_many({ "BusinessDate": getDateTimeFromISO8601String(date_to_analyze_ymd_iso_00_00_format), "SetupID": int(setup_code)})
                    
                    data_dict = final_df_2.to_dict("records_final")
                    coll_1_for_writing_prediction_data.insert_many(data_dict) 
                    
                    data_dict_for_testingdb = final_df_2.to_dict("records_final_for_testingdb")
                    
                    print(setup_code)
                    print(date_i)
                    Logger_obj.log_to_file(param_filename=log_filepath, param_log_str='After inserting in db')

                    print(setup_code)
                    print(date_i)
                    print('Following Task ID done')
                    print(TaskID_z)
                    Message_z = str(TaskID_z) + '|' + str(csc_z) + '|' + str(ReconPurpose_z) + '|' + str(
                        collection_meo_z) + '|' + str(ProcessID_z) + '|' + 'SUCCESS' + '|' + str(
                        Setup_Code_z) + '|' + str(MongoDB_TaskID_z)
                    rb_mq_obj_new_for_publish.fun_publish_single_message(param_message_body=Message_z)
                    print(Message_z)
                    Logger_obj.log_to_file(param_filename=log_filepath, param_log_str=Message_z)

                    outer_while_loop_iterator = outer_while_loop_iterator + 1





except Exception as e:
    logging.error('Exception occured', exc_info=True)



