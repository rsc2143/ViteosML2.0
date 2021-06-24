#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np 
import pandas as pd


import math

from dateutil.parser import parse
import operator
import itertools

import re
import os


# In[ ]:


df1 = pd.read_csv('daily file.csv')


# In[2]:


from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix


# In[ ]:


df1 = df1[(df1['ViewData.Status'] == 'OB') & (~(df1['ViewData.InternalComment2'].isna()))]

df1 = df1.drop_duplicates()

df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].fillna('AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].fillna('BB')

def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b
    
df1['final_ID'] = df1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)

df1 = df1.sort_values(['final_ID','ViewData.Business Date'], ascending = [True, True])

uni2 = df1.groupby(['final_ID','ViewData.Task Business Date']).last().reset_index()
uni2 = uni2.sort_values(['final_ID','ViewData.Task Business Date'], ascending = [True, True])
uni2 = uni2.groupby(['final_ID']).last().reset_index()

df2 = uni2.copy()


# In[ ]:


import re

def doubleside(com):
    if type(com)!= str:
        return 'Float'
    else:
        com = com.lower()
    #x = com.split('[(\s+).]')
        x = re.split("[,/. \- !?:]+", com)
        if (('opposite' in x) & ('sign' in x)):
            return 'oppo'
        elif (('opposite' in x) & ('direction' in x)):
            return 'oppo'
        elif ('opposite' in x) :
            return 'oppo'
        elif ('difference' in x):
            return 'diff'
        elif ((('up' in x) & (('down' in x)| ('dow' in x))) | ('up/down' in x)) :
            return 'upd'
        
        elif ((('matching' in x) | ('netting' in x)  )):
            return "oppo"
        elif 'close' in x:
            return "close"
        elif (('viteos' in x) & ('working' in x)):
            return "viteos working on it"
        elif (('matched' in x)):
            return "matched"
        elif (('no' in x) & ('action' in x)):
            return "no action"
    
        else:
            return 'single'
        
df2['ViewData.InternalComment2'] = df2['ViewData.InternalComment2'].apply(lambda x  : x.replace(u'\xa0', u' ') ) 
df2['s/d'] = df2['ViewData.InternalComment2'].apply(lambda x : doubleside(x))
com = pd.read_csv('comment cat with naveen lombard.csv')


# In[ ]:


com = com.drop(['var','Catogery','Client'] ,axis = 1)
com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())

com = com.drop_duplicates()

both_final = ['ViewData.Currency',
 'ViewData.Account Type',
 'ViewData.Accounting Net Amount',

 'ViewData.Activity Code',
 'ViewData.Age',
 'ViewData.Age WK',
 'ViewData.Alt ID 1',
 'ViewData.Asset Type Category',
 'ViewData.Assigned To',
 'ViewData.Base Currency',
 'ViewData.Base Net Amount',
 'ViewData.Bloomberg_Yellow_Key',
 'ViewData.B-P Net Amount',
 
 
 'ViewData.BreakID',
 'ViewData.Business Date',
 'ViewData.Call Put Indicator',
 'ViewData.Cancel Amount',
 'ViewData.Cancel Flag',
 'ViewData.ClusterID',
 'ViewData.Commission',
 'ViewData.CUSIP',
 'ViewData.Custodian',
 'ViewData.Custodian Account',
 
 'ViewData.Department',
 'ViewData.Derived Source',
 'ViewData.Description',
 'ViewData.ETL File Code',
 'ViewData.ETL Package Code',
 'ViewData.ExpiryDate',

 'ViewData.Fund',
 'ViewData.FX Rate',
 'ViewData.Group',
 'ViewData.Has Attachments',
 'ViewData.Interest Amount',
 'ViewData.InternalComment1',
 'ViewData.InternalComment2',
 'ViewData.InternalComment3',
 'ViewData.Investment ID',
 'ViewData.Investment Type',
 'ViewData.Is Combined Data',
 'ViewData.ISIN',
 'ViewData.Keys',
 'ViewData.Knowledge Date',
 'ViewData.Legal Entity',
 
 'ViewData.Mapped Custodian Account',
 'ViewData.Net Amount Difference',
 'ViewData.Net Amount Difference Absolute',
 'ViewData.Non Trade Description',
 'ViewData.OTE Custodian Account',
 'ViewData.OTE Ticker',
 'ViewData.OTEIncludeFlag',
 'ViewData.PB Account Numeric',
 'ViewData.Portfolio ID',
 'ViewData.Portolio',
 'ViewData.Price',
 'ViewData.Prime Broker',
 'ViewData.Principal Amount',
 'ViewData.Prior Knowledge Date',
 'ViewData.Priority',
 'ViewData.Quantity',
 'ViewData.Reviewer',
 'ViewData.Rule And Key',
 'ViewData.Sec Fees',
 'ViewData.SEDOL',
 'ViewData.Settle Date',
 'ViewData.SPM ID',
 'ViewData.Status',
 'ViewData.Strategy',
 'ViewData.Strike Price',
 'ViewData.System Comments',
 'ViewData.Ticker',
 'ViewData.Trade Date',
 'ViewData.Trade Expenses',
 'ViewData.Transaction Category',
 'ViewData.Transaction ID',
 'ViewData.Transaction Type',
 'ViewData.Underlying Cusip',
 'ViewData.Underlying Investment ID',
 'ViewData.Underlying ISIN',
 'ViewData.Underlying Sedol',
 'ViewData.Underlying Ticker',
 'ViewData.UserTran1',
 'ViewData.UserTran2',
 'ViewData.Value Date',
 'ViewData.Workflow Remark',
 'ViewData.Workflow Status','ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds','ViewData.Task Business Date','s/d']


# In[ ]:


remove = ['ViewData.Interest Amount', 'ViewData.OTE Ticker', 'ViewData.Activity Code', 'ViewData.Principal Amount', 'ViewData.Derived Source', 'ViewData.Sec Fees', 'ViewData.Call Put Indicator', 'ViewData.OTE Custodian Account', 'ViewData.FX Rate', 'ViewData.Legal Entity', 'ViewData.Strike Price', 'ViewData.Bloomberg_Yellow_Key']


# In[ ]:


both_final1 = []

for item in both_final:
    if item not in remove:
        both_final1.append(item)


# In[ ]:


df3 = df2[both_final1]
df3 = df3[df3['s/d']=='single']
cat_list = list(set(com['Pairing']))


# In[ ]:


def comclean(com,cat_list):
    cat_all1 = []
    m = 0
    
    if type(com)==str:
        com = com.lower()
    
    
        list1 = cat_list
    
        com1 = re.split("[-,.() /!_?:]+", com)
        com2 = []
        for itemk in com1:
            itemk1 = itemk.split(' - ')
            com2= com2+ itemk1
            
    
    
        for item in list1:
            item = item.lower()
            item1 = item.split(' ')
            lst3 = [value for value in item1 if value in com2] 
            if len(lst3) == len(item1):
                cat_all1.append(item)
                m = m+1
            
            else:
                m = m
            
        if m >0 :
            return cat_all1
        else:
            return "NA"
    else:
        return 'Float'
    


# In[ ]:


df3['Com_cat'] = df3['ViewData.InternalComment2'].apply(lambda row : comclean(row,cat_list))


# In[ ]:


def intereset_clean(x):
    k = 0
    i = 0
    for item in x:
        a = len(item)
        if a>k:
            k = a
            word = item
            #print(word)
    
    ret_item = []
    for item1  in x:
        
        item2 = item1.split()
        words = word.split()
        lst3 = [value for value in item2 if value in words]
        #print(lst3)
        if len(lst3)!=len(item2):
            ret_item.append(item1)
            
    ret_item.append(word)
    return list(set(ret_item))


# In[ ]:


df3['Com_cat1'] = df3['Com_cat'].apply(lambda row : intereset_clean(row))


# In[ ]:


def catcln1(cat,df):
    ret = []
    if cat == 'NA':
        return 'NA'
    elif 'equity swap settlement' in cat:
        ret.append('equity swap settlement')
    elif 'equity swap' in cat:
        ret.append('equity swap settlement')
    elif 'swap settlement' in cat:
        ret.append('equity swap settlement')
    elif (('follow up email' in cat)|('email sent' in cat) ):
        if (('follow up email' in cat)& ('email sent' in cat) & (len(cat)==2) ):
            ret.append('follow up email')
            cat.remove('email sent')
            cat.remove('follow up email')
        elif (('follow up email' in cat) & (len(cat)==1)):
            ret.append('follow up email')
            cat.remove('follow up email')
        elif (('email sent' in cat) & (len(cat)==1)):
            ret.append('follow up email')
            cat = cat.remove('email sent')
        elif (('A' in cat) & ('N' in cat) :
            ret.append('NA')
            
                
        else:
            if len(cat)==0:
                ret.append('email sent')
            else:
                for item in cat:
                    item = str(item)
        
                    if item == 'sd':
                        ret.append('sd1')
                    else:
            
                        a = df[df['Pairing']==item]['replace'].values[0]
                        if ((a!= 'sd') | (a not in ret)):
                            ret.append(a)
                
           
        
       
            
          
                
        
        
    else:
        for item in cat:
            item = str(item)
        
            if item == 'sd':
                ret.append('sd1')
            else:
            
                a = df[df['Pairing']==item]['replace'].values[0]
                if ((a!= 'sd') | (a not in ret)):
                    ret.append(a)
    
    return list(set(ret))


# In[ ]:


df3['Com_cat2'] = df3['Com_cat1'].apply(lambda row : catcln1(row,com))


# In[ ]:


remove_keyword = ['break cleared','breaks cleared','break clear next run','future dated trade','trade','follow up email','currency', 'security','mapped custodian account','allocleg','cusip','corp']


# In[ ]:


def comcat1(x):
    if x == 'NA':
        k = x
   
    elif len(x)==1:
        k = x
    else:
        k = []
        for item in x:
            if item not in remove_keyword:
                k.append(item)
    return k
            


# In[ ]:


df3['Com_cat2'] =df3['Com_cat1'].apply(lambda row : comcat1(row))


# In[ ]:


df3['Category'] = df3['Com_cat2'].apply(lambda x : ' '.join(x))


# In[ ]:




#df3 = ob.copy()

df = pd.read_excel('Mapping variables for variable cleaning.xlsx', sheet_name='General')
def make_dict(row):
    keys_l = str(row['Keys']).lower()
    keys_s = keys_l.split(', ')
    keys = tuple(keys_s)
    return keys
df['tuple'] = df.apply(make_dict, axis=1)
clean_map_dict = df.set_index('tuple')['Value'].to_dict()

df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Asset Type Category'] = df3['ViewData.Asset Type Category'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Investment Type'] = df3['ViewData.Investment Type'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Prime Broker'] = df3['ViewData.Prime Broker'].apply(lambda x : x.lower() if type(x)==str else x)

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

df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker'].apply(lambda x : clean_mapping(x) if type(x)==str else x)

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

df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].apply(lambda x : comb_clean(x) if type(x)==str else x)

import re

com = pd.read_csv('desc cat with naveen oaktree.csv')
cat_list = list(set(com['Pairing']))

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

df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))

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


# In[ ]:


df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))
com = com.drop(['var','Catogery'], axis = 1)
com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())

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
    
df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))

comp = ['inc','stk','corp ','llc','pvt','plc']
df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)
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
        
df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))

df3['new_pb'] = df3['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)
new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}
def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x
df3['new_pb'] = df3['new_pb'].apply(lambda x : new_pf_mapping(x))
df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].fillna('kkk')
df3['new_pb1'] = df3.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)
df3['new_pb1'] = df3['new_pb1'].apply(lambda x : x.lower())


# ### Preparation for OB

# In[ ]:


data = df3.copy()


# In[3]:


#data = pd.read_csv('preprocessed lombard 249 comment v2.csv')


# In[4]:


data['ViewData.Prime Broker'].count()


# In[80]:


data['new_pb1'].value_counts().iloc[0:120]


# In[6]:


data['new_pb1'] = data['new_pb1'].apply(lambda x : x.split('-')[0] if type(x)==str else str(x))


# In[71]:


data['new_pb1'] = data['new_pb1'].apply(lambda x : x.lstrip())
data['new_pb1'] = data['new_pb1'].apply(lambda x : x.rstrip())


# In[76]:


def pbmap(x):
    if x == 'goldman sachs':
        return 'gs'
    elif x == 'morgan stanley':
        return 'ms'
    else:
        return x


# In[78]:


data['new_pb1'] = data['new_pb1'].apply(lambda x: pbmap(x))


# In[94]:


data['new_pb1'] = data['new_pb1'].fillna('AAA')


# In[82]:


days = [1,30,31,29]
data['ViewData.Settle Date'] = pd.to_datetime(data['ViewData.Settle Date'])
data['monthend marker'] = data['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)
data['ViewData.Commission'] = data['ViewData.Commission'].fillna('NA')
def comfun(x):
    if x=="NA":
        k = 'NA'
       
    elif x == 0.0:
        k = 'zero'
    else:
        k = 'positive'
   
    return k
data['comm_marker'] = data['ViewData.Commission'].apply(lambda x : comfun(x))
data['new_pb2'] = data.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)
#data['new_pb2'] = data['new_pb2'].apply(lambda x : x.lower())


# In[83]:


data['ViewData.Settle Date'] = pd.to_datetime(data['ViewData.Settle Date'])


# In[84]:


days = [1,30,31,29]


# In[85]:


data['monthend marker'] = data['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)


# In[86]:


data['ViewData.Commission'] = data['ViewData.Commission'].fillna('NA')


# In[87]:


def comfun(x):
    if x=="NA":
        k = 'NA'
       
    elif x == 0.0:
        k = 'zero'
    else:
        k = 'positive'
   
    return k
    


# In[88]:


data['comm_marker'] = data['ViewData.Commission'].apply(lambda x : comfun(x))


# In[91]:


data['new_pb2'] = data.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)


# In[92]:


data['new_pb2'] = data['new_pb2'].apply(lambda x : x.lower())


# In[96]:


Pre_final = [
    
'ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID',
 


 'ViewData.Currency',
 'ViewData.Custodian',
     'ViewData.ISIN',

 'ViewData.Mapped Custodian Account',
 
 'ViewData.Net Amount Difference Absolute',
 
 'ViewData.Portolio',
 'ViewData.Settle Date',
 
 'ViewData.Trade Date',
 'ViewData.Transaction Type1',


 
 
 
 'ViewData.Accounting Net Amount',
 'ViewData.Asset Type Category1',
 
 
 'ViewData.CUSIP',
 'ViewData.Commission',
 
 'ViewData.Fund',
 
 
 'ViewData.Investment ID',
 'ViewData.Investment Type1',
 
 
 'ViewData.Price',
 'ViewData.Prime Broker1',

 'ViewData.Quantity',
 
'ViewData.InternalComment2', 'new_desc_cat','new_pb2','new_pb1',
    'Category'
 
]


# In[97]:


data = data[Pre_final]


# In[98]:


df_mod1 = data.copy()


# In[99]:


df_mod1.count()


# In[100]:


#df_mod1['ViewData.Business Date'] = df_mod1['ViewData.Business Date'].fillna(0)
df_mod1['ViewData.Custodian'] = df_mod1['ViewData.Custodian'].fillna('AA')
df_mod1['ViewData.Portolio'] = df_mod1['ViewData.Portolio'].fillna('bb')
df_mod1['ViewData.Settle Date'] = df_mod1['ViewData.Settle Date'].fillna(0)
df_mod1['ViewData.Trade Date'] = df_mod1['ViewData.Trade Date'].fillna(0)
df_mod1['ViewData.Accounting Net Amount'] = df_mod1['ViewData.Accounting Net Amount'].fillna(0)
df_mod1['ViewData.Asset Type Category1'] = df_mod1['ViewData.Asset Type Category1'].fillna('CC')
df_mod1['ViewData.CUSIP'] = df_mod1['ViewData.CUSIP'].fillna('DD')
df_mod1['ViewData.Fund'] = df_mod1['ViewData.Fund'].fillna('EE')
df_mod1['ViewData.Investment ID'] = df_mod1['ViewData.Investment ID'].fillna('FF')
df_mod1['ViewData.Investment Type1'] = df_mod1['ViewData.Investment Type1'].fillna('GG')
#df_mod1['ViewData.Knowledge Date'] = df_mod1['ViewData.Knowledge Date'].fillna(0)
df_mod1['ViewData.Price'] = df_mod1['ViewData.Price'].fillna(0)
df_mod1['ViewData.Prime Broker1'] = df_mod1['ViewData.Prime Broker1'].fillna("HH")
df_mod1['ViewData.Quantity'] = df_mod1['ViewData.Quantity'].fillna(0)
#df_mod1['ViewData.Sec Fees'] = df_mod1['ViewData.Sec Fees'].fillna(0)
#df_mod1['ViewData.Strike Price'] = df_mod1['ViewData.Strike Price'].fillna(0)
df_mod1['ViewData.Commission'] = df_mod1['ViewData.Commission'].fillna(0)
df_mod1['ViewData.Transaction Type1'] = df_mod1['ViewData.Transaction Type1'].fillna('kk')
df_mod1['ViewData.ISIN'] = df_mod1['ViewData.ISIN'].fillna('mm')
#df_mod1['new_desc_cat'] = df_mod1['new_desc_cat'].fillna('nn')
df_mod1['Category'] = df_mod1['Category'].fillna('NA')
df_mod1['new_desc_cat'] = df_mod1['new_desc_cat'].fillna('nn')


# In[101]:


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b


# In[102]:


df_mod1['final_ID'] = df_mod1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


# In[103]:


df_mod1.shape


# ### With new categories

# In[105]:


data2 = df_mod1.copy()


# In[106]:


data2.columns


# In[107]:


data2.shape


# 

# In[108]:


cat_vars = [ 
      
       'ViewData.Transaction Type1','ViewData.Asset Type Category1', 
            'ViewData.Investment Type1','new_desc_cat',
       'new_pb2', 'new_pb1'
       ]


# In[109]:


data2.count()


# - Remove all the categories with value count less than 2. Change remove array according to the data

# In[110]:


data4 = data2.copy()


# In[113]:


cat_all = data2['Category'].value_counts().reset_index()


# In[114]:


remove = list(set(cat_all[cat_all['Category']<11]['index']))


# In[115]:


remove_new  = ['A N','mo','follow up email','break cleared','email sent','plug in cash','currency','proceeds trade settlement repo proceeds repo','cancelled cash plug mail sent','transfer']


# In[116]:


remove_all = remove + remove_new


# In[117]:


data2 = data2[(data2['Category']!='NA') & (~data2['Category'].isin(remove_all) )]


# In[239]:


data2.columns


# In[240]:


data2.count()


# In[119]:


data2['new_pb2'] = data2['new_pb2'].fillna('aaa')
data2['new_pb1'] = data2['new_pb1'].fillna('bbb')


# In[120]:


#data2['Category'] = data2['Category'].apply(lambda x: 'equity swap settlement' if x =='swap settlement' else x)


# In[121]:


X_temp, X_test, y_temp, y_test = train_test_split(data2.drop((['Category']),axis=1), 
           data2['Category'], test_size=0.20, 
            random_state=88, shuffle=True,stratify=data2['Category'])


# In[122]:


cols = [
 

 
'ViewData.Transaction Type1','ViewData.Asset Type Category1', 
            'ViewData.Investment Type1','new_desc_cat',
       'new_pb2', 'new_pb1'
 

 
 
 
              
             ]


# In[123]:


X_temp1 =  X_temp[cols]
X_test1 = X_test[cols]


# In[124]:


X_train, X_val, y_train, y_val = train_test_split(X_temp1, 
           y_temp, test_size=0.1, 
            random_state=87, shuffle=True,stratify=y_temp)


# In[125]:


from catboost import CatBoostClassifier


# In[126]:


import catboost


# In[127]:


print(catboost.__version__)


# In[128]:


SEED =88
params ={'loss_function' : 'MultiClass',
        'eval_metric' : 'Accuracy',
        'learning_rate':0.05,
        'iterations':700,
        'depth':5,
        'random_seed':SEED,
        'od_type':'Iter',
        'od_wait':200,
        'cat_features':cat_vars,
        'task_type':'CPU'}

clf = CatBoostClassifier(**params)
   

clf.fit(X_train, 
            y_train,eval_set=(X_val, y_val),
       use_best_model=True,plot=True)


# In[ ]:


import pickle
filename = 'finalized_model_lombard 249 after retraining.sav'
pickle.dump(clf, open(filename, 'wb'))


# In[ ]:




