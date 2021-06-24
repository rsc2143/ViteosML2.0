#!/usr/bin/env python
# coding: utf-8

# In[25]:


import numpy as np
import pandas as pd
import shap
import smote


# In[26]:


cols = ['Currency','Account Type','Accounting Net Amount',
#'Accounting Net Amount Difference','Accounting Net Amount Difference Absolute ',
#'Activity Code',
'Age','Age WK',
'Asset Type Category','Base Currency','Base Net Amount',
#'Bloomberg_Yellow_Key',
'B-P Net Amount',
#'B-P Net Amount Difference','B-P Net Amount Difference Absolute',
'BreakID',
'Business Date','Cancel Amount','Cancel Flag','CUSIP','Custodian',
'Custodian Account',
#'Derived Source',
'Description','Department',
        #'ExpiryDate','ExternalComment1','ExternalComment2',
'ExternalComment3','Fund',
#'FX Rate',
#'Interest Amount',
'InternalComment1','InternalComment2',
'InternalComment3','Investment Type','Is Combined Data','ISIN','Keys',
'Mapped Custodian Account','Net Amount Difference','Net Amount Difference Absolute','Non Trade Description',
#'OTE Custodian Account',
#'Predicted Action','Predicted Status','Prediction Details',
'Price','Prime Broker',
'Quantity','SEDOL','Settle Date','SPM ID','Status',
#'Strike Price',
'System Comments','Ticker','Trade Date','Trade Expenses','Transaction Category','Transaction ID','Transaction Type',
'Underlying Cusip','Underlying Investment ID','Underlying ISIN','Underlying Sedol','Underlying Ticker','Source Combination','_ID']
#'UnMapped']

add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
      # 'MetaData.0._RecordID','MetaData.1._RecordID',
       'ViewData.Task Business Date']


# In[27]:


new_cols = ['ViewData.' + x for x in cols] + add


# In[28]:


len(new_cols)


# In[110]:


#df_125 = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/HST_RecData_123.csv",usecols=new_cols)

df_125 = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/For_Arjun/Weiss/HST_RecData_125.csv",usecols=new_cols)


# In[111]:


df_125.shape


# In[112]:


df = df_125[~df_125['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
#df = df[df['MatchStatus'] != 21]
df = df[~df['ViewData.Status'].isnull()]
df = df.reset_index()
df = df.drop('index',1)


# In[113]:


df.shape


# In[114]:


#pd.set_option('display.max_columns', 500)


# In[115]:


df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])
#df1['Date'] = pd.to_datetime(df1['ViewData.Task Business Date'])


# In[116]:


df = df[~df['Date'].isnull()]
df = df.reset_index()
df = df.drop('index',1)


# In[117]:


df['Date'] = pd.to_datetime(df['Date']).dt.date
#df1['Date'] = pd.to_datetime(df1['Date']).dt.date


# In[118]:


df['Date'] = df['Date'].astype(str)
#df1['Date'] = df1['Date'].astype(str)


# In[119]:


df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)


# In[120]:


df['flag'] = df['ViewData.InternalComment2'].apply(lambda x: 1 if 'Difference of' in str(x) else 0)


# In[121]:


unique_side0_id_array = list(df[(df['ViewData.Status'].isin(['UMR','UMT'])) & (df['flag_side0']==1) & (df['flag_side1']==1)]['ViewData.Side0_UniqueIds'].unique())
unique_side1_id_array = list(df[(df['ViewData.Status'].isin(['UMR','UMT'])) & (df['flag_side0']==1) & (df['flag_side1']==1)]['ViewData.Side1_UniqueIds'].unique())


# In[122]:


unique_side0_id_array_umb = list(df[df['ViewData.Side0_UniqueIds'].isin(df[(df['ViewData.Status']=='UMB') & (df['flag_side0']==1) & (df['flag_side1']==1) & (df['ViewData.Age']<10) & (df['Date']>'2020-02-15')]['ViewData.Side0_UniqueIds'])]['ViewData.Side0_UniqueIds'].unique())
 
unique_side1_id_array_umb = list(df[df['ViewData.Side1_UniqueIds'].isin(df[(df['ViewData.Status']=='UMB') & (df['flag_side0']==1) & (df['flag_side1']==1) & (df['ViewData.Age']<10) & (df['Date']>'2020-02-15')]['ViewData.Side1_UniqueIds'])]['ViewData.Side1_UniqueIds'].unique())
                                                                     


# In[123]:


import tqdm


# In[124]:


for i in range(df[(df['ViewData.Status'].isin(['UMR','UMT'])) & (df['flag_side0']==1) & (df['flag_side1']>1)].shape[0]):
    mid_array = [j for j in df[(df['ViewData.Status'].isin(['UMR','UMT'])) & (df['flag_side0']==1) & (df['flag_side1']>1)]['ViewData.Side1_UniqueIds'].values[i].split(',')]
    unique_side1_id_array = unique_side1_id_array + mid_array


# In[125]:


for i in range(df[(df['ViewData.Status'].isin(['UMR','UMT'])) & (df['flag_side0']==1) & (df['flag_side1']>1)].shape[0]):
    mid_array = [j for j in df[(df['ViewData.Status'].isin(['UMR','UMT'])) & (df['flag_side0']==1) & (df['flag_side1']>1)]['ViewData.Side0_UniqueIds'].values[i].split(',')]
    unique_side0_id_array = unique_side0_id_array + mid_array   


# In[126]:


#set(unique_side0_id_array)
unique_side1_id_array = list(set(unique_side1_id_array))
unique_side0_id_array = list(set(unique_side0_id_array))


# In[127]:


#unique_side0_id_array_umb
df[(df['Date']>'2020-04-01')].shape


# In[128]:


df4 = df[(df['Date']>'2020-04-01') |(df['ViewData.Status'].isin(['UMR','UMT'])) | (df['ViewData.Side0_UniqueIds'].isin(unique_side0_id_array)) | (df['ViewData.Side1_UniqueIds'].isin(unique_side1_id_array))]
df4 = df4.reset_index()
df4 = df4.drop('index',1)


# In[129]:


#sample = df[df['Date']!='2019-11-20']
#sample = df3[df3['Set_up']==833].copy()
sample = df4.copy()


# In[130]:


sample['ViewData.Side0_UniqueIds'] = sample['ViewData.Side0_UniqueIds'].astype(str)
sample['ViewData.Side1_UniqueIds'] = sample['ViewData.Side1_UniqueIds'].astype(str)


# In[131]:


sample['flag_side0'] = sample.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
sample['flag_side1'] = sample.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)


# In[132]:


sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0


# In[133]:



sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'

sample.loc[sample['Trans_side']=='A_side','ViewData.B-P Currency'] = sample.loc[sample['Trans_side']=='A_side','ViewData.Currency']
sample.loc[sample['Trans_side']=='B_side','ViewData.Accounting Currency'] = sample.loc[sample['Trans_side']=='B_side','ViewData.Currency'] 

sample['ViewData.B-P Currency'] = sample['ViewData.B-P Currency'].astype(str)
sample['ViewData.Accounting Currency'] = sample['ViewData.Accounting Currency'].astype(str)
sample['ViewData.Source Combination'] = sample['ViewData.Source Combination'].astype(str)

sample['filter_key'] = sample.apply(lambda x: x['ViewData.Source Combination'] + x['ViewData.Mapped Custodian Account'] + x['ViewData.B-P Currency'] if x['Trans_side']=='A_side' else x['ViewData.Source Combination'] + x['ViewData.Mapped Custodian Account'] + x['ViewData.Accounting Currency'], axis=1)


sample1 = sample[(sample['flag_side0']<=1) & (sample['flag_side1']<=1) & (sample['ViewData.Status'].isin(['OB','SDB','UDB','UOB','UCB']))]

sample1 = sample1.reset_index()
sample1 = sample1.drop('index', 1)

#sample1.loc[sample1['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
#sample1.loc[sample1['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'


# In[134]:


sample1 = sample[(sample['flag_side0']<=1) & (sample['flag_side1']<=1) & (sample['ViewData.Status'].isin(['OB','SDB','UDB','UOB','UCB']))]

sample1 = sample1.reset_index()
sample1 = sample1.drop('index', 1)


# In[135]:


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


# In[136]:


#sample['ViewData.Status'].value_counts()
sample1['Comment_cat'] = sample1['ViewData.InternalComment2'].apply(lambda x: doubleside(x))


# In[137]:


sample1 = sample1[(sample1['Comment_cat']=='single') & (sample1['ViewData.InternalComment2']!='Match')]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[138]:


sample1['PB_reporting_flag'] = sample1['ViewData.InternalComment2'].apply(lambda x: 1 if 'PB reporting' in x else 0)


# In[139]:


sample1['Updown_reporting_flag'] = sample1['ViewData.InternalComment1'].apply(lambda x: 1 if 'UpDown' in str(x) else 0)


# In[140]:


sample1['Weak_key_record_flag'] = sample1['ViewData.System Comments'].apply(lambda x: 1 if 'Weak key records with matching keys' in str(x) else 0)


# In[141]:


#sample1[['Comment_cat','ViewData.InternalComment2']]

sample1 = sample1[sample1['PB_reporting_flag'] ==0]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[142]:


sample1 = sample1[sample1['Updown_reporting_flag'] ==0]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[143]:


sample1 = sample1[sample1['Weak_key_record_flag'] ==0]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[144]:


sample1['ViewData.BreakID'] = sample1['ViewData.BreakID'].astype(int)


# In[145]:


#spm['ViewData.BreakID'] = spm['ViewData.BreakID'].astype(int)


# In[146]:


sample1 = sample1[sample1['ViewData.BreakID']!=-1]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[147]:


sample1 = sample1.sort_values(['ViewData.BreakID','Date'], ascending =[True, False])
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[148]:


sample1['OB_unique_flag'] = sample1.groupby('ViewData.BreakID').cumcount()


# In[149]:


sample1 = sample1[(sample1['OB_unique_flag']==0) | (sample1['ViewData.Status']=='UCB')]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[150]:


zero_side_unique_statuses = sample1.groupby('ViewData.Side0_UniqueIds')['ViewData.Status'].unique().reset_index()
one_side_unique_statuses = sample1.groupby('ViewData.Side1_UniqueIds')['ViewData.Status'].unique().reset_index()


# In[151]:


zero_side_unique_statuses['flag'] = zero_side_unique_statuses['ViewData.Status'].apply(lambda x: 1 if 'UCB' not in x else 0)


# In[152]:


one_side_unique_statuses['flag'] = one_side_unique_statuses['ViewData.Status'].apply(lambda x: 1 if 'UCB' not in x else 0)


# In[153]:


sample1_split1 = sample1[sample1['ViewData.Side0_UniqueIds'].isin(zero_side_unique_statuses[zero_side_unique_statuses['flag']== 1]['ViewData.Side0_UniqueIds'])]
sample1_split2 = sample1[sample1['ViewData.Side1_UniqueIds'].isin(one_side_unique_statuses[one_side_unique_statuses['flag']== 1]['ViewData.Side1_UniqueIds'])]


# In[154]:


sample1_split3 = sample1[sample1['ViewData.Status']=='UCB']


# In[155]:


final_sample1 = pd.concat([sample1_split1, sample1_split2, sample1_split3], axis=0)
final_sample1 = final_sample1.reset_index()
final_sample1 = final_sample1.drop('index',1)


# In[156]:


sample2 =  final_sample1.sample(frac =.9)


# In[157]:


sample2 = final_sample1.copy()


# In[158]:


aa = sample2[sample2['Trans_side']=='A_side']
bb = sample2[sample2['Trans_side']=='B_side']


# In[159]:


aa['filter_key'] = aa['ViewData.Source Combination'].astype(str) + aa['ViewData.Mapped Custodian Account'].astype(str) + aa['ViewData.B-P Currency'].astype(str)

bb['filter_key'] = bb['ViewData.Source Combination'].astype(str) + bb['ViewData.Mapped Custodian Account'].astype(str) + bb['ViewData.Accounting Currency'].astype(str)


# In[160]:



#'ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds'
common_cols = ['ViewData.Accounting Net Amount', 'ViewData.Age',
'ViewData.Age WK', 'ViewData.Asset Type Category',
'ViewData.B-P Net Amount', 'ViewData.Base Net Amount','ViewData.CUSIP', 
 'ViewData.Cancel Amount',
       'ViewData.Cancel Flag',
#'ViewData.Commission',
        'ViewData.Currency', 'ViewData.Custodian',
       'ViewData.Custodian Account',
      'ViewData.Description',
               'ViewData.Department',
               #'ViewData.ExpiryDate', 
               'ViewData.Fund',
       'ViewData.ISIN',
       'ViewData.Investment Type',
      # 'ViewData.Keys',
       'ViewData.Mapped Custodian Account',
       'ViewData.Net Amount Difference',
       'ViewData.Net Amount Difference Absolute',
        #'ViewData.OTE Ticker',
        'ViewData.Price',
       'ViewData.Prime Broker', 'ViewData.Quantity',
       'ViewData.SEDOL', 'ViewData.SPM ID', 'ViewData.Settle Date',
       
   # 'ViewData.Strike Price',
               'Date','ViewData.Underlying Investment ID',
       'ViewData.Ticker', 'ViewData.Trade Date',
       'ViewData.Transaction Category',
       'ViewData.Transaction Type', 'ViewData.Underlying Cusip',
       'ViewData.Underlying ISIN',
       'ViewData.Underlying Sedol','filter_key','ViewData.Status','ViewData.BreakID',
              'ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData._ID']
               #'ViewData.InternalComment2']

#y_col = ['ViewData.Status']


# In[161]:


aa = aa.reset_index()
aa = aa.drop('index', 1)
bb = bb.reset_index()
bb = bb.drop('index', 1)


# In[162]:


from tqdm import tqdm
from pandas import merge

pool =[]
key_index =[]
training_df =[]

no_pair_ids = []
#max_rows = 5

for d in tqdm(aa['Date'].unique()):
    aa1 = aa.loc[aa['Date']==d,:][common_cols]
    bb1 = bb.loc[bb['Date']==d,:][common_cols]
    
    aa1 = aa1.reset_index()
    aa1 = aa1.drop('index',1)
    bb1 = bb1.reset_index()
    bb1 = bb1.drop('index', 1)
    
    bb1 = bb1.sort_values(by='filter_key',ascending =True)
    
    for key in (list(np.unique(np.array(list(aa1['filter_key'].values) + list(bb1['filter_key'].values))))):
        
        df1 = aa1[aa1['filter_key']==key]
        df2 = bb1[bb1['filter_key']==key]

        if df1.empty == False and df2.empty == False:
            #aa_df = pd.concat([aa1[aa1.index==i]]*repeat_num, ignore_index=True)
            #bb_df = bb1.loc[pool[len(pool)-1],:][common_cols].reset_index()
            #bb_df = bb_df.drop('index', 1)

            df1 = df1.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
            df2 = df2.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})

            #dff  = pd.concat([aa[aa.index==i],bb.loc[pool[i],:][accounting_vars]],axis=1)

            df1 = df1.reset_index()
            df2 = df2.reset_index()
            df1 = df1.drop('index', 1)
            df2 = df2.drop('index', 1)

            df1.columns = ['SideA.' + x  for x in df1.columns] 
            df2.columns = ['SideB.' + x  for x in df2.columns]

            df1 = df1.rename(columns={'SideA.filter_key':'filter_key'})
            df2 = df2.rename(columns={'SideB.filter_key':'filter_key'})

            #dff = pd.concat([aa_df,bb_df],axis=1)
            dff = merge(df1, df2, on='filter_key')
            training_df.append(dff)
                #key_index.append(i)
            #else:
            #no_pair_ids.append([aa1[(aa1['filter_key']=='key') & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values[0]])
               # no_pair_ids.append(aa1[(aa1['filter_key']== key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values[0])
    
        else:
            no_pair_ids.append([aa1[(aa1['filter_key']==key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values])
            no_pair_ids.append([bb1[(bb1['filter_key']==key) & (bb1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side0_UniqueIds'].values])
            

no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])


# In[163]:


training_df1 = pd.concat(training_df)


# In[164]:


training_df1.shape


# In[165]:


training_df1['open_key'] = training_df1['SideB.ViewData.Side0_UniqueIds'].astype(str) + training_df1['SideA.ViewData.Side1_UniqueIds'].astype(str)


# In[166]:


df['open_key'] = df['ViewData.Side0_UniqueIds'].astype(str) + df['ViewData.Side1_UniqueIds'].astype(str)


# In[167]:


keys_with_umr_umb = df[df['open_key'].isin(training_df1['open_key'].unique())]['open_key'].unique()


# In[168]:


training_df1.loc[training_df1['open_key'].isin(keys_with_umr_umb),'key_with_umr_umb'] = 1


# In[169]:


tt = df[df['open_key'].isin(training_df1['open_key'].unique())][['open_key','ViewData.Status']].drop_duplicates()
tt.columns =['open_key','new_label']


# In[170]:



training_df1 = pd.merge(training_df1, tt,on='open_key',how='left')


# In[171]:


#training_df1[['SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side']]

training_df1['SideB.ViewData.BreakID_B_side'] = training_df1['SideB.ViewData.BreakID_B_side'].astype('int64')
training_df1['SideA.ViewData.BreakID_A_side'] = training_df1['SideA.ViewData.BreakID_A_side'].astype('int64')


# In[172]:


training_df1 = training_df1.reset_index()
training_df1 = training_df1.drop('index',1)


# In[173]:



#sample['filter_key'] = sample['ViewData.Mapped Custodian Account'].astype(str) + sample['ViewData.B-P Currency'].astype(str)

umr_all_day = sample[sample['ViewData.Status'].isin(['UMR'])]

umr_all_day = umr_all_day[(umr_all_day['flag_side0']==1) & (umr_all_day['flag_side1']==1)]
umr_all_day = umr_all_day.reset_index()
umr_all_day = umr_all_day.drop('index', 1)


# In[174]:


#sample[sample['ViewData.Side0_UniqueIds']=='131_379726394_Advent Geneva']


# In[175]:


#sample['filter_key'] = sample['ViewData.Mapped Custodian Account'].astype(str) + sample['ViewData.B-P Currency'].astype(str)

umb_all_day = sample[sample['ViewData.Status'].isin(['UMB'])]

umb_all_day = umb_all_day[(umb_all_day['flag_side0']==1) & (umb_all_day['flag_side1']==1)]
umb_all_day = umb_all_day.reset_index()
umb_all_day = umb_all_day.drop('index', 1)


# In[176]:


umt_all_day = sample[sample['ViewData.Status'].isin(['UMT'])]

umt_all_day = umt_all_day[(umt_all_day['flag_side0']==1) & (umt_all_day['flag_side1']==1)]
umt_all_day = umt_all_day.reset_index()
umt_all_day = umt_all_day.drop('index', 1)


# In[177]:


umr_df = []

for i in range(len(umr_all_day)):
    side1 = umr_all_day.loc[i, 'ViewData.Side0_UniqueIds']
    side2 = umr_all_day.loc[i, 'ViewData.Side1_UniqueIds']
    first_record_side1 = sample[sample['ViewData.Side0_UniqueIds']==side1][common_cols].head(1)
    first_record_side2 = sample[sample['ViewData.Side1_UniqueIds']==side2][common_cols].head(1)
    
    first_record_side1 = first_record_side1.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})
    first_record_side2 = first_record_side2.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
    
    first_record_side1 = first_record_side1.reset_index()
    first_record_side2 = first_record_side2.reset_index()
    
    first_record_side1 = first_record_side1.drop('index', 1)
    first_record_side2 = first_record_side2.drop('index', 1)
    
    first_record_side1.columns = ['SideB.' + x for x in first_record_side1.columns] 
    first_record_side2.columns = ['SideA.' + x for x in first_record_side2.columns]
            
    umr_new = pd.concat([first_record_side1,first_record_side2],axis=1)
    umr_df.append(umr_new)


# In[178]:


umb_all_day = umb_all_day[umb_all_day['ViewData.Side0_UniqueIds'].isin(unique_side0_id_array_umb)]

umb_all_day = umb_all_day.reset_index()
umb_all_day = umb_all_day.drop('index',1)

umb_all_day = umb_all_day[umb_all_day['ViewData.Side1_UniqueIds'].isin(unique_side1_id_array_umb)]

umb_all_day = umb_all_day.reset_index()
umb_all_day = umb_all_day.drop('index',1)


# In[179]:


umb_all_day_new = umb_all_day[['ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds']].drop_duplicates()


# In[180]:


umb_all_day_new = umb_all_day_new.reset_index()
umb_all_day_new = umb_all_day_new.drop('index',1)


# In[181]:


ss = umb_all_day_new['ViewData.Side1_UniqueIds'].value_counts().reset_index()


# In[182]:


umb_all_day_new = umb_all_day_new[umb_all_day_new['ViewData.Side1_UniqueIds'].isin(ss[ss['ViewData.Side1_UniqueIds']<2]['index'].unique())]
umb_all_day_new = umb_all_day_new.reset_index()
umb_all_day_new = umb_all_day_new.drop('index',1)


# In[183]:


rr = umb_all_day_new['ViewData.Side0_UniqueIds'].value_counts().reset_index()


# In[184]:


umb_all_day_new = umb_all_day_new[umb_all_day_new['ViewData.Side0_UniqueIds'].isin(rr[rr['ViewData.Side0_UniqueIds']<2]['index'].unique())]
umb_all_day_new = umb_all_day_new.reset_index()
umb_all_day_new = umb_all_day_new.drop('index',1)


# In[185]:


umb_df = []

for i in range(len(umb_all_day_new)):
    side1 = umb_all_day_new.loc[i, 'ViewData.Side0_UniqueIds']
    side2 = umb_all_day_new.loc[i, 'ViewData.Side1_UniqueIds']
    first_record_side1 = sample[(sample['ViewData.Side0_UniqueIds']==side1) & (sample['ViewData.Status'].isin(['OB','SDB','UOB','UDB','SPM']))][common_cols].head(1)
    first_record_side2 = sample[(sample['ViewData.Side1_UniqueIds']==side2) & (sample['ViewData.Status'].isin(['OB','SDB','UOB','UDB','SPM']))][common_cols].head(1)
    
    first_record_side1 = first_record_side1.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})
    first_record_side2 = first_record_side2.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
    
    first_record_side1 = first_record_side1.reset_index()
    first_record_side2 = first_record_side2.reset_index()
    
    first_record_side1 = first_record_side1.drop('index', 1)
    first_record_side2 = first_record_side2.drop('index', 1)
    
    first_record_side1.columns = ['SideB.' + x for x in first_record_side1.columns] 
    first_record_side2.columns = ['SideA.' + x for x in first_record_side2.columns]
            
    umb_new = pd.concat([first_record_side1,first_record_side2],axis=1)
    umb_df.append(umb_new)
    #key_index.append(i)


# In[186]:


#train_full_new['SideA.ViewData.Side1_UniqueIds'].value_counts()

umt_all_day_new = umt_all_day[['ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds']].drop_duplicates()
umt_all_day_new = umt_all_day_new.reset_index()
umt_all_day_new = umt_all_day_new.drop('index',1)


# In[187]:


umt_df = []

for i in range(len(umt_all_day_new)):
    side1 = umt_all_day_new.loc[i, 'ViewData.Side0_UniqueIds']
    side2 = umt_all_day_new.loc[i, 'ViewData.Side1_UniqueIds']
    first_record_side1 = sample[sample['ViewData.Side0_UniqueIds']==side1][common_cols].head(1)
    first_record_side2 = sample[sample['ViewData.Side1_UniqueIds']==side2][common_cols].head(1)
    
    first_record_side1 = first_record_side1.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})
    first_record_side2 = first_record_side2.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
    
    first_record_side1 = first_record_side1.reset_index()
    first_record_side2 = first_record_side2.reset_index()
    
    first_record_side1 = first_record_side1.drop('index', 1)
    first_record_side2 = first_record_side2.drop('index', 1)
    
    first_record_side1.columns = ['SideB.' + x for x in first_record_side1.columns] 
    first_record_side2.columns = ['SideA.' + x for x in first_record_side2.columns]
            
    umt_new = pd.concat([first_record_side1,first_record_side2],axis=1)
    umt_df.append(umt_new)
    #key_index.append(i)


# In[188]:


umr_final = pd.concat(umr_df)


# In[189]:


umb_final = pd.concat(umb_df)


# In[190]:


umb_final.shape


# In[191]:


umt_final = pd.concat(umt_df)


# In[192]:


umr_final = umr_final.reset_index()
umr_final = umr_final.drop('index',1)


# In[193]:


umb_final = umb_final.reset_index()
umb_final = umb_final.drop('index',1)


# In[194]:


umt_final = umt_final.reset_index()
umt_final = umt_final.drop('index',1)


# In[195]:


umr_final['label'] = 'UMR_One_to_One'

umb_final['label'] = 'UMB_One_to_One'


# In[196]:


umt_final['label'] = 'UMT_One_to_One'


# In[197]:


training_df1['label'] = 'No-Pair'


# In[198]:


training_df1 = training_df1.reset_index()
training_df1 = training_df1.drop('index',1)


# In[199]:


#training_df1[training_df1['new_label']=='UMR'][['SideA.ViewData.B-P Net Amount','SideB.ViewData.Accounting Net Amount']]
training_df1.loc[~training_df1['new_label'].isnull(),'label'] = training_df1.loc[~training_df1['new_label'].isnull(),'new_label']


# In[200]:


training_df1['label'] = training_df1.apply(lambda x: "UMR_One_to_One" if x['label']=='UMR' else("UMB_One_to_One" if x['label']=='UMB' else("UMT_One_to_One" if x['label']=='UMT' else "No-Pair")), axis=1)


# In[201]:


train_full = pd.concat([training_df1.drop(['open_key', 'key_with_umr_umb', 'new_label'],1), umr_final,umt_final,umb_final], axis=0)
train_full = train_full.reset_index()
train_full = train_full.drop('index', 1)


# In[202]:


umr_unique = train_full[train_full['label'].isin(['UMR'])][['SideA.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.Side0_UniqueIds','SideB.ViewData.Side1_UniqueIds']]
umr_unique = umr_unique.reset_index()
umr_unique = umr_unique.drop('index',1)


# In[203]:


train_full['duplicate_flag'] = 0

for i in range(len(umr_unique)):
    side1 = umr_unique.loc[i, 'SideA.ViewData.Side0_UniqueIds']
    side2 = umr_unique.loc[i, 'SideB.ViewData.Side1_UniqueIds']
    train_full.loc[((train_full['SideA.ViewData.Side0_UniqueIds']==side1) & (train_full['SideB.ViewData.Side1_UniqueIds']==side2) & (~train_full['label'].isin(['UMR']))),'duplicate_flag']=1


# In[204]:


train_full = train_full[train_full['duplicate_flag']==0]
train_full = train_full.reset_index()
train_full = train_full.drop('index',1)


# ## One to Many

# In[205]:


#ViewData.B-P Currency
sample['ViewData.B-P Currency'] = sample['ViewData.B-P Currency'].astype(str)
sample['ViewData.Accounting Currency'] = sample['ViewData.Accounting Currency'].astype(str)
sample['filter_key'] = sample.apply(lambda x: x['ViewData.Mapped Custodian Account'] + x['ViewData.B-P Currency'] if x['Trans_side']=='A_side' else x['ViewData.Mapped Custodian Account'] + x['ViewData.Accounting Currency'], axis=1)

#sample.loc[sample[sample['Trans_side']=='A_side'], 'filter_key'] = sample.iloc[sample[sample['Trans_side']=='A_side'], 'ViewData.Mapped Custodian Account'].astype(str) + sample.iloc[sample[sample['Trans_side']=='A_side'], 'ViewData.B-P Currency'].astype(str)

#sample.loc[sample[sample['Trans_side']=='B_side'], 'filter_key'] = sample.iloc[sample[sample['Trans_side']=='B_side'], 'ViewData.Mapped Custodian Account'].astype(str) + sample.iloc[sample[sample['Trans_side']=='B_side'], 'ViewData.Accounting Currency'].astype(str)

#sample['filter_key'] = sample['ViewData.Mapped Custodian Account'].astype(str) + sample['ViewData.B-P Currency'].astype(str)


# In[206]:


one_to_many = sample[(sample['flag_side0']==1) & (sample['flag_side1']>1) & (sample['ViewData.Status'].isin(['UMR','UMT','UMB']))]

one_to_many[one_to_many['ViewData.Side0_UniqueIds'] !='nan']['ViewData.Status'].value_counts()

comb = one_to_many[one_to_many['ViewData.Side0_UniqueIds'] =='nan']
comb_and_match = one_to_many[one_to_many['ViewData.Side0_UniqueIds'] !='nan']

####################################################################################

many_to_one = sample[(sample['flag_side0']>1) & (sample['flag_side1']==1) & (sample['ViewData.Status'].isin(['UMR','UMT','UMB']))]

#one_to_many[one_to_many['ViewData.Side0_UniqueIds'] !='nan']['ViewData.Status'].value_counts()

comb_mto = many_to_one[many_to_one['ViewData.Side1_UniqueIds'] =='nan']
comb_and_match_mto = many_to_one[many_to_one['ViewData.Side1_UniqueIds'] !='nan']

####################################################################################

many_to_many = sample[(sample['flag_side0']>1) & (sample['flag_side1']>1) & (sample['ViewData.Status'].isin(['UMR','UMT','UMB']))]

#one_to_many[one_to_many['ViewData.Side0_UniqueIds'] !='nan']['ViewData.Status'].value_counts()

comb_mtm = many_to_many[many_to_many['ViewData.Side0_UniqueIds'] =='nan']
comb_and_match_mtm = many_to_many[(many_to_many['ViewData.Side0_UniqueIds'] !='nan') & (many_to_many['ViewData.Side1_UniqueIds'] !='nan')]

####################################################################################


# ## OTM loop

# In[207]:


comb_and_match = comb_and_match.reset_index()
comb_and_match = comb_and_match.drop('index',1)
#'378_153134435_Advent Geneva
#comb_and_match['ViewData.Side0_UniqueIds'].unique()
otm_pool =[]
for i,val in enumerate(comb_and_match['ViewData.Side0_UniqueIds'].unique()):
    #print(val)
    
    if sample[sample['ViewData.Side0_UniqueIds'] ==val].empty ==False and sample[sample['ViewData.Side0_UniqueIds'] ==val].head(1)['ViewData.Status'].values[0] in ['OB','SMB','UOB','SDB','SPM','SDB']  and sample[sample['ViewData.Side0_UniqueIds'] ==val].tail(1)['ViewData.Status'].values[0] in ['UMT','UMR','UMB']:
        #print('Yes')
        acc_side = sample[sample['ViewData.Side0_UniqueIds'] ==val].head(1)[common_cols]
        #acc_side = acc_side.drop('ViewData.Side1_UniqueIds',1)
        acc_side = acc_side.reset_index()
        acc_side = acc_side.drop('index',1)
        acc_side = acc_side.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})
        acc_side.columns = ['SideB.' + x for x in acc_side.columns] 

        #print(val)
        #print(acc_side)
        for j in comb_and_match.loc[comb_and_match['ViewData.Side0_UniqueIds']==val,'ViewData.Side1_UniqueIds'].head(1).values[0].split(','):

            pb_side = sample[sample['ViewData.Side1_UniqueIds'] ==j].head(1)[common_cols]
            pb_side = pb_side.reset_index()
            pb_side = pb_side.drop('index',1)
            pb_side = pb_side.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
            pb_side.columns = ['SideA.' + x for x in pb_side.columns]

            final_data = pd.concat([acc_side,pb_side], axis=1)
            final_data['SideA.ViewData.Side1_UniqueIds']= j
            otm_pool.append(final_data.reset_index().drop('index',1))

full_otm_data = pd.concat(otm_pool,axis=0)
full_otm_data['label'] ='Partial_match'


# In[208]:


full_otm_data = full_otm_data.reset_index()
full_otm_data = full_otm_data.drop('index',1)


# ## Many to One loop

# In[209]:



comb_and_match_mto = comb_and_match_mto.reset_index()
comb_and_match_mto = comb_and_match_mto.drop('index',1)

mto_pool =[]
for i,val in enumerate(comb_and_match_mto['ViewData.Side1_UniqueIds'].unique()):
    
    if sample[sample['ViewData.Side1_UniqueIds'] ==val].empty ==False  and sample[sample['ViewData.Side1_UniqueIds'] ==val].head(1)['ViewData.Status'].values[0] in ['OB','SMB','UOB','SDB','SPM','SDB']  and sample[sample['ViewData.Side1_UniqueIds'] ==val].tail(1)['ViewData.Status'].values[0] in ['UMT','UMR','UMB']:

        pb_side = sample[sample['ViewData.Side1_UniqueIds'] ==val].head(1)[common_cols]
        #acc_side = acc_side.drop('ViewData.Side1_UniqueIds',1)
        pb_side = pb_side.reset_index()
        pb_side = pb_side.drop('index',1)
        pb_side = pb_side.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
        pb_side.columns = ['SideA.' + x for x in pb_side.columns] 

        #print(val)
        #print(acc_side)
        for j in comb_and_match_mto.loc[comb_and_match_mto['ViewData.Side1_UniqueIds']==val,'ViewData.Side0_UniqueIds'].head(1).values[0].split(','):
            
            acc_side = sample[sample['ViewData.Side0_UniqueIds'] ==j].head(1)[common_cols]
            acc_side = acc_side.reset_index()
            acc_side = acc_side.drop('index',1)
            acc_side = acc_side.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})
            acc_side.columns = ['SideB.' + x for x in acc_side.columns]

            final_data = pd.concat([pb_side,acc_side], axis=1)
            final_data['SideB.ViewData.Side0_UniqueIds']= j
            mto_pool.append(final_data)

full_mto_data = pd.concat(mto_pool,axis=0)
full_mto_data['label'] ='Partial_match_MTO'


# In[210]:


full_otm_data = full_otm_data[~full_otm_data['SideA.ViewData.B-P Net Amount'].isnull()]
full_otm_data = full_otm_data.reset_index()
full_otm_data = full_otm_data.drop('index',1)


# In[211]:


full_mto_data = full_mto_data[~full_mto_data['SideB.ViewData.Accounting Net Amount'].isnull()]
full_mto_data = full_mto_data.reset_index()
full_mto_data = full_mto_data.drop('index',1)


# In[212]:


full_otm_data['group_num'] = full_otm_data.groupby(['SideB.ViewData.Accounting Net Amount']).ngroup()


# In[213]:


group_data = full_otm_data.groupby(['group_num'])['SideB.ViewData.Accounting Net Amount'].max().reset_index()


# In[214]:


train_full_new = pd.concat([train_full, full_otm_data,full_mto_data], axis=0)


# ## MTM

# In[215]:


equity_tt = ['EQUITY SWAP','EQUITY SWAP LONG FINANCING',
'EQUITY SWAP LONG PERFORMANCE',
'EQUITY SWAP SHORT DIVIDEND',
'EQUITY SWAP SHORT PERFORMANCE',
'EQUITY SWAP SHORT FINANCING',
'EQUITY SWAP RESET PAYMENT',
'EQUITY SWAP LONG DIVIDEND',
'QRBH1- EQSWAP DIV CLIENT TAX',
'QO99F- EQSWAP DIV CLIENT TAX',
'QRDE7- EQSWAP DIV CLIENT TAX',
'QRDE8- EQSWAP DIV CLIENT TAX',
'QPPE1- EQSWAP DIV CLIENT TAX',
'QRDE7- EQSWAP DIV CLIENT TAX',
'QRVS8- EQSWAP DIV CLIENT TAX',
'QPWH9- EQSWAP DIV CLIENT TAX',
'QRY9U- EQSWAP DIV CLIENT TAX',
'EQUITY SWAP LONG FEE']


# In[216]:


## FinalTraining File


# In[217]:


train_full_new = train_full_new.reset_index()
train_full_new = train_full_new.drop('index',1)


# In[218]:


train_full_new = train_full_new[train_full_new['SideB.ViewData.Status'].isin(['OB','SPM','SDB','UDB','UOB','CNF','CMF'])]
train_full_new = train_full_new[train_full_new['SideA.ViewData.Status'].isin(['OB','SPM','SDB','UDB','UOB','CNF','CMF'])]
train_full_new = train_full_new.reset_index()
train_full_new = train_full_new.drop('index', 1)
                                


# In[219]:


model_cols = [
    'SideA.ViewData.Accounting Net Amount', 
        #'SideA.ViewData.Age',
      # 'SideA.ViewData.Age WK', 'SideA.ViewData.Asset Type Category',
      'SideA.ViewData.B-P Net Amount', 
        #'SideA.ViewData.Base Net Amount',
      'SideA.ViewData.CUSIP', 
    #'SideA.ViewData.Cancel Amount',
      # 'SideA.ViewData.Cancel Flag', 'SideA.ViewData.Commission',
       'SideA.ViewData.Currency', 
      #  'SideA.ViewData.Custodian',
      # 'SideA.ViewData.Custodian Account', 
       'SideA.ViewData.Description',
     'SideA.ViewData.Department',
      # 'SideA.ViewData.ExpiryDate',
        'SideA.ViewData.Fund',
      'SideA.ViewData.ISIN', 
    #'SideA.ViewData.Investment Type',
      # 'SideA.ViewData.Mapped Custodian Account',
      # 'SideA.ViewData.Net Amount Difference',
       #'SideA.ViewData.Net Amount Difference Absolute',
       'SideA.ViewData.OTE Ticker', 
    'SideA.ViewData.Investment ID',
    #'SideA.ViewData.Price',
      # 'SideA.ViewData.Prime Broker', 
       #       'SideA.ViewData.Quantity',
       #'SideA.ViewData.SEDOL', 
        #'SideA.ViewData.SPM ID',
       'SideA.ViewData.Settle Date', 
    #'SideA.ViewData.Strike Price',
       #'SideA.Date', 
       # 'SideA.ViewData.Ticker', 
        'SideA.ViewData.Trade Date',
       #'SideA.ViewData.Transaction Category',
       'SideA.ViewData.Transaction Type', 
    #'SideA.ViewData.Underlying Cusip',
       #'SideA.ViewData.Underlying ISIN', 'SideA.ViewData.Underlying Sedol',
      # 'SideA.filter_key', 'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side',
        'SideB.ViewData.Accounting Net Amount',
       #'SideB.ViewData.Age', 'SideB.ViewData.Age WK',
       #'SideB.ViewData.Asset Type Category', 
     'SideB.ViewData.B-P Net Amount',
      # 'SideB.ViewData.Base Net Amount',
            'SideB.ViewData.CUSIP',
       #'SideB.ViewData.Cancel Amount', 
        #'SideB.ViewData.Cancel Flag',
       #'SideB.ViewData.Commission',
            'SideB.ViewData.Currency',
       #'SideB.ViewData.Custodian', 'SideB.ViewData.Custodian Account',
       'SideB.ViewData.Description', 
     'SideB.ViewData.Department',
              #'SideB.ViewData.ExpiryDate',
      'SideB.ViewData.Fund', 
           'SideB.ViewData.ISIN',
       #'SideB.ViewData.Investment Type',
      # 'SideB.ViewData.Mapped Custodian Account',
      # 'SideB.ViewData.Net Amount Difference',
      # 'SideB.ViewData.Net Amount Difference Absolute',
       'SideB.ViewData.OTE Ticker',
    'SideB.ViewData.Investment ID',
    #'SideB.ViewData.Price',
       #'SideB.ViewData.Prime Broker',
        #      'SideB.ViewData.Quantity',
       #'SideB.ViewData.SEDOL', 
        #'SideB.ViewData.SPM ID',
       'SideB.ViewData.Settle Date', 
    #'SideB.ViewData.Strike Price',
       #'SideB.Date',
       # 'SideB.ViewData.Ticker', 
    'SideB.ViewData.Trade Date',
       #'SideB.ViewData.Transaction Category',
       'SideB.ViewData.Transaction Type',
    #'SideB.ViewData.Underlying Cusip',
       #'SideB.ViewData.Underlying ISIN', 'SideB.ViewData.Underlying Sedol',
#'SideB.filter_key', 
        'SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 
              'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','label']


y_col = ['label']


# In[220]:


train_full_new['label'].value_counts()


# In[221]:


#no_pair = train_full_new[train_full_new['label']=='No-Pair'].sample(frac =.1)

#rest = train_full_new[train_full_new['label']!='No-Pair']

#train_full_new = pd.concat([no_pair,rest])


# In[222]:


train_full_new['label'].value_counts(normalize=True)


# In[223]:


train_full_new['SideB.ViewData.CUSIP'] = train_full_new['SideB.ViewData.CUSIP'].str.split(".",expand=True)[0]
train_full_new['SideA.ViewData.CUSIP'] = train_full_new['SideA.ViewData.CUSIP'].str.split(".",expand=True)[0]


# In[224]:


#train_full_new = train_full_new[~train_full_new['label'].isin(['UMR'])]
train_full_new = train_full_new.reset_index()
train_full_new = train_full_new.drop('index', 1)
train_full_new['label'].value_counts(normalize=True)


# In[225]:


train_full_new['SideA.ViewData.ISIN'] = train_full_new['SideA.ViewData.ISIN'].astype(str)
train_full_new['SideB.ViewData.ISIN'] = train_full_new['SideB.ViewData.ISIN'].astype(str)
train_full_new['SideA.ViewData.CUSIP'] = train_full_new['SideA.ViewData.CUSIP'].astype(str)
train_full_new['SideB.ViewData.CUSIP'] = train_full_new['SideB.ViewData.CUSIP'].astype(str)
train_full_new['SideA.ViewData.Currency'] = train_full_new['SideA.ViewData.Currency'].astype(str)
train_full_new['SideB.ViewData.Currency'] = train_full_new['SideB.ViewData.Currency'].astype(str)

train_full_new['SideA.ViewData.Fund'] = train_full_new['SideA.ViewData.Fund'].astype(str)
train_full_new['SideA.ViewData.Trade Date'] = train_full_new['SideA.ViewData.Trade Date'].astype(str)
train_full_new['SideA.ViewData.Settle Date'] = train_full_new['SideA.ViewData.Settle Date'].astype(str)

train_full_new['SideB.ViewData.Fund'] = train_full_new['SideB.ViewData.Fund'].astype(str)
train_full_new['SideB.ViewData.Trade Date'] = train_full_new['SideB.ViewData.Trade Date'].astype(str)
train_full_new['SideB.ViewData.Settle Date'] = train_full_new['SideB.ViewData.Settle Date'].astype(str)

train_full_new['SideA.ISIN_NA'] =  train_full_new.apply(lambda x: 1 if x['SideA.ViewData.ISIN']=='nan' else 0, axis=1)
train_full_new['SideB.ISIN_NA'] =  train_full_new.apply(lambda x: 1 if x['SideB.ViewData.ISIN']=='nan' else 0, axis=1)

train_full_new['ISIN_match'] = train_full_new.apply(lambda x: 1 if x['SideA.ViewData.ISIN']==x['SideB.ViewData.ISIN'] else 0, axis=1)
train_full_new['CUSIP_match'] = train_full_new.apply(lambda x: 1 if x['SideA.ViewData.CUSIP']==x['SideB.ViewData.CUSIP'] else 0, axis=1)
train_full_new['Currency_match'] = train_full_new.apply(lambda x: 1 if x['SideA.ViewData.Currency']==x['SideB.ViewData.Currency'] else 0, axis=1)


train_full_new['Trade_Date_match'] = train_full_new.apply(lambda x: 1 if x['SideA.ViewData.Trade Date']==x['SideB.ViewData.Trade Date'] else 0, axis=1)
train_full_new['Settle_Date_match'] = train_full_new.apply(lambda x: 1 if x['SideA.ViewData.Settle Date']==x['SideB.ViewData.Settle Date'] else 0, axis=1)
train_full_new['Fund_match'] = train_full_new.apply(lambda x: 1 if x['SideA.ViewData.Fund']==x['SideB.ViewData.Fund'] else 0, axis=1)


# In[226]:


#train_full_new[(train_full_new['Fund_match']==0) & (train_full_new['label']=='UMR_One_to_One')]['SideA.ViewData.Fund'].value_counts()


# In[227]:


train_full_new['Amount_diff_1'] = train_full_new['SideA.ViewData.Accounting Net Amount'] - train_full_new['SideB.ViewData.B-P Net Amount']
train_full_new['Amount_diff_2'] = train_full_new['SideB.ViewData.Accounting Net Amount'] - train_full_new['SideA.ViewData.B-P Net Amount']


# In[228]:


train_full_new['Trade_date_diff'] = (pd.to_datetime(train_full_new['SideA.ViewData.Trade Date']) - pd.to_datetime(train_full_new['SideB.ViewData.Trade Date'])).dt.days

train_full_new['Settle_date_diff'] = (pd.to_datetime(train_full_new['SideA.ViewData.Settle Date'],errors = 'coerce') - pd.to_datetime(train_full_new['SideB.ViewData.Settle Date'], errors = 'coerce')).dt.days


# In[229]:


#train_full_new[['SideA.ViewData.Settle Date', 'SideB.ViewData.Settle Date','Settle_date_diff']]


# In[231]:


model_cols = ['SideA.ViewData.Accounting Net Amount',
 'SideA.ViewData.B-P Net Amount',
 #'SideA.ViewData.CUSIP',
 #'SideA.ViewData.Currency',

'SideA.ViewData.Price',
'SideA.ViewData.Quantity',
'SideA.ViewData.Cancel Flag',
              
              
'SideA.ViewData.OTE Ticker',
 'SideA.ViewData.Investment ID',             
              
              
'SideA.ViewData.Transaction Type',
 'SideA.ViewData.Description',
               'SideA.ViewData.Department',
 #'SideA.ViewData.ISIN',
 'SideB.ViewData.Accounting Net Amount',
 'SideB.ViewData.B-P Net Amount',
 #'SideB.ViewData.CUSIP',
 #'SideB.ViewData.Currency',
              

'SideB.ViewData.Price',
'SideB.ViewData.Quantity',
'SideB.ViewData.Cancel Flag',
              
'SideB.ViewData.OTE Ticker',
 'SideB.ViewData.Investment ID', 
              
'SideB.ViewData.Transaction Type',
 'SideB.ViewData.Description',
               'SideB.ViewData.Department',
 #'SideB.ViewData.ISIN',
 'SideB.ViewData.Status',
 'SideB.ViewData.BreakID_B_side',
 'SideA.ViewData.Status',
 'SideA.ViewData.BreakID_A_side',
    'SideA.ISIN_NA',
    'SideB.ISIN_NA',
'ISIN_match',
  'CUSIP_match',
'Currency_match',
'Trade_Date_match',
'Settle_Date_match',
'Fund_match',             
'Amount_diff_1',
'Amount_diff_2',
'Trade_date_diff',
'Settle_date_diff',
 'label']


# ## Description code

# In[232]:


com = pd.read_csv('desc cat with naveen oaktree.csv')


# In[233]:


cat_list = list(set(com['Pairing']))


# In[234]:


import re


# In[235]:


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


# In[236]:


#df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))

train_full_new['SideA.desc_cat'] = train_full_new['SideA.ViewData.Description'].apply(lambda x : descclean(x,cat_list))
train_full_new['SideB.desc_cat'] = train_full_new['SideB.ViewData.Description'].apply(lambda x : descclean(x,cat_list))


# In[237]:


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
        


# In[238]:



#df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))

train_full_new['SideA.desc_cat'] = train_full_new['SideA.desc_cat'].apply(lambda x : currcln(x))
train_full_new['SideB.desc_cat'] = train_full_new['SideB.desc_cat'].apply(lambda x : currcln(x))


# In[239]:



com = com.drop(['var','Catogery'], axis = 1)

com = com.drop_duplicates()

com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


# In[240]:


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


## Transaction Type, Investment Type and Asset Category


# In[241]:


#df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))

train_full_new['SideA.new_desc_cat'] = train_full_new['SideA.desc_cat'].apply(lambda x : catcln1(x,com))
train_full_new['SideB.new_desc_cat'] = train_full_new['SideB.desc_cat'].apply(lambda x : catcln1(x,com))


# In[242]:



comp = ['inc','stk','corp ','llc','pvt','plc']
#df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

train_full_new['SideA.new_desc_cat'] = train_full_new['SideA.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

train_full_new['SideB.new_desc_cat'] = train_full_new['SideB.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)


# In[243]:


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
        return x


# In[244]:


#df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))

train_full_new['SideA.new_desc_cat'] = train_full_new['SideA.new_desc_cat'].apply(lambda x : desccat(x))
train_full_new['SideB.new_desc_cat'] = train_full_new['SideB.new_desc_cat'].apply(lambda x : desccat(x))

train_full_new['SideB.new_desc_cat'].value_counts()


# ## Prime Broker

# In[245]:


train_full_new['new_pb'] = train_full_new['SideA.ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)


# In[246]:


new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}


# In[247]:


def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x


# In[248]:


train_full_new['SideA.ViewData.Prime Broker'] = train_full_new['SideA.ViewData.Prime Broker'].fillna('kkk')


# In[249]:


train_full_new['new_pb1'] = train_full_new.apply(lambda x : x['new_pb'] if x['SideA.ViewData.Prime Broker']=='kkk' else x['SideA.ViewData.Prime Broker'],axis = 1)


# ## Transaction type, Investment type and Asset Type Category

# In[250]:


import pandas as pd
import dask.dataframe as dd
import glob
import math
from sklearn.feature_extraction.text import TfidfVectorizer
from dateutil.parser import parse
import operator
import itertools
from sklearn.feature_extraction.text import CountVectorizer
import re
import os
import numpy as np


#column_names = ['SideA.ViewData.Transaction Type', 'ViewData.Investment Type', 'ViewData.Asset Type Category', 'ViewData.Prime Broker', 'ViewData.Description']

trans_type_A_side = train_full_new['SideA.ViewData.Transaction Type']
trans_type_B_side = train_full_new['SideB.ViewData.Transaction Type']

asset_type_cat_A_side = train_full_new['SideA.ViewData.Asset Type Category']
asset_type_cat_B_side = train_full_new['SideB.ViewData.Asset Type Category']

invest_type_A_side = train_full_new['SideA.ViewData.Investment Type']
invest_type_B_side = train_full_new['SideB.ViewData.Investment Type']

prime_broker_A_side = train_full_new['SideA.ViewData.Prime Broker']
prime_broker_B_side = train_full_new['SideB.ViewData.Prime Broker']


# In[251]:



# LOWER CASE
trans_type_A_side = [str(item).lower() for item in trans_type_A_side]
trans_type_B_side = [str(item).lower() for item in trans_type_B_side]

asset_type_cat_A_side = [str(item).lower() for item in asset_type_cat_A_side]
asset_type_cat_B_side = [str(item).lower() for item in asset_type_cat_B_side]

invest_type_A_side = [str(item).lower() for item in invest_type_A_side]
invest_type_B_side = [str(item).lower() for item in invest_type_B_side]

prime_broker_A_side = [str(item).lower() for item in prime_broker_A_side]
prime_broker_B_side = [str(item).lower() for item in prime_broker_B_side]


# In[303]:


# UNIQUE VALUES
#trans_type = list(set(trans_type))
#asset_type_cat = list(set(asset_type_cat))
#invest_type = list(set(invest_type))
#prime_broker = list(set(prime_broker))


# In[252]:


split_trans_A_side = [item.split() for item in trans_type_A_side]
split_trans_B_side = [item.split() for item in trans_type_B_side]


split_asset_A_side = [item.split() for item in asset_type_cat_A_side]
split_asset_B_side = [item.split() for item in asset_type_cat_B_side]


split_invest_A_side = [item.split() for item in invest_type_A_side]
split_invest_B_side = [item.split() for item in invest_type_B_side]

split_prime_A_side = [item.split() for item in prime_broker_A_side]
split_prime_b_side = [item.split() for item in prime_broker_B_side]

# In[310]:


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
    


# In[253]:


## Transacion type

remove_nums_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_trans_A_side]
remove_nums_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_trans_B_side]

#remove_dates_A_side = [[item for item in sublist if not is_date(item)] for sublist in remove_nums_A_side]

#remove_dates_B_side = [[item for item in sublist if not is_date(item)] for sublist in remove_nums_B_side]

remove_dates_A_side = [[item for item in sublist if not (is_date_format(item) or date_edge_cases(item))] for sublist in remove_nums_A_side]
remove_dates_B_side = [[item for item in sublist if not (is_date_format(item) or date_edge_cases(item))] for sublist in remove_nums_B_side]


# Specific to clients already used on, will have to be edited for other edge cases
remove_amts_A_side = [[item for item in sublist if item[0] != '$'] for sublist in remove_dates_A_side]
remove_amts_B_side = [[item for item in sublist if item[0] != '$'] for sublist in remove_dates_B_side]


clean_adr_A_side = [(['ADR'] if 'adr' in item else item) for item in remove_amts_A_side]
clean_adr_B_side = [(['ADR'] if 'adr' in item else item) for item in remove_amts_B_side]

clean_tax_A_side = [(item[:2] if '30%' in item else item) for item in clean_adr_A_side]
clean_tax_B_side = [(item[:2] if '30%' in item else item) for item in clean_adr_B_side]

remove_ons_A_side = [(item[:item.index('on')] if 'on' in item else item) for item in clean_tax_A_side]
remove_ons_B_side = [(item[:item.index('on')] if 'on' in item else item) for item in clean_tax_B_side]

clean_eqswap_A_side = [(item[1:] if 'eqswap' in item else item) for item in remove_ons_A_side]
clean_eqswap_B_side = [(item[1:] if 'eqswap' in item else item) for item in remove_ons_B_side]

remove_mh_A_side = [[item for item in sublist if 'mh' not in item] for sublist in clean_eqswap_A_side]
remove_mh_B_side = [[item for item in sublist if 'mh' not in item] for sublist in clean_eqswap_B_side]

remove_ats_A_side = [(item[:item.index('@')] if '@' in item else item) for item in remove_mh_A_side]
remove_ats_B_side = [(item[:item.index('@')] if '@' in item else item) for item in remove_mh_B_side]

#remove_blanks_A_side = [item for item in remove_ats_A_side if item]
#remove_blanks_B_side = [item for item in remove_ats_B_side if item]


# In[319]:

cleaned_trans_types_A_side = [' '.join(item) for item in remove_ats_A_side]
cleaned_trans_types_B_side = [' '.join(item) for item in remove_ats_B_side]
#cleaned_trans_types = list(set(cleaned_trans_types))


# In[254]:


# # INVESTMENT TYPE

# # In[322]:


remove_nums_i_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_invest_A_side]
remove_nums_i_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_invest_B_side]

remove_dates_i_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_i_A_side]
remove_dates_i_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_i_B_side]

#remove_blanks_i_A_side = [item for item in remove_dates_i_A_side if item]
#remove_blanks_i_B_side = [item for item in remove_dates_i_B_side if item]
#remove_blanks_i[:10]


# # In[323]:

cleaned_invest_A_side = [' '.join(item) for item in remove_dates_i_A_side]
cleaned_invest_B_side = [' '.join(item) for item in remove_dates_i_B_side]
#cleaned_invest = list(set(cleaned_invest))


# In[255]:


cleaned_trans_types_B_side


# In[256]:


train_full_new.shape


# In[257]:


# # ASSET TYPE CATEGORY:

# # In[320]:


remove_nums_a_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_A_side]
remove_nums_a_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_B_side]

remove_dates_a_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_A_side]
remove_dates_a_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_B_side]
# remove_blanks_a = [item for item in remove_dates_a if item]
# # remove_blanks_a[:10]


# # In[321]:

cleaned_asset_A_side = [' '.join(item) for item in remove_dates_a_A_side]
cleaned_asset_B_side = [' '.join(item) for item in remove_dates_a_B_side]
# cleaned_asset = list(set(cleaned_asset))


# In[258]:


def mhreplaced(item):
    word1 = []
    word2 = []
    if (type(item) == str):
    
        for items in item.split(' '):
            if (type(items) == str):
                items = items.lower()
                if items.isdigit() == False:
                    word1.append(items)
                    
                for c in word1:
                    if c.endswith('MH')==False:
                        word2.append(c)
    
                words = ' '.join(word2)
                return words
    else:
        return item
    

def fundmatch(item):
    items = item.lower()
    items = item.replace(' ','') 
    return items

train_full_new['SideA.ViewData.Fund'] = train_full_new.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
train_full_new['SideB.ViewData.Fund'] = train_full_new.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)


#train_full_new['SideB.ViewData.Transaction Type'] = train_full_new['SideB.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))
#train_full_new['SideA.ViewData.Transaction Type'] = train_full_new['SideA.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[259]:


train_full_new['SideA.ViewData.Transaction Type'] = cleaned_trans_types_A_side
train_full_new['SideB.ViewData.Transaction Type'] = cleaned_trans_types_B_side

train_full_new['SideA.ViewData.Investment Type'] = cleaned_invest_A_side
train_full_new['SideB.ViewData.Investment Type'] = cleaned_invest_B_side

train_full_new['SideA.ViewData.Asset Category Type'] = cleaned_asset_A_side
train_full_new['SideB.ViewData.Asset Category Type'] = cleaned_asset_B_side


# In[260]:


#train_full_new['SideB.ViewData.Transaction Type'] = train_full_new['SideB.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))
#train_full_new['SideA.ViewData.Transaction Type'] = train_full_new['SideA.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[261]:


train_full_new['SideB.ViewData.Transaction Type'].unique()


# In[262]:


train_full_new['SideA.ViewData.Transaction Type'].unique()


# In[263]:


train_full_new.loc[train_full_new['SideA.ViewData.Transaction Type']=='int','SideA.ViewData.Transaction Type'] = 'interest'
train_full_new.loc[train_full_new['SideA.ViewData.Transaction Type']=='wires','SideA.ViewData.Transaction Type'] = 'wire'
train_full_new.loc[train_full_new['SideA.ViewData.Transaction Type']=='dividends','SideA.ViewData.Transaction Type'] = 'dividend'
train_full_new.loc[train_full_new['SideA.ViewData.Transaction Type']=='miscellaneous','SideA.ViewData.Transaction Type'] = 'misc'
train_full_new.loc[train_full_new['SideA.ViewData.Transaction Type']=='div','SideA.ViewData.Transaction Type'] = 'dividend'


# In[ ]:





# In[264]:


#train_full_new[train_full_new['label'].isin(['UMB_One_to_One'])]['SideB.ViewData.BreakID_B_side'].astype(int).value_counts().reset_index()


# In[265]:


uu = train_full_new[train_full_new['label'].isin(['UMR_One_to_One'])]['SideB.ViewData.BreakID_B_side'].astype(int).value_counts().reset_index()
vv = train_full_new[train_full_new['label'].isin(['UMR_One_to_One'])]['SideA.ViewData.BreakID_A_side'].astype(int).value_counts().reset_index()


# In[266]:


#vv[vv['SideA.ViewData.BreakID_A_side']>1]


# In[267]:


train_full_new = train_full_new[~train_full_new['SideB.ViewData.BreakID_B_side'].isin(uu[uu['SideB.ViewData.BreakID_B_side']>1]['index'].unique())]
train_full_new = train_full_new.reset_index()
train_full_new = train_full_new.drop('index', 1)


# In[268]:


train_full_new = train_full_new[~train_full_new['SideA.ViewData.BreakID_A_side'].isin(vv[vv['SideA.ViewData.BreakID_A_side']>1]['index'].unique())]
train_full_new = train_full_new.reset_index()
train_full_new = train_full_new.drop('index', 1)


# In[270]:


def a_keymatch(a_cusip, a_isin):
    
    pb_nan = 0
    a_common_key = 'NA' 
    if a_cusip=='nan' and a_isin =='nan':
        pb_nan =1
    elif(a_cusip!='nan' and a_isin == 'nan'):
        a_common_key = a_cusip
    elif(a_cusip =='nan' and a_isin !='nan'):
        a_common_key = a_isin
    else:
        a_common_key = a_isin
        
    return pd.Series([pb_nan, a_common_key])

def b_keymatch(b_cusip, b_isin):
    accounting_nan = 0
    b_common_key = 'NA'
    if b_cusip =='nan' and b_isin =='nan':
        accounting_nan =1
    elif (b_cusip!='nan' and b_isin == 'nan'):
        b_common_key = b_cusip
    elif(b_cusip =='nan' and b_isin !='nan'):
        b_common_key = b_isin
    else:
        b_common_key = b_isin
    return pd.Series([accounting_nan, b_common_key])

train_full_new[['SideB.ViewData.key_NAN','SideB.ViewData.Common_key']] = train_full_new.apply(lambda x: b_keymatch(x['SideB.ViewData.CUSIP'], x['SideB.ViewData.ISIN']), axis=1)
train_full_new[['SideA.ViewData.key_NAN','SideA.ViewData.Common_key']] = train_full_new.apply(lambda x: a_keymatch(x['SideA.ViewData.CUSIP'],x['SideA.ViewData.ISIN']), axis=1)


# In[271]:


train_full_new[['SideB.ViewData.key_NAN','SideB.ViewData.Common_key']] = train_full_new.apply(lambda x: b_keymatch(x['SideB.ViewData.CUSIP'], x['SideB.ViewData.ISIN']), axis=1)
train_full_new[['SideA.ViewData.key_NAN','SideA.ViewData.Common_key']] = train_full_new.apply(lambda x: a_keymatch(x['SideA.ViewData.CUSIP'],x['SideA.ViewData.ISIN']), axis=1)


# In[272]:


train_full_new['All_key_nan'] = train_full_new.apply(lambda x: 1 if x['SideB.ViewData.key_NAN']==1 and x['SideA.ViewData.key_NAN']==1 else 0, axis=1)


# In[274]:


#train_full_new[train_full_new['All_key_nan']==0]

train_full_new['SideB.ViewData.Common_key'] = train_full_new['SideB.ViewData.Common_key'].astype(str)
train_full_new['SideA.ViewData.Common_key'] = train_full_new['SideA.ViewData.Common_key'].astype(str)

train_full_new['new_key_match'] = train_full_new.apply(lambda x: 1 if x['SideB.ViewData.Common_key']==x['SideA.ViewData.Common_key'] and x['All_key_nan']==0 else 0, axis=1)


# In[297]:


#pd.set_option('max_rows', 5000)


# In[298]:


#train_full_new[train_full_new['label']=='No-Pair'][['SideA.ViewData.CUSIP','SideB.ViewData.Transaction Type', 'SideA.ViewData.Transaction Type','SideB.ViewData.CUSIP', 'SideA.ViewData.ISIN', 'SideB.ViewData.ISIN','SideA.ViewData.OTE Ticker','SideA.ViewData.Fund','SideB.ViewData.Fund','label']]


# In[299]:


#umb_final['SideB.ViewData.BreakID_B_side'].astype(int).value_counts()


# In[275]:


train_full_new = train_full_new[~((train_full_new['label'].isin(['UMR_One_to_One','UMT_One_to_One','UMB_One_to_One'])) & (train_full_new['Trade_date_diff'].isnull()))]
train_full_new = train_full_new.reset_index()
train_full_new = train_full_new.drop('index',1)


# In[ ]:





# In[276]:


train_full_new1 = train_full_new.copy()


# In[277]:


train_full_new1['ViewData.Combined Transaction Type'] = train_full_new1['SideA.ViewData.Transaction Type'].astype(str) + train_full_new1['SideB.ViewData.Transaction Type'].astype(str)
train_full_new1['ViewData.Combined Fund'] = train_full_new1['SideA.ViewData.Fund'].astype(str) + train_full_new1['SideB.ViewData.Fund'].astype(str)


# In[278]:


#train_full_new1 = train_full_new1[train_full_new1['label'].isin(['UMR_One_to_One','No-Pair'])]
#train_full_new1 = train_full_new1.reset_index()
#train_full_new1 = train_full_new1.drop('index',1)


# In[279]:


model_cols = [
    'SideA.ViewData.Accounting Net Amount',
 'SideA.ViewData.B-P Net Amount',
 'SideA.ViewData.Price',
 'SideA.ViewData.Quantity',
    'SideA.ViewData.Cancel Flag',
    'SideA.ViewData.Description',
     'SideA.ViewData.Department',
#'SideA.ViewData.OTE Ticker',
# 'SideA.ViewData.Investment ID',
# 'SideA.ViewData.Transaction Type',
 'SideB.ViewData.Accounting Net Amount',
 'SideB.ViewData.B-P Net Amount',
 'SideB.ViewData.Price',
 'SideB.ViewData.Quantity',
    'SideB.ViewData.Cancel Flag',
    'SideB.ViewData.Description',
     'SideB.ViewData.Department',
#    'SideB.ViewData.OTE Ticker',
# 'SideB.ViewData.Investment ID',
    
# 'SideB.ViewData.Transaction Type',
 'SideB.ViewData.Status',
 'SideB.ViewData.BreakID_B_side',
 'SideA.ViewData.Status',
 'SideA.ViewData.BreakID_A_side',
    
 #'ISIN_match',
 #'CUSIP_match',
 #'Currency_match',
 'Trade_Date_match',
 'Settle_Date_match',
 'Fund_match',
# 'Amount_diff_1',
'Amount_diff_2',
 'Trade_date_diff',
 'Settle_date_diff',
'SideA.ISIN_NA',
       'SideB.ISIN_NA',
    'ViewData.Combined Fund',
'ViewData.Combined Transaction Type',
    'All_key_nan',
    'new_key_match',
    'new_pb1',
 'label']


# In[280]:


train_full_new11 = train_full_new1[~((train_full_new1['Amount_diff_2']==0) & ((train_full_new1['label']=='No-Pair') & (train_full_new1['new_key_match']>=0)))]
train_full_new11 = train_full_new11.reset_index()
train_full_new11 = train_full_new11.drop('index', 1)


# In[282]:


train_full_new2 = train_full_new11[train_full_new11['SideB.ViewData.Status'].isin(['OB','SPM','SDB','UDB','UOB'])]
train_full_new2 = train_full_new2.reset_index()
train_full_new2 = train_full_new2.drop('index', 1)

train_full_new2 = train_full_new2[train_full_new2['SideA.ViewData.Status'].isin(['OB','SPM','SDB','UDB','UOB'])]
train_full_new2 = train_full_new2.reset_index()
train_full_new2 = train_full_new2.drop('index', 1)


# In[283]:


train_full_new2 = train_full_new11.copy()


# In[284]:


train_full_new2['open_key'] = train_full_new2['SideB.ViewData.Side0_UniqueIds'].astype(str) + train_full_new2['SideA.ViewData.Side1_UniqueIds'].astype(str)


# In[285]:


umb_keys =[]
non_umb_keys =[]
for i in train_full_new2[train_full_new2['label']=='UMB_One_to_One']['SideA.ViewData.Side1_UniqueIds'].unique():
    status = df[df['ViewData.Side1_UniqueIds']==i]['ViewData.Status'].values
    if status[len(status)-1] =='UMB':
        umb_keys.append(i)
    else:
        non_umb_keys.append(i)


# In[286]:


train_full_new3 = train_full_new2[~(train_full_new2['SideA.ViewData.Side1_UniqueIds'].isin(non_umb_keys))]
train_full_new3 = train_full_new3.reset_index()
train_full_new3 = train_full_new3.drop('index',1)


# In[287]:


umt_keys =[]
non_umt_keys =[]
for i in train_full_new2[train_full_new2['label']=='UMT_One_to_One']['SideA.ViewData.Side1_UniqueIds'].unique():
    status = df[df['ViewData.Side1_UniqueIds']==i]['ViewData.Status'].values
    if status[len(status)-1] =='UMT':
        umt_keys.append(i)
    else:
        non_umt_keys.append(i)


# In[288]:


train_full_new4 = train_full_new3[~(train_full_new3['SideA.ViewData.Side1_UniqueIds'].isin(non_umt_keys))]
train_full_new4 = train_full_new4.reset_index()
train_full_new4 = train_full_new4.drop('index',1)


# In[323]:


#df[df['ViewData.Side1_UniqueIds']==umt_keys[19]]['ViewData.Status']


# In[324]:


#oo1 = train_full_new4[train_full_new4['label']=='Partial_match']['SideA.ViewData.Side1_UniqueIds'].unique()
#oo2 = train_full_new4[train_full_new4['label']=='Partial_match']['SideB.ViewData.Side0_UniqueIds'].unique()


# In[321]:


#train_full_new5 = train_full_new4[~((train_full_new4['SideB.ViewData.Side0_UniqueIds'].isin(oo2)) & (train_full_new4['label'] != 'Partial_match'))]
#train_full_new5 = train_full_new5.reset_index()
#train_full_new5 = train_full_new5.drop('index',1)


# In[322]:


#train_full_new6 = train_full_new5[~((train_full_new5['SideA.ViewData.Side1_UniqueIds'].isin(oo1)) & (train_full_new5['label'] != 'Partial_match'))]
#train_full_new6 = train_full_new6.reset_index()
#train_full_new6 = train_full_new6.drop('index',1)


# In[289]:


train_full_new5 = train_full_new4.copy()


# In[290]:


train_full_new5['amount_percent'] = (train_full_new5['SideA.ViewData.B-P Net Amount']/train_full_new5['SideB.ViewData.Accounting Net Amount']*100)


# In[291]:


train_full_new7 = train_full_new5.copy()


# In[292]:


train_full_new7 = train_full_new7[train_full_new7['label']!='UMB_One_to_One']
train_full_new7 = train_full_new7.reset_index().drop('index',1)


# In[293]:


#train_full_new7.loc[train_full_new7['label']=='Partial_match','label']


# In[294]:


train_full_new7.loc[train_full_new7['label']=='Partial_match','label'] = 'UMB_One_to_One'

train_full_new7.loc[train_full_new7['label']=='Partial_match_MTO','label'] = 'UMB_One_to_One'
#train_full_new7.loc[train_full_new7['label']=='Partial_match','label'] = 'UMB_One_to_One'
#train_full_new7.loc[train_full_new7['label']=='UMT_One_to_One','label'] = 'UMB_One_to_One'


# In[295]:


train_full_new7['Trade_date_diff'].quantile(0.02)


# In[296]:


train_full_new7['SideB.ViewData.Investment Type']= train_full_new7['SideB.ViewData.Investment Type'].apply(lambda x: str(x).lower())
train_full_new7['SideA.ViewData.Investment Type'] = train_full_new7['SideA.ViewData.Investment Type'].apply(lambda x: str(x).lower())


# In[297]:


train_full_new7['SideB.ViewData.Prime Broker'] = train_full_new7['SideB.ViewData.Prime Broker'].apply(lambda x: str(x).lower())
train_full_new7['SideA.ViewData.Prime Broker'] = train_full_new7['SideA.ViewData.Prime Broker'].apply(lambda x: str(x).lower())


# In[298]:


train_full_new7['SideB.ViewData.Asset Type Category'] = train_full_new7['SideB.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())
train_full_new7['SideA.ViewData.Asset Type Category'] = train_full_new7['SideA.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())


# In[299]:


#train_full_new7['SideB.ViewData.Transaction Category']

train_full_new7['SideB.ViewData.Transaction Category']= train_full_new7['SideB.ViewData.Transaction Category'].apply(lambda x: str(x).lower())
train_full_new7['SideA.ViewData.Transaction Category'] = train_full_new7['SideA.ViewData.Transaction Category'].apply(lambda x: str(x).lower())


# In[301]:


model_cols = [
 #   'SideA.ViewData.Accounting Net Amount',
 'SideA.ViewData.B-P Net Amount',
# 'SideA.ViewData.Price',
# 'SideA.ViewData.Quantity',
  #  'SideA.ViewData.Cancel Flag',
    'SideA.ViewData.Description',
     'SideA.ViewData.Department',
   'SideA.ViewData.Investment Type',
  #  'SideA.ViewData.Prime Broker',
  #  'SideA.ViewData.Transaction Category',
     'SideA.ViewData.Asset Type Category',
 'SideB.ViewData.Accounting Net Amount',
# 'SideB.ViewData.B-P Net Amount',
# 'SideB.ViewData.Price',
# 'SideB.ViewData.Quantity',
  #  'SideB.ViewData.Cancel Flag',
    'SideB.ViewData.Description',
     'SideB.ViewData.Department',
   'SideB.ViewData.Investment Type',
#    'SideB.ViewData.Prime Broker',
    # 'SideB.ViewData.Transaction Category',
     'SideB.ViewData.Asset Type Category',
 'Trade_Date_match',
 'Settle_Date_match',
 #'Fund_match',
 'Amount_diff_2',
 'Trade_date_diff',
 'Settle_date_diff',
 
'SideA.ISIN_NA',
 'SideB.ISIN_NA',
    
 'ViewData.Combined Fund',
'ViewData.Combined Transaction Type',
 'All_key_nan',
 'new_key_match',
    'pb_new1',
#'amount_percent',
    
'SideB.ViewData.Status',
 'SideB.ViewData.BreakID_B_side',
 'SideA.ViewData.Status',
 'SideA.ViewData.BreakID_A_side',
 'label']


# In[302]:


train_full_new7 = train_full_new7[~((train_full_new7['label']=='UMR_One_to_One') & (train_full_new7['Amount_diff_2']!=0))]
train_full_new7 = train_full_new7.reset_index().drop('index',1)


# In[303]:


#train_full_new7[((train_full_new7['label']=='UMB_One_to_One') & (train_full_new7['Amount_diff_2']==0))]


# In[304]:


train_full_new7.loc[(train_full_new7['label']=='UMB_One_to_One') & (train_full_new7['Amount_diff_2']==0),'label'] = 'UMR_One_to_One'


# In[305]:


close_remove_ids0 = sample[(sample['ViewData.Side0_UniqueIds'].isin(train_full_new7['SideB.ViewData.Side0_UniqueIds'].unique())) & (sample['ViewData.Status']=='UCB')]['ViewData.Side0_UniqueIds'].unique()


# In[306]:


close_remove_ids1 = sample[(sample['ViewData.Side1_UniqueIds'].isin(train_full_new7['SideA.ViewData.Side1_UniqueIds'].unique())) & (sample['ViewData.Status']=='UCB')]['ViewData.Side1_UniqueIds'].unique()


# In[307]:


#close_remove_ids1

train_full_new8 = train_full_new7[~((train_full_new7['SideB.ViewData.Side0_UniqueIds'].isin(close_remove_ids0)))]
train_full_new8 = train_full_new8.reset_index()
train_full_new8 = train_full_new8.drop('index',1)

train_full_new8 = train_full_new8[~((train_full_new8['SideA.ViewData.Side1_UniqueIds'].isin(close_remove_ids1)))]
train_full_new8 = train_full_new8.reset_index()
train_full_new8 = train_full_new8.drop('index',1)


# In[308]:


train_full_new8['label'].value_counts()


# In[309]:


train_full_new8 = train_full_new8[~train_full_new8['SideA.ViewData.B-P Net Amount'].isnull()]
train_full_new8 = train_full_new8.reset_index()
train_full_new8 = train_full_new8.drop('index',1)


# In[310]:


train_full_new8 = train_full_new8[(train_full_new8['SideA.ViewData.Investment Type']!='nan')]
train_full_new8 = train_full_new8[(train_full_new8['SideB.ViewData.Investment Type']!='nan')]

train_full_new8 = train_full_new8.reset_index()
train_full_new8 = train_full_new8.drop('index',1)


# In[311]:


train_full_new8 = train_full_new8[(train_full_new8['SideA.ViewData.Asset Category Type']!='nan')]
train_full_new8 = train_full_new8[(train_full_new8['SideB.ViewData.Asset Category Type']!='nan')]

train_full_new8 = train_full_new8.reset_index()
train_full_new8 = train_full_new8.drop('index',1)


# In[312]:


train_full_new8['Combined_Asset_Type_Category'] = train_full_new8['SideA.ViewData.Asset Category Type'].astype(str) + train_full_new8['SideB.ViewData.Asset Category Type'].astype(str)


# In[313]:


train_full_new9 = train_full_new8[train_full_new8['label'].isin(['UMR_One_to_One','No-Pair','UMT_One_to_One','UMB_One_to_One'])]

train_full_new9 = train_full_new9.reset_index()
train_full_new9 = train_full_new9.drop('index',1)


# In[314]:


train_full_new9['label'].value_counts()


# In[315]:


#train_full_new9[train_full_new9['label']!='No-Pair']['SideA.ViewData.Investment Type'].value_counts()


train_full_new9['SideA.ViewData.Investment Type'] = train_full_new9['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
train_full_new9['SideA.ViewData.Investment Type'] = train_full_new9['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
train_full_new9['SideA.ViewData.Investment Type'] = train_full_new9['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
train_full_new9['SideA.ViewData.Investment Type'] = train_full_new9['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))


# In[316]:


train_full_new9['SideB.ViewData.Investment Type'] = train_full_new9['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
train_full_new9['SideB.ViewData.Investment Type'] = train_full_new9['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
train_full_new9['SideB.ViewData.Investment Type'] = train_full_new9['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
train_full_new9['SideB.ViewData.Investment Type'] = train_full_new9['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))


# In[317]:


train_full_new9['Combined_Investment_Type'] = train_full_new9['SideA.ViewData.Investment Type'].astype(str) + train_full_new9['SideB.ViewData.Investment Type'].astype(str)


# In[318]:


train_full_new9['SideA.ViewData.Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].apply(lambda x: x.replace('cover short','covershort'))


# In[319]:


train_full_new9['SideA.ViewData.Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].apply(lambda x: x.replace('jnl','journal'))


# In[320]:


def inttype(x):
    if type(x)== float:
        return 'interest'
    else:
        x1 = x.lower()
        x2 = x1.split()
        if 'int' in x2:
            return 'interest'
        else:
            return x1 
        
def divclient(x):
    if (type(x) == str):
        if ('eqswap dividend client tax' in x) :
            return 'eqswap dividend client tax'
        else:
            return x
    else:
        return 'float'
    
def mhreplace(item):
    item1 = item.split()
    for items in item1:
        if items.endswith('mh')==True:
            item1.remove(items)
    return ' '.join(item1).lower()

def dollarreplace(item):
    item1 = item.split()
    for items in item1:
        if items.startswith('$')==True:
            item1.remove(items)
    return ' '.join(item1).lower()

def thirtyper(item):
    item1 = item.split()
    if '30%' in item1:
        return '30 percent'
    else:
        return item

train_full_new9['SideB.ViewData.Transaction Type'] = train_full_new9['SideB.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))
train_full_new9['SideA.ViewData.Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


train_full_new9['SideA.ViewData.Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].apply(lambda x :x.lower())
train_full_new9['SideA.ViewData.Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].apply(lambda x : inttype(x))
train_full_new9['SideA.ViewData.Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].apply(lambda x : divclient(x))
train_full_new9['SideA.ViewData.Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].apply(lambda x :mhreplace(x))
train_full_new9['SideA.ViewData.Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].apply(lambda x :dollarreplace(x))
train_full_new9['SideA.ViewData.Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].apply(lambda x :thirtyper(x))


train_full_new9['SideB.ViewData.Transaction Type'] = train_full_new9['SideB.ViewData.Transaction Type'].apply(lambda x :x.lower())
train_full_new9['SideB.ViewData.Transaction Type'] = train_full_new9['SideB.ViewData.Transaction Type'].apply(lambda x : inttype(x))
train_full_new9['SideB.ViewData.Transaction Type'] = train_full_new9['SideB.ViewData.Transaction Type'].apply(lambda x : divclient(x))
train_full_new9['SideB.ViewData.Transaction Type'] = train_full_new9['SideB.ViewData.Transaction Type'].apply(lambda x :mhreplace(x))
train_full_new9['SideB.ViewData.Transaction Type'] = train_full_new9['SideB.ViewData.Transaction Type'].apply(lambda x :dollarreplace(x))
train_full_new9['SideB.ViewData.Transaction Type'] = train_full_new9['SideB.ViewData.Transaction Type'].apply(lambda x :thirtyper(x))


train_full_new9['SideA.ViewData.Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].apply(lambda x: x.replace('withdrawal','withdraw'))
train_full_new9['SideA.ViewData.Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].apply(lambda x: x.replace('cover short','covershort'))


train_full_new9['ViewData.Combined Transaction Type'] = train_full_new9['SideA.ViewData.Transaction Type'].astype(str) + train_full_new9['SideB.ViewData.Transaction Type'].astype(str)
train_full_new9['ViewData.Combined Transaction Type'] = train_full_new9['ViewData.Combined Transaction Type'].apply(lambda x: x.replace('jnl','journal'))


# In[321]:


train_full_new9['SideB.ViewData.Age'] = train_full_new9['SideB.ViewData.Age'].astype(int)


# In[322]:


#from imblearn.over_sampling import SMOTE

#from imblearn.over_sampling import SMOTENC

train_full_new9 = train_full_new9[~train_full_new9['SideB.ViewData.Age']<0]
train_full_new9 = train_full_new9.reset_index()
train_full_new9 = train_full_new9.drop('index',1)


# In[323]:


trade_types_A = ['buy', 'sell', 'covershort','sellshort',
       'fx', 'fx settlement', 'sell short',
       'trade not to be reported_buy', 'covershort','ptbl','ptss', 'ptcs', 'ptcl']

trade_types_B = ['trade not to be reported_buy','buy', 'sellshort', 'sell', 'covershort',
       'spotfx', 'forwardfx',
       'trade not to be reported_sell',
       'trade not to be reported_sellshort',
       'trade not to be reported_covershort']


# In[324]:


train_full_new9['SideA.TType'] = train_full_new9.apply(lambda x: "Trade" if x['SideA.ViewData.Transaction Type'] in trade_types_A else "Non-Trade", axis=1)


# In[325]:


train_full_new9['SideB.TType'] = train_full_new9.apply(lambda x: "Trade" if x['SideB.ViewData.Transaction Type'] in trade_types_B else "Non-Trade", axis=1)


# In[326]:


#train_full_new12 = train_full_new9[~((train_full_new9['label']=='No-Pair') & (train_full_new9['SideA.TType']==train_full_new9['SideB.TType']) & (train_full_new9['new_key_match']==1) & (train_full_new9['Settle_Date_match']==1))]


# In[327]:


train_full_new12 = train_full_new9.copy()


# In[328]:


train_full_new12 = train_full_new12.reset_index().drop('index',1)


# In[329]:


train_full_new12['abs_amount_flag'] = train_full_new12.apply(lambda x: 1 if x['SideB.ViewData.Accounting Net Amount'] == x['SideA.ViewData.B-P Net Amount']*(-1) else 0, axis=1)


# In[330]:


train_full_new12.shape


# In[331]:


#train_full_new13 = train_full_new13.reset_index().drop('index',1)
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


# In[332]:


import re
def  clean_text(df, text_field, new_text_field_name):
    df[text_field] = df[text_field].astype(str)
    df[new_text_field_name] = df[text_field].str.lower()
     
    
    df[new_text_field_name] = df[new_text_field_name].apply(lambda x: re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", "", x))  
    # remove numbers
    df[new_text_field_name] = df[new_text_field_name].apply(lambda x: re.sub(r"\d+", "", x))
    df[new_text_field_name] = df[new_text_field_name].str.replace('usd','')
    df[new_text_field_name] = df[new_text_field_name].str.replace('eur0','')
    df[new_text_field_name] = df[new_text_field_name].str.replace(' usd','')
    df[new_text_field_name] = df[new_text_field_name].str.replace(' euro','')

    df[new_text_field_name] = df[new_text_field_name].str.replace(' eur','')
    df[new_text_field_name] = df[new_text_field_name].str.replace('eur','')
    
    return df


# In[333]:


train_full_new12 =  clean_text(train_full_new12,'SideA.ViewData.Description', 'SideA.ViewData.Description_new') 
train_full_new12 =  clean_text(train_full_new12,'SideB.ViewData.Description', 'SideB.ViewData.Description_new') 


# In[334]:


train_full_new12['description_similarity_score'] = train_full_new12.apply(lambda x: fuzz.token_sort_ratio(x['SideA.ViewData.Description_new'], x['SideB.ViewData.Description_new']), axis=1)


# ## Mapped UMR

# In[336]:


mapped_tt_umr = train_full_new12[train_full_new12['label'].isin(['UMR_One_to_One'])]['ViewData.Combined Transaction Type'].unique()


# In[337]:


train_full_new12['tt_map_flag'] = train_full_new12.apply(lambda x: 1 if x['ViewData.Combined Transaction Type'] in mapped_tt_umr else 0, axis=1)


# In[338]:


train_full_new13 = train_full_new12.copy()


# In[339]:


#train_full_new12['SideA.Date']
import pandas as pd
import xgboost as xgb
from sklearn.preprocessing import LabelEncoder
import numpy as np

#le = LabelEncoder()
for feature in ['SideA.Date','SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date','SideA.ViewData.Trade Date','SideB.ViewData.Trade Date']:
    #train_full_new12[feature] = le.fit_transform(train_full_new12[feature])
    train_full_new13[feature] = pd.to_datetime(train_full_new13[feature],errors = 'coerce').dt.weekday


# In[340]:


train_full_new13['Combined_Desc'] = train_full_new13['SideA.new_desc_cat'] + train_full_new13['SideB.new_desc_cat']


# In[341]:


from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix


# In[342]:


#train_full_new9[train_full_new9['label']=='UMR_One_to_One'].to_csv('Soros_UMR_new.csv')

#train_full_new9[train_full_new9['label'].isin(['UMR_One_to_One','No-Pair'])].to_csv('Soros_UMR_No-Pair_new.csv')

model_cols = [#'SideA.ViewData.B-P Net Amount', 
              #'SideA.ViewData.Cancel Flag', 
             # 'SideA.new_desc_cat',
              #'SideA.ViewData.Description',
            # 'SideA.ViewData.Investment Type', 
              #'SideA.ViewData.Asset Type Category', 
              
              #'SideB.ViewData.Accounting Net Amount', 
              #'SideB.ViewData.Cancel Flag', 
              #'SideB.ViewData.Description',
             # 'SideB.new_desc_cat',
             # 'SideB.ViewData.Investment Type', 
              #'SideB.ViewData.Asset Type Category', 
              'Trade_Date_match', 'Settle_Date_match', 
           #'Amount_diff_2', 
              'Trade_date_diff', 
            'Settle_date_diff', 'SideA.ISIN_NA', 'SideB.ISIN_NA', 
             'ViewData.Combined Fund',
              'ViewData.Combined Transaction Type', 'Combined_Investment_Type','Combined_Asset_Type_Category',
             # 'Combined_Desc',
             # 'ViewData.Combined Investment Type',
             # 'SideA.TType', 'SideB.TType',
              'abs_amount_flag', 'tt_map_flag', 'description_similarity_score',
              'SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date',
                'SideA.ViewData.Trade Date','SideB.ViewData.Trade Date',
              'All_key_nan','new_key_match', 'new_pb1','desc_any_match','SEDOL_match','TD_bucket','SD_bucket',
              #'Combined_TType',
              #'SideB.Date',
              'SideB.ViewData.Status', 'SideB.ViewData.BreakID_B_side',
              'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side', 
              'label']


# In[343]:


train_full_new14 = train_full_new13[~train_full_new13['SideB.ViewData.Settle Date'].isnull()]
train_full_new14 = train_full_new14[~train_full_new14['SideA.ViewData.Settle Date'].isnull()]

train_full_new14 = train_full_new14[~train_full_new14['SideB.ViewData.Trade Date'].isnull()]
train_full_new14 = train_full_new14[~train_full_new14['SideA.ViewData.Trade Date'].isnull()]

train_full_new14 = train_full_new14.reset_index().drop('index',1)
#train_full_new14['SideA.ViewData.Settle Date'] = train_full_new14['SideA.ViewData.Settle Date'].astype(int)
#train_full_new14['SideB.ViewData.Settle Date'] = train_full_new14['SideB.ViewData.Settle Date'].astype(int)


# In[344]:


train_full_new14['SideA.ViewData.Settle Date'] = train_full_new14['SideA.ViewData.Settle Date'].astype(int)
train_full_new14['SideB.ViewData.Settle Date'] = train_full_new14['SideB.ViewData.Settle Date'].astype(int)

train_full_new14['SideA.ViewData.Trade Date'] = train_full_new14['SideA.ViewData.Trade Date'].astype(int)
train_full_new14['SideB.ViewData.Trade Date'] = train_full_new14['SideB.ViewData.Trade Date'].astype(int)


# In[345]:


dummy = full_otm_data[full_otm_data['SideB.ViewData.Side0_UniqueIds'].isin(comb_and_match[comb_and_match['ViewData.Status'].isin(['UMR','UMB','UMT'])]['ViewData.Side0_UniqueIds'].unique())]

dummy1 = dummy[(dummy['SideB.ViewData.Status']!='SMB') & (dummy['SideA.ViewData.Status']!='SMB')]
dummy2 = full_mto_data[full_mto_data['SideA.ViewData.Side1_UniqueIds'].isin(comb_and_match_mto[comb_and_match_mto['ViewData.Status'].isin(['UMR','UMB','UMT'])]['ViewData.Side1_UniqueIds'].unique())]

dummy3 = dummy2[(dummy2['SideB.ViewData.Status']!='SMB') & (dummy2['SideA.ViewData.Status']!='SMB')]


# In[346]:


train_full_new15 = train_full_new14[(train_full_new14['SideB.ViewData.Side0_UniqueIds'].isin(dummy1['SideB.ViewData.Side0_UniqueIds'].unique())) | (train_full_new14['SideA.ViewData.Side1_UniqueIds'].isin(dummy3['SideA.ViewData.Side1_UniqueIds'].unique())) | (train_full_new14['label'].isin(['No-Pair','UMR_One_to_One']))]


# In[347]:


train_full_new15 = train_full_new15.reset_index().drop('index',1)


# In[348]:


train_full_new16 = train_full_new15[~train_full_new15['label'].isin(['UMR_One_to_One'])]
train_full_new16 = train_full_new16.reset_index().drop('index',1)


# In[359]:


train_full_new16 = train_full_new14.copy()


# In[360]:


train_full_new16['label'].value_counts()


# In[351]:


train_full_new16 = train_full_new16[~train_full_new16['label'].isin(['UMR_One_to_One'])]
train_full_new16 = train_full_new16.reset_index().drop('index',1)


# In[361]:


train_full_new16['label'].value_counts()


# In[362]:


train_full_new16 = train_full_new16[(train_full_new16['SideA.TType']!='Trade') & (train_full_new16['SideB.TType']!='Trade')]
train_full_new16 = train_full_new16.reset_index().drop('index',1)


# In[363]:


train_full_new16['Combined_TType'] = train_full_new16['SideA.TType'] + train_full_new16['SideB.TType']


# In[364]:


train_full_new16['label'].value_counts()


# In[365]:


train_full_new18 = train_full_new16.copy()


# In[366]:


train_full_new18['label'].value_counts()


# In[367]:


train_full_new18['new_pb1'] = train_full_new18['new_pb1'].apply(lambda x: x.replace('Citi','CITI'))
#train_full_new18['new_pb1'] = train_full_new18['new_pb1'].apply(lambda x: x.replace('options','option'))


# In[368]:


def desc_any_string_check(text1, text2):
    match = 0
    match2 = 0
    text1 = text1.replace('interest','loan')
    text1 = text1.replace('principal','loan')
    text1 = text1.split(" ")
    text2 = text2.split(" ")
    for i in text1:
        for j in text2:
            if i in j and len(i)>1 and len(j)>1:
                match = 1
                break
    for i in text2:
        for j in text1:
            if i in j and len(i)>1 and len(j)>1:
                match2 = 1
                break
    if match==0 and match2==0:
        return 0
    else: 
        return 1


# In[369]:


train_full_new18['desc_any_match'] = train_full_new18.apply(lambda x: desc_any_string_check(x['SideA.ViewData.Description_new'],x['SideB.ViewData.Description_new']), axis=1)


# In[370]:


train_full_new18['SideA.ViewData.SEDOL'] = train_full_new18['SideA.ViewData.SEDOL'].astype(str) 
train_full_new18['SideB.ViewData.SEDOL'] = train_full_new18['SideB.ViewData.SEDOL'].astype(str) 

def sedol_match(text1,text2):
    if text1 !='nan' and text2!='nan' and (text1 in text2 or text2 in text1):
        return 1
    elif text1 !='nan' and text2!='nan' and (text1 not in text2 or text2 not in text1):
        return 2
    else:
        return 0
    
train_full_new18['SEDOL_match'] = train_full_new18.apply(lambda x: sedol_match(x['SideA.ViewData.SEDOL'],x['SideB.ViewData.SEDOL']),axis=1)


# In[371]:


train_full_new19 = train_full_new18[~((train_full_new18['SEDOL_match']==2)& (train_full_new18['label']=='UMB_One_to_One') & (train_full_new18['new_key_match']==0))]
train_full_new19 = train_full_new19.reset_index().drop('index',1)


# In[372]:


ticker_vars =['SideA.ViewData.Underlying ISIN', 'SideA.ViewData.Underlying Cusip','SideA.ViewData.Underlying Sedol',
             'SideA.ViewData.SEDOL','SideA.ViewData.CUSIP', 'SideA.ViewData.ISIN','SideA.ViewData.Ticker',
              'SideB.ViewData.Underlying ISIN', 'SideB.ViewData.Underlying Cusip','SideB.ViewData.Underlying Sedol',
             'SideB.ViewData.SEDOL','SideB.ViewData.CUSIP', 'SideB.ViewData.ISIN','SideB.ViewData.Ticker']


# In[373]:


train_full_new19['Trade_date_diff'] = np.abs(train_full_new19['Trade_date_diff'])
train_full_new19['Settle_date_diff'] = np.abs(train_full_new19['Settle_date_diff'])


# In[374]:


train_full_new19['TD_bucket'] = train_full_new19['Trade_date_diff'].apply(lambda x: 0 if x==0 else(1 if x<=3 else 2))
train_full_new19['SD_bucket'] = train_full_new19['Settle_date_diff'].apply(lambda x: 0 if x==0 else(1 if x<=3 else 2))


# In[380]:


train_full_new20 = train_full_new19.copy()

#train_full_new20 = train_full_new20[train_full_new20['label'].isin(['UMR_One_to_One','UMB_One_to_One'])]
#train_full_new20 = train_full_new20.reset_index().drop('index',1)


# In[ ]:


train_full_new20.loc[train_full_new20['label']=='UMR_One_to_One', 'label'] = 'UMB_One_to_One'
train_full_new20.loc[train_full_new20['label']=='UMT_One_to_One', 'label'] = 'UMB_One_to_One'
#train_full_new20 = train_full_new20.reset_index().drop('index',1)


# In[381]:


train_full_new20['SideA.ViewData.Status'].value_counts()


# In[382]:


train_full_new21 = train_full_new20[(train_full_new20['SideA.ViewData.Status'].isin(['OB','SPM'])) & (train_full_new20['SideB.ViewData.Status'].isin(['OB','SPM']))]

train_full_new21 = train_full_new21.reset_index().drop('index',1)
                 


# In[383]:


train_full_new21['label'].value_counts()


# In[390]:


model_cols = [
   'SideA.ViewData.B-P Net Amount', 
              #'SideA.ViewData.Cancel Flag', 
             # 'SideA.new_desc_cat',
              #'SideA.ViewData.Description',
            # 'SideA.ViewData.Investment Type', 
              #'SideA.ViewData.Asset Type Category', 
              
              'SideB.ViewData.Accounting Net Amount', 
              #'SideB.ViewData.Cancel Flag', 
              #'SideB.ViewData.Description',
             # 'SideB.new_desc_cat',
             # 'SideB.ViewData.Investment Type', 
              #'SideB.ViewData.Asset Type Category', 
              'Trade_Date_match', 'Settle_Date_match', 
           'Amount_diff_2', 
              'Trade_date_diff', 
            'Settle_date_diff', 'SideA.ISIN_NA', 'SideB.ISIN_NA', 
             'ViewData.Combined Fund',
              'ViewData.Combined Transaction Type', 'Combined_Investment_Type',
     'Combined_Asset_Type_Category',
             # 'Combined_Desc',
             # 'ViewData.Combined Investment Type',
             # 'SideA.TType', 'SideB.TType',
              'abs_amount_flag',
    'tt_map_flag', 
    'description_similarity_score',
              'SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date',
                'SideA.ViewData.Trade Date','SideB.ViewData.Trade Date',
              'All_key_nan','new_key_match', 'new_pb1','desc_any_match','SEDOL_match','TD_bucket','SD_bucket',
              #'Combined_TType',
              #'SideB.Date',
              'SideB.ViewData.Status', 'SideB.ViewData.BreakID_B_side',
              'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side', 
              'label']


# In[ ]:





# In[391]:


X_train, X_test, y_train, y_test = train_test_split(train_full_new21[model_cols].drop((['label']),axis=1), 
          train_full_new21[model_cols]['label'], test_size=0.2, 
            random_state=88)


# In[392]:


X_train = X_train.fillna(0)
X_test = X_test.fillna(0)


# In[393]:


X_train.shape


# In[394]:


from catboost import CatBoostClassifier


# In[395]:


# Create the model with 100 trees
#model = RandomForestClassifier(n_estimators=1000, 
#                               bootstrap = True,
#                               max_features = 'sqrt', max_depth=None)
                               #min_samples_leaf= 5, min_samples_split = 12)

cat_features = ['ViewData.Combined Transaction Type',
                'ViewData.Combined Fund',
                'All_key_nan', 
                #'Fund_match',
                'new_key_match',
                'Trade_Date_match','Settle_Date_match', 
                'SideA.ISIN_NA','SideB.ISIN_NA',
                #'SideA.ViewData.Description','SideB.ViewData.Description',
                #'SideA.new_desc_cat','SideB.new_desc_cat',
              #'SideA.ViewData.Investment Type','SideB.ViewData.Investment Type',
               'SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date',
                'SideA.ViewData.Trade Date','SideB.ViewData.Trade Date',
                
                'abs_amount_flag',
                'tt_map_flag',
                #'Combined_Desc',
                'new_pb1','desc_any_match','SEDOL_match','TD_bucket','SD_bucket',
               # 'SideA.ViewData.Prime Broker','SideB.ViewData.Prime Broker',
               # 'SideA.ViewData.Transaction Category','SideB.ViewData.Transaction Category',
                # 'SideA.TType','SideB.TType',
            #    # 'SideA.ViewData.Asset Type Category','SideB.ViewData.Asset Type Category']
             'Combined_Asset_Type_Category',
            'Combined_Investment_Type']
                #'Combined_TType']


SEED =88
params ={'loss_function' : 'MultiClass',
        'eval_metric' : 'AUC',
        'learning_rate':0.15,
        'iterations':800,
        'depth':4,
        'random_seed':SEED,
        'od_type':'Iter',
        'od_wait':200,
        'cat_features':cat_features,
        'task_type':'CPU'}
       # 'class_weights' : [1,1]}

clf74 = CatBoostClassifier(**params)
   

clf74.fit(X_train.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1), 
            y_train,eval_set=(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1), y_test),
       use_best_model=True,plot=True)

# Fit on training data
#model.fit(X_train.drop(['ViewData.BreakID_A_side','ViewData.BreakID_B_side'],1), y_train)


# In[448]:


# Actual class predictions
rf_predictions = clf74.predict(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1))
# Probabilities for each class
rf_probs = clf74.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1))[:, 1]


# In[449]:


#X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'], 1)


# In[691]:


rf_predictions = clf74.predict(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1))
# Probabilities for each class
rf_probs = clf74.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side'],1))[:, 1]

from sklearn.metrics import confusion_matrix
confusion_matrix(y_test, rf_predictions)


# In[506]:


import pickle
filename = 'Weiss_final_model2_with_umt.sav'
pickle.dump(clf74, open(filename, 'wb'))

