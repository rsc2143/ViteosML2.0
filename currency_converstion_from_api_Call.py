# -*- coding: utf-8 -*-
"""
Created on Wed Aug 11 11:50:08 2021

@author: consultant138
"""


import requests
#import pandas as pd
# Where USD is the base currency you want to use
url = 'https://v6.exchangerate-api.com/v6/a4a44000cd52ee36932bf88a/latest/USD'
proxies = { "http": "http://192.168.170.200:3128",
           "https": "https://192.168.170.200:3128"
}
# Making our request
response = requests.get(url,proxies=proxies)
data = response.json()

# Your JSON object
print(data)
conv = pd.DataFrame(data=data.get('conversion_rates').items(),columns=['Currency','reciprocal_conversion'])
conv['conversion'] =  conv['reciprocal_conversion'].apply(lambda x : 1/x)
conv.drop(columns = 'reciprocal_conversion')
