# -*- coding: utf-8 -*-
"""
Created on Mon Apr 26 17:04:46 2021

@author: consultant138
"""

import timeit
start = timeit.default_timer()
import memory_profiler


import numpy as np
import pandas as pd
#from imblearn.over_sampling import SMOTE


import os
os.chdir('D:\\ViteosModel2.0\\')
#from imblearn.over_sampling import SMOTE
from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report
from tqdm import tqdm
import pickle
import datetime as dt
import sys
from src.ViteosMongoDB_Production import  ViteosMongoDB_Class as mngdb
from datetime import datetime,date,timedelta
from pandas.io.json import json_normalize
import dateutil.parser
from difflib import SequenceMatcher
import pprint
import json
from pandas import merge,DataFrame

import re

import dask.dataframe as dd
import glob
import math
from sklearn.feature_extraction.text import TfidfVectorizer
from dateutil.parser import parse
import operator
import itertools
from sklearn.feature_extraction.text import CountVectorizer

import xgboost as xgb
from sklearn.preprocessing import LabelEncoder

from fuzzywuzzy import fuzz
import random
import decimal

import subprocess
from subprocess import check_output, STDOUT, CalledProcessError

from src.RabbitMQ_Production import RabbitMQ_Class as rb_mq
import pika
from src.ViteosLogger_Production import ViteosLogger_Class

now = datetime.now()
current_date_and_time = now.strftime('%y-%m-%d_%H-%M-%S')

Logger_obj = ViteosLogger_Class()
log_folder = os.getcwd() + '\\logs\\'
log_filename = 'log_datetime_' + str(current_date_and_time) + '.txt'
#log_filename = 'log_datetime_21-04-14_13-07-19.txt'
log_filepath = log_folder + log_filename

Logger_obj.log_to_file(param_filename = log_filepath, param_log_str = 'Log started for datettime = ' + str(current_date_and_time))

with open(os.getcwd() + '\\data\\Weiss_125_Production_loop_1_parameters.json') as f:
    parameters_dict = json.load(f)
