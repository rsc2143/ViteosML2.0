# -*- coding: utf-8 -*-
"""
Created on Fri May 14 14:16:07 2021

@author: riteshkumar.patra
"""

import sys
import os
import subprocess
from subprocess import check_output, STDOUT, CalledProcessError
import datetime
from datetime import datetime
os.chdir('D:\\ViteosModel2.0\\')
from src.ViteosLogger_Production import ViteosLogger_Class

now = datetime.now()
current_date_and_time = now.strftime('%y-%m-%d_%H-%M-%S')

Logger_obj = ViteosLogger_Class()
log_folder = os.getcwd() + '\\logs\\'
log_filename = 'log_datetime_' + str(current_date_and_time) + '.txt'
#log_filename = 'log_datetime_21-04-14_13-07-19.txt'
log_filepath = log_folder + log_filename

Logger_obj.log_to_file(param_filename = log_filepath, param_log_str = 'Log started for datettime = ' + str(current_date_and_time))

try:
  s2_out = subprocess.check_output([sys.executable, os.getcwd() + '\\ML2_RMQ_Receive_Production.py'])
except Exception:
    data = None
