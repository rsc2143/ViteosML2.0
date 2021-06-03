# -*- coding: utf-8 -*-
"""
Created on Wed May 26 12:10:23 2021

@author: riteshkumar.patra
"""
import time, sys
from datetime import datetime
i = 0
while i < 2:
    f = open("D:\new_log.txt", "a")
    now = datetime.now() # current date and time
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    #f.write("date and time:",date_time)
    #f.write("logging date  " +date_time + str(sys.argv[1]) +"\n")
#    f.write(str(type(sys.argv[0]) + "\n"))
#    f.write(str(type(sys.argv[1]) + "\n"))
    now = datetime.now()
    current_date_and_time = now.strftime('%y-%m-%d_%H-%M-%S')

    f.write(str(current_date_and_time) + str(sys.argv[0]) + "\n")
    f.write(str(current_date_and_time) + str(sys.argv[1]) + "\n")

    f.write('ssd' + '\n')
    f.close()
    time.sleep(2)
    i = i + 1

