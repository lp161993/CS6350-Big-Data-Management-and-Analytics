#! /usr/bin/env python3
"""map_q5.py"""

import sys

#friends_dict = {}
#input comes from STDIN (standard input)
#file_ptr = open("/home/hadoop/mf_data/mf_input_1.txt","r")
"""user_data = open("/home/hadoop/mf_data/userdata.txt","r")
user_data_dict = {}
for line in user_data:
    line = line.strip()
    user_line = line.split(",");
    user_id = int(user_line[0])
    user_data_dict[user_id] = user_line[1:10]"""
line_number = 0
for line in sys.stdin:
        #remove the leading and trailing whitespaces
        line_number = line_number+1
        line = line.strip()
        #split based on tab
        user = line.split(",")
        for i in user:
            print(i,"\t",line_number)
