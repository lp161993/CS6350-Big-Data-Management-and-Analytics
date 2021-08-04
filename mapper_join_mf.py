#! /usr/bin/env python3
"""mapper_join_mf.py"""

import sys

#friends_dict = {}
#input comes from STDIN (standard input)
#file_ptr = open("/home/hadoop/mf_data/mf_input_1.txt","r")
user_data = open("/home/hadoop/mf_data/userdata.txt","r")
user_data_dict = {}
for line in user_data:
    line = line.strip()
    user_line = line.split(",");
    user_id = user_line[0]
    user_data_dict[user_id] = user_line[1:10]
for line in sys.stdin:
        #remove the leading and trailing whitespaces
        line = line.strip()
        #split based on tab
        line = line.replace("\t"," ")
        user = line.split(" ")
        user = [i for i in user if i] #Remove all spaces in the list
        #print(user)
        if(len(user)>1):              #To avoid users with no friends
            user_A = int(user[0])
            friends_list = []
            user[1] = user[1].strip()
            friends_list_str = user[1].split(",")
            for val in friends_list_str:
                    friends_list.append(int(val)) #converting strings to integers
            for i in range(len(friends_list)):
                    if(friends_list[i]<user_A):
                        key = (friends_list[i], user_A)
                    else:
                        key = (user_A, friends_list[i])
                    output_list = []

                    for j in range(len(friends_list)):
                        if(i != j):
                                name = user_data_dict[j][0]
                                dob = user_data_dict[j][-1]
                                user_dob = (friends_list[j],name,dob)
                                output_list.append(user_dob)
                    print(key,"\t",output_list)
