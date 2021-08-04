#! /usr/bin/env python3
"""mapper_mf.py"""

import sys

#friends_dict = {}
#input comes from STDIN (standard input)
#file_ptr = open("/home/hadoop/mf_data/mf_input_1.txt","r")
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
                                output_list.append(friends_list[j])
                    print(key,"\t",output_list)
