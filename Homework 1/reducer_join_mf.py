#!/usr/bin/env python3
"""reducer_join_mf.py"""

from operator import itemgetter
from ast import literal_eval as make_tuple
import sys

#input_list = [(0,1), (20, 28193), (1, 29826), (6222, 19272), (28041, 28056)]
#file_ptr = open("mf_q1_output.txt","w")
friends_dict = {}
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper_mf.py
    map_input = line.split('\t')
    # convert count (currently a string) to tuple
    try:
        key = make_tuple(map_input[0].strip())
        mf_value = make_tuple(map_input[1].strip()) #make_tuple is an user defined name, here it is converted to a list
    except ValueError:
        # count was not a tuple, so silently
        # ignore/discard this line
        continue
    #once we have obtained the key, the same keys are going to end up in the same reducer
    if key in friends_dict:
        friends_dict[key] += mf_value
    else:
        friends_dict[key] = mf_value
    #if(key == (0,1)):
    #    print(key,"\t",friends_dict[key])

max_mf_key = ()
max_mf_count = 0
for k in friends_dict:
        output_list = []
        count = 0
        for i in range(len(friends_dict[k])):
                for j in range(i+1,len(friends_dict[k])):
                        if(friends_dict[k][i][0] == friends_dict[k][j][0]):
                                output_list.append((friends_dict[k][j][1],friends_dict[k][j][2]))
                                count = count + 1
        if(count >= max_mf_count):
                max_mf_key = k
                max_mf_count = count
        print(k[0]," ",k[1],"\t[", end="")
        for i in range(len(output_list)): 
                print(output_list[i][0]," : ",output_list[i][1],end="")
                if (i!= len(output_list)-1): print(" , ",end="")
        print("]")

"""        if k in input_list:
                u1 =  k[0]
                u2 =  k[1]
                file_ptr.write("{}".format(u1))
                file_ptr.write(",")
                file_ptr.write("{}".format(u2))
                file_ptr.write("\t")
                file_ptr.write("[")
                for i in output_list:
                    file_ptr.write("{} ".format(i))
                file_ptr.write("]")
                file_ptr.write("\n")

max_mf_file  = open("max_mf_file.txt","w")
max_mf_file.write("{}".format(max_mf_key))
max_mf_file.write("\t")
max_mf_file.write("{}".format(max_mf_count))
max_mf_file.close()
file_ptr.close()"""
        
