#!/usr/bin/env python3
"""reduce_q5.py"""

from operator import itemgetter
from ast import literal_eval as make_tuple
import sys
from datetime import datetime,date
from dateutil.relativedelta import relativedelta, MO

inverted_dict = {}
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper_mf.py
    input_line = line.split('\t')
    word = input_line[0]
    line_number = input_line[1]

    if word not in inverted_dict:
        inverted_dict[word] = []
    inverted_dict[word].append(line_number)

for key in inverted_dict:
    inverted_dict[key].sort()
    print(key,"\t", inverted_dict[key])
        
    # convert count (currently a string) to tuple
    try:
        key = map_input[0]
        mf_value = make_tuple(map_input[1].strip()) #make_tuple is an user defined name, here it is converted to a list
    except ValueError:
        # count was not a tuple, so silently
        # ignore/discard this line
        continue
    #once we have obtained the key, the same keys are going to end up in the same reducer
    max_age = 0
    today = date.today()
    for i in mf_value:
        dob = user_data_dict[i][-1].strip()
        #print(type(dob))
        dob = dob.replace("-","/")
        dob_obj = datetime.strptime(dob, '%m/%d/%Y')
        time_difference = relativedelta(today, dob_obj)
        age = time_difference.years

        if(age> max_age): max_age = age

    print(key,",",max_age)
