CS6350 Big Data Management and Analytics

Homework #1

All the codes have been written in Python, Hadoop was installed on Ubuntu 20.04 
in VirtualBox

There are a total of 8 codes(4 Map + 4 reduce) written in Python

Q1,Q2 of the homework is solved using mapper_mf.py, reducer_mf.py
Q3 of HW1 is solved using mapper_join_mf.py, reducer_join_mf.py
Q4 of HW1 is solved using map_q4.py, reduce_q4.py
Q5 of HW1 is solved using map_q5.py, reduce_q5.py

Q1 was executed using hadoop streaming with the following command in Ubuntu in 
VirtualBox

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar \
-files /home/hadoop/codes \
-mapper /home/hadoop/codes/mapper_mf.py   \
-reducer /home/hadoop/codes/reducer_mf.py  \
-input /hadooponubuntu/soc-LiveJournal1Adj.txt \
-output /test/mf_op1

Q2 has two users with maximum number of mutual friends and it is directly stored 
in a file named max_mf_file.txt 

Q3,Q4 has in-memory join in the code and has execution similar to the command 
executed above

Q5 contructs an inverted index and uses only userdata.txt as input.

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar \
-files /home/hadoop/codes \
-mapper /home/hadoop/codes/map_q5.py   \
-reducer /home/hadoop/codes/reduce_q5.py  \
-input /hadooponubuntu/userdata.txt \
-output /test/invertedindex