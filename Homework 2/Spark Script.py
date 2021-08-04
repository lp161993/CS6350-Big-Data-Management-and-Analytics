# File location and type
file_location = "/FileStore/tables/review.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ":"

# The applied options are for CSV files. For other file types, these will be ignored.
re = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.option("header", first_row_is_header) \
.option("sep", delimiter) \
.load(file_location)

#re = sqlContext.read.format("com.databricks.spark.csv").option("header",
"False").option("delimiter",":").load(file_location)
#re = re.drop("_c1","_c3","_c5")

re = re.drop("_c1","_c3","_c5")
re = re.withColumnRenamed("_c0","review_id")
re = re.withColumnRenamed("_c2","user_id")
re = re.withColumnRenamed("_c4","business_id")
re = re.withColumnRenamed("_c6","stars")

#display(re)
file_location = "/FileStore/tables/business.csv"
file_type = "csv"
bu = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.option("header", first_row_is_header) \
.option("sep", delimiter) \
.load(file_location)

bu = bu.drop("_c1","_c3")
bu = bu.withColumnRenamed("_c0","business_id")
bu = bu.withColumnRenamed("_c2","full_address")
bu = bu.withColumnRenamed("_c4","categories")

bu_filter =bu.filter(bu.full_address.rlike("\wStanford,"))
Q3 = bu_filter.join(re, on="business_id", how="inner")
Q3 = Q3.dropDuplicates()
display(Q3.select("user_id","stars"))
bure = re.join(bu,"business_id","left").where("business_id is not null")
bure = bure.dropDuplicates()
bure1 = bure.select("business_id","full_address","categories","stars")
bure_avg = bure1.groupBy("business_id","full_address","categories").agg({"stars":"avg"})
bure_avg10 = bure_avg.orderBy("avg(stars)", ascending=False).head(10)

display(bure_avg10)

from pyspark.sql.functions import split, explode
from pyspark.sql.functions import *

#We want to remove the List word from the categories column
newBu = bu.withColumn('categories', regexp_replace('categories', 'List', ''))
newBu = newBu.withColumn('categories', regexp_replace('categories', '[()]', ''))
newBu = newBu.withColumn('categories',explode(split('categories',',')))
bu_count = newBu.groupBy('categories').count()

display(bu_count)
bu_count_top10 = bu_count.orderBy('count', ascending=False).head(10)
display(bu_count_top10)

# File location and type
from pyspark.sql.functions import *
from pyspark.sql.window import Window
file_location = "/FileStore/tables/userdata.txt"
file_type = "csv"
# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = "\n"
# The applied options are for CSV files. For other file types, these will be ignored.

ii_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.option("header", first_row_is_header) \
.option("sep", delimiter) \
.load(file_location)

ii_df= ii_df.withColumn("new_column",lit("ABC"))
w = Window().partitionBy('new_column').orderBy(lit('A'))
ii_df = ii_df.withColumn("row_num", row_number().over(w)).drop("new_column")
ii_df =ii_df.withColumnRenamed('_c0','Key')

from pyspark.sql.functions import split, explode
ii_df = ii_df.withColumn('Words',explode(split('Key',',')))
ii_df = ii_df.groupBy("Words").agg({"row_num":"collect_list"})
ii_df = ii_df.withColumnRenamed("collect_list(row_num)","Line Numbers")
display(ii_df)
