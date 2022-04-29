from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SQLContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# file modified by me and added '[' ']' also between '{'  added comma ',' charcter to be readble as "json" 
#Change can see below easily
#Define Structure and schema

# [{  -> added '['' character
#   "Type": "Electronics",
#   "Name": "Tv stick",
#   "Country": "GB",
#   "Customer": "{\"username\":\"Customer 1\",\"country_code\":\"GB\",\"country_name\":\"United Kingdom\"}",
#   "Timestamp": "2022-04-01T00:00:38.768Z",
#   "Quantity": "1"
# }, --> Added comma ',' character
# {
#   "Type": "Kitchen",
#   "Name": "Water Bottle",
#   "Country": "GB",
#   "Customer": "{\"username\":\"Customer 2\",\"country_code\":\"PL\",\"country_name\":\"Poland\"}",
#   "Timestamp": "2022-07-01T00:00:38.768Z",
#   "Quantity": "10"
# }
# ] --> added ']'' character

# File manually copied to s3 bucket 
#The file name and URL : s3://bizdata-city-data/ecommercesource/sourceEcommerce.json
# The schema needs to be generated manually (original json structure still broken) based on json structure 
schemaL = StructType([\
    StructField("Type",StringType(),True), \
    StructField("Name",StringType(),True), \
    StructField("Country", StringType(), True), \
    #StructField('Customer',MapType(StringType(),StringType())),\
    StructField('Customer',StringType(),True),\
    StructField("Timestamp",StringType(),True), \
    StructField("Quantity", StringType(), True) ])
#print(schemaL)

# Read json file and get file content
rdd = spark.sparkContext.wholeTextFiles("s3://bizdata-city-data/ecommercesource/sourceEcommerce.json")
ecommerce_Json_String = rdd.collect()[0][1]
#print(ecommerce_Json_String)

#Read json string and parse it. 
df = spark.read.json(sc.parallelize([Sample_Json_String]),schema=schemaL)

#customer column is nested and creted key value pairs so converted tuple 
df2 = df.select(col("Type"),col("Name"),col("Country"),\
          json_tuple(col("Customer"),"username","country_code","country_name"),\
          col("Timestamp"),col("Quantity")) \
    .toDF("Type","Name","Country","customer_username","customer_country_code","customer_country_name","Timestamp","Quantity")

# create view called flattened for using SparkSQL 
df2.createOrReplaceTempView("flattened")

# Create ProductCountry Snowflake dimension ( view )  for Product Table used rank  windows fuction to incremental id's
# To join records i try to generate Unique Keys ( Surrogate Key ) for required Dimension 
# Created view for later join Product table
df_ProductCountry = spark.sql("select distinct rank() OVER(ORDER BY Country) as CountryId ,Country as CountryCode,Country as CountryName from flattened")
df_ProductCountry.createOrReplaceTempView("df_ProductCountry")

# Create CustomerCountry Snowflake dimension ( view )  for Customer  Table used rank  windows fuction to incremental id's
# To join records i try to generate Unique Keys ( Surrogate Key ) for required Dimension 
# Created view for later join Customer  table
df_CustomerCountry = spark.sql("select distinct  rank() OVER(ORDER BY Name,Type,Country) as CountryId ,customer_country_code as CountryCode , customer_country_name as CountryName  from flattened")
df_CustomerCountry.createOrReplaceTempView("df_CustomerCountry")

# Every mart or mini mart need a dimension table of course. I generated table based from json.
#But would be nice to generate manually which includes more date and column inside. 
df_DimDate = spark.sql("select distinct rank() OVER(ORDER BY date(TimeStamp)) as DateId, date(TimeStamp) as DateShort,Year(date(TimeStamp)) as YearShort,Month(date(TimeStamp)) as MonthShort,date_format(date(TimeStamp),'MMMM') as MonthNameShort,dayofweek(date(TimeStamp)) as WeekDayShort,date_format(date(TimeStamp),'EEEE') as DayNameShort from flattened")
df_DimDate.createOrReplaceTempView("df_DimDate")


#Customer Dimension 
# That dimension needs to join with CustomerCountry dimension in order to get country id ( surrogate Key)
# Temp table ( view ) created for using in order to generate fact table 
# With distinct and rank function every customer becoming unique and getting code 
df_Customer = spark.sql("select distinct  rank() OVER(ORDER BY f.customer_username) as CustomerId , f.customer_username as CustomerUserName ,dc.CountryId  from flattened f left join df_CustomerCountry dc on dc.CountryCode = f.customer_country_code")
df_Customer.createOrReplaceTempView("df_Customer")

#Product Dimension 
# That dimension needs to join with ProductCountry dimension in order to get country id ( surrogate Key)
# Temp table ( view ) created for  using in order to generate fact table
# With distinct and rank function every prouct becoming unique and getting code 
df_Product = spark.sql("select distinct  rank() OVER(ORDER BY f.Type,f.Name) as ProductId , f.Name as ProductName, f.Type as ProductType ,f.Country,dc.CountryId  from flattened f left join df_ProductCountry dc on dc.CountryCode = f.Country")
df_Customer.createOrReplaceTempView("df_Product")



#Now all required dimension created 
# We can generate fact table 
df_OrderItem = spark.sql("select c.CustomerId,  d.DateId, f.Quantity, f.Timestamp as OrderItemTimeStamp from flattened f Left join df_Customer c on c.CustomerUserName = f.customer_username left join  df_DimDate d on d.DateShort = date(f.TimeStamp) left join df_Product p on p.ProductName=f.Name and p.ProductType = f.Type and p.Country=f.Country ")
#df_Customer.createOrReplaceTempView("df_OrderItem") #we dont need that line i used for test 

#Now we can fill redshift tables .
### That part of code runs DDL code and create tables under  Redshift

#Lets Generate Tables the Database
#Connection options supports preactions ( inserting a temp value on temp table and excute might work with that option )
# If user doesnt like this method DDL code needs to be executed on Redshift with specific user accounts. 
# preaction_query="
# "begin;create table if not exists ProductCountry(CountryId numeric(18,0) not null,CountryCode varchar(10) not null,CountryName varchar(150) not null,primary key(CountryId) ); create table if not exists Product(ProductId numeric(18,0) not null,ProductName varchar(150) not null,ProductType varchar(50) not null,CountryId numeric(18,0) not null,primary key(ProductId)  ); create table if not exists CustomerCountry(CountryId numeric(18,0) not null,CountryCode varchar(10) not null,CountryName varchar(150) not null,primary key(CountryId)  ); create table if not exists Customer(CustomerId numeric(18,0) not null,CustomerUserName varchar(50) not null,CountryId numeric(18,0) not null,primary key(CustomerId)  ); create table if not exists DimDate(DateId numeric(18,0) not null,DateShort date not null,YearShort integer not null,MonthShort integer not null,MonthNameShort varchar(50) not null,WeekDayShort integer not null, DayNameShort  varchar(50) not null primary key(DateId)  ); create table if not exists OrderItem(CustomerId numeric(18,0) not null,ProductId numeric(18,0) not null,DateId numeric(18,0) not null,Quantity numeric(18,3) not null,OrderItemTimeStamp timestamp  not null ); end;"
# ddl_generate_node = glueContext.write_dynamic_frame.from_jdbc_conf(frame = datasource0, catalog_connection = "test_red", connection_options = {"preactions":preaction_query,"dbtable": "temp_table", "database": "reddwarfdb"},
# redshift_tmp_dir = 's3://bizdata-city-data/temp/', transformation_ctx = "ddl_generate_node")



## That part writes dimensions and fact table to redshift database
# that style is also possible to use 
#each dataframe 
#    df_ProductCountry.write.format("com.databricks.spark.redshift").option("url", "jdbc:redshift://redshift-cluster-1.cyxvhtriyp5m.us-east-1.redshift.amazonaws.com:5439/dev?user=username&password=password").option("dbtable", name).option("forward_spark_s3_credentials", "true").option("tempdir", args["TempDir"]).mode("overwrite").save()

#mode("append").save() # can be modified as append ( only for insert needed) 

# I used friendly version below 

Dim_node1 = glueContext.write_dynamic_frame.from_catalog(
    frame=df_ProductCountry,
    database="reddwarfdb",
    table_name="ProductCountry",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="Dim_node1",
)

Dim_node2 = glueContext.write_dynamic_frame.from_catalog(
    frame=df_CustomerCountry,
    database="reddwarfdb",
    table_name="CustomerCountry",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="Dim_node2",
)

Dim_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=df_DimDate,
    database="reddwarfdb",
    table_name="DimDate",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="Dim_node3",
)
Dim_node4 = glueContext.write_dynamic_frame.from_catalog(
    frame=df_Customer,
    database="reddwarfdb",
    table_name="Customer",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="Dim_node4",
)

Dim_node5 = glueContext.write_dynamic_frame.from_catalog(
    frame=df_Product,
    database="reddwarfdb",
    table_name="Product",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="Dim_node5",
)
Dim_node6 = glueContext.write_dynamic_frame.from_catalog(
    frame=df_OrderItem,
    database="reddwarfdb",
    table_name="OrderItem",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="Dim_node4",
)

job.commit()
