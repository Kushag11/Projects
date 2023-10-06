# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC  %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df1987 =  spark.read.option("Header",True)\
               .option("Inferschema",True)\
               .csv("dbfs:/databricks-datasets/asa/airlines/1987.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1)  Create a DF(airlines_1987_to_2008) from this path
# MAGIC
# MAGIC           %fs ls dbfs:/databricks-datasets/asa/airlines/
# MAGIC
# MAGIC           (There are csv files in airlines folder. It contains 1987.csv to  2008.csv files. 
# MAGIC
# MAGIC           Create only one DF from all the files )

# COMMAND ----------

df = spark.read.option("Header",True)\
               .option("Inferschema",True)\
               .csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2)  Create a PySpark Datatypes schema for the above DF

# COMMAND ----------

airschema = StructType([
              StructField("Year",IntegerType()),
              StructField("Month",IntegerType()),
              StructField("DayofMonth",IntegerType()),
              StructField("DayofWeek",IntegerType()),
              StructField("DepTime",IntegerType()),
              StructField("CrsDepTime",IntegerType()),
              StructField("ArrTime",IntegerType()),
              StructField("CrsArrTime",IntegerType()),
              StructField("UniqueCarrier",StringType()),
              StructField("FlightNum",IntegerType()),
              StructField("TailNum",StringType()),
              StructField("ActualElapsedTime",StringType()),
              StructField("CrsElapsedTime",StringType()),
              StructField("AirTime",StringType()),
              StructField("ArrDelay",StringType()),
              StructField("DepDelay",StringType()),
              StructField("Origin",StringType()),
              StructField("Dest",StringType()),
              StructField("Distance",StringType()),
              StructField("TaxiIn",StringType()),
              StructField("TaxiOut",StringType()),
              StructField("Cancelled",IntegerType()),
              StructField("CancellationCode",StringType()),
              StructField("Diverted",IntegerType()),
              StructField("CarrierDelay",StringType()),
              StructField("WeatherDelay",StringType()),
              StructField("NasDelay",StringType()),
              StructField("SecurityDelay",StringType()),
              StructField("LateAircraftDelay",StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) View the dataframe

# COMMAND ----------

df1 = spark.read.option("Header",True)\
               .schema(airschema)\
               .csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####4) Return count of records in dataframe

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####5) Select the columns - Origin, Dest and Distance

# COMMAND ----------

df1.select("Origin","Dest","Distance").display()

# COMMAND ----------

df1.where("Year == 1987").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6) Filtering data with 'where' method, where Year = 2001

# COMMAND ----------

df1.where("Year == 2001").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####7) Create a new dataframe (airlines_1987_to_2008_drop_DayofMonth) exluding dropped column (“DayofMonth”)

# COMMAND ----------

dfnew = df1.drop("DayOfMonth")

# COMMAND ----------

# MAGIC %md
# MAGIC ####8) Display new DataFrame

# COMMAND ----------

dfnew.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 9) Create column 'Weekend' and a new dataframe(AddNewColumn) and display

# COMMAND ----------

dfAddNewColumn = df1.withColumn("Weekend",when(col("DayOfWeek")=='1',"Sunday")
                                         .when(col("DayOfWeek")=='2',"Monday")
                                         .when(col("DayOfWeek")=='3',"Tueday")
                                         .when(col("DayOfWeek")=='4',"Wednesday")
                                         .when(col("DayOfWeek")=='5',"Thursday")
                                         .when(col("DayOfWeek")=='6',"Friday")
                                         .when(col("DayOfWeek")=='7',"Saturday")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 10) Cast ActualElapsedTime column to integer and use printschema to verify

# COMMAND ----------

df1.withColumn("ActualElapsedTime",col("ActualElapsedTime").cast(IntegerType())).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####11) Rename 'DepTime' to 'DepartureTime'

# COMMAND ----------

df1.withColumnRenamed('DepTime','DepartureTime').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####12) Drop duplicate rows based on Year and Month and Create new df (Drop Rows)

# COMMAND ----------

dfdroprows = df1.dropDuplicates(['Year','Month']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####13) Display Sort by descending order for Year Column using sort()

# COMMAND ----------

df1.sort(col("Year").desc()).display()

# COMMAND ----------

df1.groupBy("Dest").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 14) Group data according to Origin and returning count

# COMMAND ----------

df1.groupBy("Origin").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 15) Group data according to dest and finding maximum value for each 'Dest'

# COMMAND ----------

df2 = df1.withColumn("Distance",col("Distance").cast(IntegerType()))

# COMMAND ----------

df2.groupBy("Dest").max("Distance").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####16) Write data in Delta format

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("airlines")
