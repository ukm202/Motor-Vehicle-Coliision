// crashes.scala
// CSCI-UA 476 Processing Big Data for Analytics Applications - Fall 2021
// CODE CONTAINS ONLY OF COMMANDS FOR WORKING WITH SPARK SHELL FOR DATA EXPLORATION
// Last updated: Nov 20, 2021
// Ngoc Hoang - netID nnh245
// --------------------------------------------------------

// Start Spark shell on Peel
// spark-shell --deploy-mode client

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

import sqlContext._
import sqlContext.implicits._

import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.functions.countDistinct

// Load .csv to dataframe
var df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "project/crashes_cleaned.csv", "header" -> "true"))

// Check initial dataframe schema
df.printSchema
// Output:
// root
//  |-- 0: string (nullable = true)
//  |-- CRASH DATE: string (nullable = true)
//  |-- CRASH TIME: string (nullable = true)
//  |-- BOROUGH: string (nullable = true)
//  |-- ZIP CODE: string (nullable = true)
//  |-- LATITUDE: string (nullable = true)
//  |-- LONGITUDE: string (nullable = true)
//  |-- ON STREET NAME: string (nullable = true)
//  |-- CROSS STREET NAME: string (nullable = true)
//  |-- OFF STREET NAME: string (nullable = true)
//  |-- NUMBER OF PEDESTRIANS INJURED: string (nullable = true)
//  |-- NUMBER OF PEDESTRIANS KILLED: string (nullable = true)
//  |-- NUMBER OF CYCLIST INJURED: string (nullable = true)
//  |-- NUMBER OF CYCLIST KILLED: string (nullable = true)
//  |-- NUMBER OF MOTORIST INJURED: string (nullable = true)
//  |-- NUMBER OF MOTORIST KILLED: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 1: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 2: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 3: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 4: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 5: string (nullable = true)
//  |-- COLLISION_ID: string (nullable = true)
//  |-- VEHICLE TYPE CODE 1: string (nullable = true)
//  |-- VEHICLE TYPE CODE 2: string (nullable = true)
//  |-- VEHICLE TYPE CODE 3: string (nullable = true)
//  |-- VEHICLE TYPE CODE 4: string (nullable = true)
//  |-- VEHICLE TYPE CODE 5: string (nullable = true)

// Change column types
// df = df.withColumn("CRASH TIME", to_timestamp($"CRASH TIME", "H:mm"))
df = df.withColumn("0", col("0").cast("integer"))
df = df.withColumn("CRASH DATE TIME", concat(col("CRASH DATE"), lit(' '), col("CRASH TIME")))
df = df.withColumn("CRASH DATE TIME", to_timestamp($"CRASH DATE TIME", "MM/dd/yyyy H:mm"))
df = df.withColumn("CRASH DATE", to_date($"CRASH DATE", "MM/dd/yyyy"))
df = df.withColumn("LATITUDE", col("LATITUDE").cast("double"))
df = df.withColumn("LONGITUDE", col("LONGITUDE").cast("double"))
df = df.withColumn("NUMBER OF PEDESTRIANS INJURED", col("NUMBER OF PEDESTRIANS INJURED").cast("integer"))
df = df.withColumn("NUMBER OF PEDESTRIANS KILLED", col("NUMBER OF PEDESTRIANS KILLED").cast("integer"))
df = df.withColumn("NUMBER OF CYCLIST INJURED", col("NUMBER OF CYCLIST INJURED").cast("integer"))
df = df.withColumn("NUMBER OF CYCLIST KILLED", col("NUMBER OF CYCLIST KILLED").cast("integer"))
df = df.withColumn("NUMBER OF MOTORIST INJURED", col("NUMBER OF MOTORIST INJURED").cast("integer"))
df = df.withColumn("NUMBER OF MOTORIST KILLED", col("NUMBER OF MOTORIST KILLED").cast("integer"))
df = df.withColumn("COLLISION_ID", col("COLLISION_ID").cast("integer"))

// Check revised dataframe schema
df.printSchema
// Output
// root
//  |-- 0: integer (nullable = true)
//  |-- CRASH DATE: date (nullable = true)
//  |-- CRASH TIME: string (nullable = true)
//  |-- BOROUGH: string (nullable = true)
//  |-- ZIP CODE: string (nullable = true)
//  |-- LATITUDE: double (nullable = true)
//  |-- LONGITUDE: double (nullable = true)
//  |-- ON STREET NAME: string (nullable = true)
//  |-- CROSS STREET NAME: string (nullable = true)
//  |-- OFF STREET NAME: string (nullable = true)
//  |-- NUMBER OF PEDESTRIANS INJURED: integer (nullable = true)
//  |-- NUMBER OF PEDESTRIANS KILLED: integer (nullable = true)
//  |-- NUMBER OF CYCLIST INJURED: integer (nullable = true)
//  |-- NUMBER OF CYCLIST KILLED: integer (nullable = true)
//  |-- NUMBER OF MOTORIST INJURED: integer (nullable = true)
//  |-- NUMBER OF MOTORIST KILLED: integer (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 1: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 2: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 3: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 4: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 5: string (nullable = true)
//  |-- COLLISION_ID: integer (nullable = true)
//  |-- VEHICLE TYPE CODE 1: string (nullable = true)
//  |-- VEHICLE TYPE CODE 2: string (nullable = true)
//  |-- VEHICLE TYPE CODE 3: string (nullable = true)
//  |-- VEHICLE TYPE CODE 4: string (nullable = true)
//  |-- VEHICLE TYPE CODE 5: string (nullable = true)
//  |-- CRASH DATE TIME: timestamp (nullable = true)

// Filter rows with null values in integer columns (count columns)
// For homework 8, I already cleaned the data and substitute blank cells with "-1"
// Check count of these filtered rows show 0 -> original data already fills 0
df.filter(df("NUMBER OF PEDESTRIANS INJURED") === -1) // count = 0
df.filter(df("NUMBER OF PEDESTRIANS KILLED") === -1) // count = 0
df.filter(df("NUMBER OF CYCLIST INJURED") === -1) // count = 0
df.filter(df("NUMBER OF CYCLIST KILLED") === -1) // count = 0
df.filter(df("NUMBER OF MOTORIST INJURED") === -1) // count = 0
df.filter(df("NUMBER OF MOTORIST KILLED") === -1) // count = 0

// Safe to add new column = sum of values from other columns
df = df.withColumn("NUMBER OF ALL INJURED", col("NUMBER OF PEDESTRIANS INJURED") + col("NUMBER OF CYCLIST INJURED") + col("NUMBER OF MOTORIST INJURED"))
df = df.withColumn("NUMBER OF ALL KILLED", col("NUMBER OF PEDESTRIANS KILLED") + col("NUMBER OF CYCLIST KILLED") + col("NUMBER OF MOTORIST KILLED"))
// Check schema again:
// ...
//  |-- NUMBER OF ALL INJURED: integer (nullable = true)
//  |-- NUMBER OF ALL KILLED: integer (nullable = true)

// Inspect min, max of two new columns
df.agg(min("NUMBER OF ALL INJURED"), max("NUMBER OF ALL INJURED")).show() // Min: 0, max: 43
df.agg(min("NUMBER OF ALL KILLED"), max("NUMBER OF ALL KILLED")).show() // Min: 0, max: 8

// Filter out crashes in Brooklyn
var brooklyn_crashes = df.filter(df("BOROUGH") === "BROOKLYN") // count = 388208

// Filter crashes where the date is greater than 2017-09-30
brooklyn_crashes.filter(brooklyn_crashes("CRASH DATE").gt(lit("2017-09-30"))) // count = 149143

// Filter crashes where the date is less than 2020-01-01
brooklyn_crashes.filter(brooklyn_crashes("CRASH DATE").lt(lit("2020-01-01"))) // count = 342126

// Number of crashes in between these two dates should be 103061
brooklyn_crashes.filter(brooklyn_crashes("CRASH DATE").gt(lit("2017-09-30"))).filter(brooklyn_crashes("CRASH DATE").lt(lit("2020-01-01"))) // count = 103061

// Inspect values in longitude & latitude columns
// Again, when I cleaned the data I substituted empty cells with -1
// Note: New York is in the Northern - Western hemisphere of the Earth -> positive latitude & negative longitude
// Valid latitudes are > 0 (not -1)
// Valid longitudes are < -2 (not -1)
brooklyn_crashes.filter(brooklyn_crashes("LATITUDE") > 0).filter(brooklyn_crashes("LONGITUDE") < -2) // count = 377919

// Inspect values in zip code column
brooklyn_crashes.filter(brooklyn_crashes("ZIP CODE") === "-1") // count = 5 -> only 5 rows are missing zip code
// Filter out crashes near Brooklyn Bridge (TENTATIVE, using zip code 11201 of the neighborhood)
brooklyn_crashes.filter(brooklyn_crashes("ZIP CODE") === "11201") // count = 14603 -> all crashes happening near Brooklyn Bridge

// Chain filter functions to filter out all crashes that happened:
// - Between 2018-10-01 and 2019-12-31 (dates included in the Brooklyn pedestrian dataset)
// - In the neighborhood that has zip code 11201
var brooklyn_bridge = brooklyn_crashes.filter(brooklyn_crashes("CRASH DATE").gt(lit("2017-09-30"))).filter(brooklyn_crashes("CRASH DATE").lt(lit("2020-01-01")))
// Count = 3774

// Inspect the new dataframe I just got
// Inspect min, max fatalities
brooklyn_bridge.agg(min("NUMBER OF ALL INJURED"), max("NUMBER OF ALL INJURED")).show() // Min: 0, max: 7
brooklyn_bridge.agg(min("NUMBER OF ALL KILLED"), max("NUMBER OF ALL KILLED")).show() // Min: 0, max: 1

// Inspect CONTRIBUTING FACTOR VEHICLE 1
brooklyn_bridge.filter(brooklyn_bridge("CONTRIBUTING FACTOR VEHICLE 1") === "-1") // count = 8 -> 8 rows with empty cells in this column
// Count number of distinct values
brooklyn_bridge.agg(countDistinct("CONTRIBUTING FACTOR VEHICLE 1")).show() // count = 38
// Count occurrences of all distinct values, not sorted
brooklyn_bridge.groupBy("CONTRIBUTING FACTOR VEHICLE 1").count.show
// Count occurrences of all distinct values, sort by count in descending order
brooklyn_bridge.groupBy("CONTRIBUTING FACTOR VEHICLE 1").count.orderBy($"count".desc).show
// Output excerpt:
// +-----------------------------+-----+                                           
// |CONTRIBUTING FACTOR VEHICLE 1|count|
// +-----------------------------+-----+
// |                  Unspecified|  639|
// |         Driver Inattentio...|  515|
// |         Following Too Clo...|  498|
// |          Passing Too Closely|  377|
// ...

// Inspect VEHICLE TYPE CODE 1
// Count occurrences of all distinct values, sort by count in descending order
brooklyn_bridge.groupBy("VEHICLE TYPE CODE 1").count.orderBy($"count".desc).show
// Output excerpt:
// +--------------------+-----+                                                    
// | VEHICLE TYPE CODE 1|count|
// +--------------------+-----+
// |               Sedan| 1868|
// |Station Wagon/Spo...| 1117|
// |                Taxi|  167|
// |       Pick-up Truck|  138|
// ...

// Nov 20

// Seq("1").toDF("date").select(
//     current_timestamp(),
//     date_trunc("Year",current_timestamp()).as("Year"),
//     date_trunc("Month",current_timestamp()).as("Month"),
//     date_trunc("Day",current_timestamp()).as("Day"),
//     date_trunc("Hour",current_timestamp()).as("Hour"),
//     date_trunc("Minute",current_timestamp()).as("Minute")
//   ).show(false)

df = df.withColumn("Time_trunc", date_trunc("Hour", $"CRASH DATE TIME"))

brooklyn_crashes.agg(min("Time_trunc"), max("Time_trunc")).show()
// +-------------------+-------------------+                                       
// |    min(Time_trunc)|    max(Time_trunc)|
// +-------------------+-------------------+
// |2017-10-01 00:00:00|2019-12-31 23:00:00|
// +-------------------+-------------------+


brooklyn_crashes.join(brooklyn, brooklyn_crashes("Time_trunc") === brooklyn("hour_begin"), "inner")

// brooklyn_crashes = 103061, brooklyn = 16057

var join_df = brooklyn_crashes.join(brooklyn, brooklyn_crashes("Time_trunc") === brooklyn("hour_begin"), "inner")
// count = 82955 (83010 if didn't drop null from brooklyn)
join_df.groupBy("weather_summary").count.orderBy($"count".desc).show
join_df.filter(join_df("Time_trunc") === "2017-12-15 17:00:00").select("Time_trunc", "CRASH DATE TIME").show()

join_df.select("CONTRIBUTING FACTOR VEHICLE 1").distinct.collect.foreach(println)

// root
//  |-- 0: integer (nullable = true)
//  |-- CRASH DATE: date (nullable = true)
//  |-- CRASH TIME: string (nullable = true)
//  |-- BOROUGH: string (nullable = true)
//  |-- ZIP CODE: string (nullable = true)
//  |-- LATITUDE: double (nullable = true)
//  |-- LONGITUDE: double (nullable = true)
//  |-- ON STREET NAME: string (nullable = true)
//  |-- CROSS STREET NAME: string (nullable = true)
//  |-- OFF STREET NAME: string (nullable = true)
//  |-- NUMBER OF PEDESTRIANS INJURED: integer (nullable = true)
//  |-- NUMBER OF PEDESTRIANS KILLED: integer (nullable = true)
//  |-- NUMBER OF CYCLIST INJURED: integer (nullable = true)
//  |-- NUMBER OF CYCLIST KILLED: integer (nullable = true)
//  |-- NUMBER OF MOTORIST INJURED: integer (nullable = true)
//  |-- NUMBER OF MOTORIST KILLED: integer (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 1: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 2: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 3: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 4: string (nullable = true)
//  |-- CONTRIBUTING FACTOR VEHICLE 5: string (nullable = true)
//  |-- COLLISION_ID: integer (nullable = true)
//  |-- VEHICLE TYPE CODE 1: string (nullable = true)
//  |-- VEHICLE TYPE CODE 2: string (nullable = true)
//  |-- VEHICLE TYPE CODE 3: string (nullable = true)
//  |-- VEHICLE TYPE CODE 4: string (nullable = true)
//  |-- VEHICLE TYPE CODE 5: string (nullable = true)
//  |-- CRASH DATE TIME: timestamp (nullable = true)
//  |-- NUMBER OF ALL INJURED: integer (nullable = true)
//  |-- NUMBER OF ALL KILLED: integer (nullable = true)
//  |-- Time_trunc: timestamp (nullable = true)
//  |-- Pedestrians: integer (nullable = true)
//  |-- Towards Manhattan: integer (nullable = true)
//  |-- Towards Brooklyn: integer (nullable = true)
//  |-- weather_summary: string (nullable = true)
//  |-- temperature: double (nullable = true)
//  |-- precipitation: double (nullable = true)
//  |-- events: string (nullable = true)
//  |-- hour_begin: timestamp (nullable = true)

var b_crashes_small = brooklyn_crashes.select("CRASH DATE TIME", "Time_trunc", "NUMBER OF ALL INJURED", "NUMBER OF ALL KILLED", "CONTRIBUTING FACTOR VEHICLE 1")
b_crashes_small.groupBy("CONTRIBUTING FACTOR VEHICLE 1").count.orderBy($"count".desc).show
b_crashes_small.filter(b_crashes_small("CONTRIBUTING FACTOR VEHICLE 1") === "-1").select("Time_trunc").show(5)
b_crashes_small.filter(b_crashes_small("Time_trunc") === "2018-08-28 12:00:00").select("CONTRIBUTING FACTOR VEHICLE 1").show
// +-----------------------------+                                                 
// |CONTRIBUTING FACTOR VEHICLE 1|
// +-----------------------------+
// |                 Unsafe Speed|
// |          Passing Too Closely|
// |         Driver Inattentio...|
// |                           -1|
// |         Driver Inattentio...|
// |                  Unspecified|
// |                  Unspecified|
// |         Driver Inattentio...|
// |                  Unspecified|
// |                  Unspecified|
// |         Following Too Clo...|
// |         Driver Inattentio...|
// |                  Unspecified|
// |                  Unspecified|
// |         Passing or Lane U...|
// +-----------------------------+
b_crashes_small = b_crashes_small.withColumn("CONTRIBUTING FACTOR VEHICLE 1", when(col("CONTRIBUTING FACTOR VEHICLE 1").equalTo("-1"), "Unspecified").otherwise(col("CONTRIBUTING FACTOR VEHICLE 1")))
// After this
// +-----------------------------+                                                 
// |CONTRIBUTING FACTOR VEHICLE 1|
// +-----------------------------+
// |                 Unsafe Speed|
// |          Passing Too Closely|
// |         Driver Inattentio...|
// |                  Unspecified|
// |         Driver Inattentio...|
// |                  Unspecified|
// |                  Unspecified|
// |         Driver Inattentio...|
// |                  Unspecified|
// |                  Unspecified|
// |         Following Too Clo...|
// |         Driver Inattentio...|
// |                  Unspecified|
// |                  Unspecified|
// |         Passing or Lane U...|
// +-----------------------------+

import org.apache.spark.sql.functions.{coalesce, lit, typedLit}
val translationMap: Column = typedLit(Map(
  "foo" -> "bar",
  "baz" -> "bab"
))
df2.withColumn("value", coalesce(translationMap($"mov"), lit(""))).show
