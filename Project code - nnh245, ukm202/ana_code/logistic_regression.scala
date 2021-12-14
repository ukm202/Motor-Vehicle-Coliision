import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.{coalesce, lit, typedLit}
import org.apache.spark.sql.Column

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import org.apache.spark.sql.Row

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegressionModel

import org.apache.hadoop.fs._

val sqlContext = new SQLContext(sc)
import sqlContext._
import sqlContext.implicits._

val fs = FileSystem.get(sc.hadoopConfiguration)

// PROCESS DATASET #1: MOTOR VEHICLE COLLISIONS - CRASHES
var df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "project_final/data_clean/crashes_cleaned.csv", "header" -> "true"))

// Filter crashes that happened in Brooklyn between 2017-09-30 and 2020-01-01 (time window available in dataset #2)
var brooklyn_crashes = df.filter(df("BOROUGH") === "BROOKLYN")
brooklyn_crashes = brooklyn_crashes.withColumn("CRASH DATE TIME", concat(col("CRASH DATE"), lit(' '), col("CRASH TIME")))
brooklyn_crashes = brooklyn_crashes.withColumn("CRASH DATE TIME", to_timestamp($"CRASH DATE TIME", "MM/dd/yyyy H:mm"))
brooklyn_crashes = brooklyn_crashes.withColumn("CRASH DATE", to_date($"CRASH DATE", "MM/dd/yyyy"))
brooklyn_crashes = brooklyn_crashes.filter(brooklyn_crashes("CRASH DATE").gt(lit("2017-09-30"))).filter(brooklyn_crashes("CRASH DATE").lt(lit("2020-01-01")))

// Modify column types by casting to correct data types
brooklyn_crashes = brooklyn_crashes.withColumn("NUMBER OF PEDESTRIANS INJURED", col("NUMBER OF PEDESTRIANS INJURED").cast("integer"))
brooklyn_crashes = brooklyn_crashes.withColumn("NUMBER OF PEDESTRIANS KILLED", col("NUMBER OF PEDESTRIANS KILLED").cast("integer"))
brooklyn_crashes = brooklyn_crashes.withColumn("NUMBER OF CYCLIST INJURED", col("NUMBER OF CYCLIST INJURED").cast("integer"))
brooklyn_crashes = brooklyn_crashes.withColumn("NUMBER OF CYCLIST KILLED", col("NUMBER OF CYCLIST KILLED").cast("integer"))
brooklyn_crashes = brooklyn_crashes.withColumn("NUMBER OF MOTORIST INJURED", col("NUMBER OF MOTORIST INJURED").cast("integer"))
brooklyn_crashes = brooklyn_crashes.withColumn("NUMBER OF MOTORIST KILLED", col("NUMBER OF MOTORIST KILLED").cast("integer"))

// Create new columns based on existing ones
brooklyn_crashes = brooklyn_crashes.withColumn("NUMBER OF ALL INJURED", col("NUMBER OF PEDESTRIANS INJURED") + col("NUMBER OF CYCLIST INJURED") + col("NUMBER OF MOTORIST INJURED"))
brooklyn_crashes = brooklyn_crashes.withColumn("NUMBER OF ALL KILLED", col("NUMBER OF PEDESTRIANS KILLED") + col("NUMBER OF CYCLIST KILLED") + col("NUMBER OF MOTORIST KILLED"))

// Truncate crash date time to the nearest hour (rounding down) -> compatible with dataset #2
brooklyn_crashes = brooklyn_crashes.withColumn("Time_trunc", date_trunc("Hour", $"CRASH DATE TIME"))

brooklyn_crashes = brooklyn_crashes.select("CRASH DATE TIME", "Time_trunc", "ZIP CODE", "NUMBER OF ALL INJURED", "NUMBER OF ALL KILLED", "CONTRIBUTING FACTOR VEHICLE 1")

// When cleaning, all empty cells in CONTRIBUTING FACTOR VEHICLE 1 were replaced with -1 -> assume that all of them were "Unspecified"
brooklyn_crashes = brooklyn_crashes.withColumn("CONTRIBUTING FACTOR VEHICLE 1", when(col("CONTRIBUTING FACTOR VEHICLE 1").equalTo("-1"), "Unspecified").otherwise(col("CONTRIBUTING FACTOR VEHICLE 1")))

// Print out metadata information
println("Dataset #1: Motor Vehicle Collisions - Crashes")
println("After processing (filtered by location and time, columns modified as needed):")
println(s"Number of records: ${brooklyn_crashes.count}")
println("Data types of columns:")
brooklyn_crashes.dtypes.foreach(println)
println()

// Save to HDFS for reuse
// Reference: https://stackoverflow.com/questions/41990086/specifying-the-filename-when-saving-a-dataframe-as-a-csv
println("Saving brooklyn_crashes dataframe to HDFS")
brooklyn_crashes.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("project_final/ana_code/dataframes/brooklyn_crashes")
val brooklyn_crashes_file = fs.globStatus(new Path("project_final/ana_code/dataframes/brooklyn_crashes/part*"))(0).getPath().getName()
fs.rename(new Path("project_final/ana_code/dataframes/brooklyn_crashes/" + brooklyn_crashes_file), new Path("project_final/ana_code/dataframes/brooklyn_crashes.csv"))
println("Saved to project_final/ana_code/dataframes/brooklyn_crashes.csv")
println()

// ============================================
// PROCESS DATASET #2: BROOKLYN BRIDGE AUTOMATED PEDESTRIAN COUNTS DEMONSTRATION PROJECT
var brooklyn = sqlContext.load("com.databricks.spark.csv", Map("path" -> "project_final/data_raw/brooklyn.csv", "header" -> "true"))

// Extract timestamp data and drop unused columns
brooklyn = brooklyn.withColumn("hour_begin", to_timestamp($"hour_beginning", "MM/dd/yyyy h:m:s a"))
brooklyn = brooklyn.drop("hour_beginning", "location", "lat", "long", "Location1", "Towards Manhattan", "Towards Brooklyn")

// Null values in various columns are from the same rows - better to drop all of them (16 rows)
brooklyn = brooklyn.na.drop(Seq("weather_summary"))

// Modify column types by casting to correct data types
brooklyn = brooklyn.withColumn("Pedestrians", col("Pedestrians").cast("integer"))
brooklyn = brooklyn.withColumn("temperature", col("temperature").cast("double"))
brooklyn = brooklyn.withColumn("precipitation", col("precipitation").cast("double"))

// Create new columns based on existing ones
// Turn column events (categorical with many null values) to binary
brooklyn = brooklyn.withColumn("has_event", when(col("events").isNull, "0").otherwise("1"))
brooklyn = brooklyn.withColumn("has_event", col("has_event").cast("integer"))

// Print out metadata information
println("Dataset #2: Brooklyn Bridge Automated Pedestrian Counts Demonstration Project")
println("After processing (filtered by location and time, columns modified as needed, dropped columns):")
println(s"Number of records: ${brooklyn.count}")
println("Data types of columns:")
brooklyn.dtypes.foreach(println)
println()

// Save to HDFS for reuse
println("Saving brooklyn dataframe to HDFS")
brooklyn.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("project_final/ana_code/dataframes/brooklyn")
val brooklyn_file = fs.globStatus(new Path("project_final/ana_code/dataframes/brooklyn/part*"))(0).getPath().getName()
fs.rename(new Path("project_final/ana_code/dataframes/brooklyn/" + brooklyn_file), new Path("project_final/ana_code/dataframes/brooklyn.csv"))
println("Saved to project_final/ana_code/dataframes/brooklyn.csv")
println()

// ============================================
// JOIN 2 DATASETS
var weather_crashes = brooklyn.join(brooklyn_crashes, brooklyn("hour_begin") === brooklyn_crashes("Time_trunc"), "left")

// Encode weather column
val weatherMap: Column = typedLit(Map(
  "sleet" -> "0",
  "wind" -> "1",
  "snow" -> "2",
  "fog" -> "3",
  "rain" -> "4",
  "partly-cloudy-night" -> "5",
  "clear-night" -> "6",
  "cloudy" -> "7",
  "partly-cloudy-day" -> "8",
  "clear-day" -> "9"
))
weather_crashes = weather_crashes.withColumn("weather", coalesce(weatherMap($"weather_summary"), lit("")))
weather_crashes = weather_crashes.withColumn("weather", col("weather").cast("integer"))

// Add binary column has_accident
// Note: there are crashes with 0 casualties -> differentiate between null values and 0 values
weather_crashes = weather_crashes.withColumn("casualties", col("NUMBER OF ALL INJURED") + col("NUMBER OF ALL KILLED"))
weather_crashes = weather_crashes.withColumn("has_accident", when(col("casualties").isNull, 0).otherwise(1))

// Categorize hour by groups - 24 hours -> 6 groups each of consecutive 4 hours
weather_crashes = weather_crashes.withColumn("hour", hour(col("hour_begin")))
weather_crashes = weather_crashes.withColumn("hour_group", (col("hour") / 4).cast("integer"))

// Extract day of week data
// Default encoding: Sunday -> 1, Monday -> 2, ... -> push Sunday to the end so that weekends have closer numerical values
weather_crashes = weather_crashes.withColumn("day_of_week", dayofweek(col("hour_begin")))
val dayMap: Column = typedLit(Map(
  1 -> 7,
  2 -> 1,
  3 -> 2,
  4 -> 3,
  5 -> 4,
  6 -> 5,
  7 -> 6
))
weather_crashes = weather_crashes.withColumn("day_of_week", coalesce(dayMap($"day_of_week"), lit("")))

// Check balance of data (datapoints with accidents and without accidents)
// weather_crashes.groupBy("has_accident").count.orderBy($"count".desc).show
// +------------+-----+                                                            
// |has_accident|count|
// +------------+-----+
// |           1|82955|
// |           0| 1383|
// +------------+-----+
// Artificially balance the data out by sampling from datapoints with accidents
// Using seed 1823 to recreate results
var no_acc = weather_crashes.filter(weather_crashes("has_accident") === 0) // count = 1383
var with_acc_sample = weather_crashes.filter(weather_crashes("has_accident") === 1).sample(0.0166, 1823) // count = 1368
var data_sample = no_acc.union(with_acc_sample)

// Print out metadata information
println("Data sample for model training")
println("After processing:")
println(s"Number of records: ${data_sample.count}")
println()

// Normalize data to produce better results
// Temperature
var temperature_stat = data_sample.agg(min("temperature"), max("temperature"))
var temperature_min = temperature_stat.first.getDouble(0)
var temperature_max = temperature_stat.first.getDouble(1)
data_sample = data_sample.withColumn("temperature_norm", ((col("temperature")-temperature_min)/(temperature_max-temperature_min)))
// Precipitation
var precipitation_stat = data_sample.agg(min("precipitation"), max("precipitation"))
var precipitation_min = precipitation_stat.first.getDouble(0)
var precipitation_max = precipitation_stat.first.getDouble(1)
data_sample = data_sample.withColumn("precipitation_norm", ((col("precipitation")-precipitation_min)/(precipitation_max-precipitation_min)))
// Weather
data_sample = data_sample.withColumn("weather_norm", (col("weather")/9))
// Day of week
data_sample = data_sample.withColumn("day_norm", ((col("day_of_week")-1)/(7-1)))
// Hour group
data_sample = data_sample.withColumn("hour_norm", (col("hour_group")/5))
// Pedestrians
var pedestrians_stat = data_sample.agg(min("Pedestrians"), max("Pedestrians"))
var pedestrians_min = pedestrians_stat.first.getInt(0)
var pedestrians_max = pedestrians_stat.first.getInt(1)
data_sample = data_sample.withColumn("pedestrians_norm", ((col("Pedestrians")-pedestrians_min)/(pedestrians_max-pedestrians_min)))

// Save normalization parameters for future use
val norm_schema = StructType(Array(
    StructField("variable", StringType, true),
    StructField("min", DoubleType, true),
    StructField("max", DoubleType, true)))
var norm_df = spark.createDataFrame(sc.emptyRDD[Row], norm_schema)
var temp_seq = Seq(("temperature", temperature_min, temperature_max)).toDF
var prec_seq = Seq(("precipitation", precipitation_min, precipitation_max)).toDF
var weather_seq = Seq(("weather", 0, 9)).toDF
var day_seq = Seq(("day", 1, 7)).toDF
var hour_seq = Seq(("hour", 0, 5)).toDF
var ped_seq = Seq(("pedestrians", pedestrians_min, pedestrians_max)).toDF
norm_df = norm_df.union(temp_seq).union(prec_seq).union(weather_seq).union(day_seq).union(hour_seq).union(ped_seq)
println("Saving normalization params dataframe to HDFS")
norm_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("project_final/ana_code/dataframes/norm_df")
val norm_file = fs.globStatus(new Path("project_final/ana_code/dataframes/norm_df/part*"))(0).getPath().getName()
fs.rename(new Path("project_final/ana_code/dataframes/norm_df/" + norm_file), new Path("project_final/ana_code/dataframes/norm_df.csv"))
println("Saved to project_final/ana_code/dataframes/norm_df.csv")
println()

// Use normalized columns to feed to the model
var lr_data = data_sample.select("temperature_norm", "precipitation_norm", "weather_norm", "day_norm", "hour_norm", "pedestrians_norm", "has_accident")

val cols = Array("temperature_norm", "precipitation_norm", "weather_norm", "day_norm", "hour_norm", "pedestrians_norm")
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val feature_df = assembler.transform(lr_data)

val indexer = new StringIndexer().setInputCol("has_accident").setOutputCol("label")
val label_df = indexer.fit(feature_df).transform(feature_df)

val seed = 1823
val Array(train, test) = label_df.randomSplit(Array(0.7, 0.3), seed)
println(s"Number of records in training data: ${train.count}")
println(s"Number of records in test data: ${test.count}")
println()

val lr = new LogisticRegression().setMaxIter(150).setRegParam(0.02).setElasticNetParam(0.8)
val lr_model = lr.fit(train)
println("Logistic Regression model finished training.")
println()
val pred = lr_model.transform(test)

val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction").setMetricName("areaUnderROC")
val accuracy = evaluator.evaluate(pred)
println(s"Accuracy score on test data: ${accuracy}")
val pred_train = lr_model.transform(train)
val accuracy_train = evaluator.evaluate(pred_train)
println(s"Accuracy score on training data: ${accuracy_train}")
println(s"Coefficients: ${lr_model.coefficients} Intercept: ${lr_model.intercept}")
println()
// Coefficients: [0.0,1.1674480524231052,1.7884268499883167,0.0,3.0271637837630037,8.083478791471325] Intercept: -3.0356003153483573

println("Saving model to project_final/lr_model")
lr_model.write.overwrite().save("project_final/lr_model")