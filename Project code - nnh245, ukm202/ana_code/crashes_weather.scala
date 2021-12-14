import org.apache.hadoop.fs._

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

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row

// Load two dataframes created in logistic_regression.scala and join them to create a new dataframe matching crashes with weather conditions
var brooklyn_crashes = sqlContext.load("com.databricks.spark.csv", Map("path" -> "project_final/ana_code/dataframes/brooklyn_crashes.csv", "header" -> "true"))
var brooklyn = sqlContext.load("com.databricks.spark.csv", Map("path" -> "project_final/ana_code/dataframes/brooklyn.csv", "header" -> "true"))
var crashes_weather = brooklyn_crashes.join(brooklyn, brooklyn_crashes("Time_trunc") === brooklyn("hour_begin"), "left")
crashes_weather = crashes_weather.na.drop(Seq("weather_summary"))

val weather_conditions = crashes_weather.select("weather_summary").distinct.map(f=>f.getString(0)).collect.toList
val schema = StructType(Array(
    StructField("weather", StringType, true),
    StructField("factor", StringType, true),
    StructField("weather_count", IntegerType, true),
    StructField("weather_factor_count", IntegerType, true)))
var weather_factor = spark.createDataFrame(sc.emptyRDD[Row], schema)

// Get counts of accident factors by weather condition
for (condition <- weather_conditions) {
    var accidents = crashes_weather.filter(crashes_weather("weather_summary") === condition).select("CONTRIBUTING FACTOR VEHICLE 1")
    var accident_count = accidents.count
    var factors = accidents.select("CONTRIBUTING FACTOR VEHICLE 1").distinct.map(f=>f.getString(0)).collect.toList
    for (factor <- factors) {
        var condition_factor_count = accidents.filter(accidents("CONTRIBUTING FACTOR VEHICLE 1") === factor).count
        var new_row = Seq((condition, factor, accident_count, condition_factor_count)).toDF
        weather_factor = weather_factor.union(new_row)
        }
    }
println("Finished processing")
weather_factor = weather_factor.withColumn("frac", col("weather_factor_count") / col("weather_count"))

// Save to HDFS
weather_factor.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("project_final/ana_code/dataframes/crashes_weather")
val fs = FileSystem.get(sc.hadoopConfiguration)
val crashes_weather_file = fs.globStatus(new Path("project_final/ana_code/dataframes/crashes_weather/part*"))(0).getPath().getName()
fs.rename(new Path("project_final/ana_code/dataframes/crashes_weather/" + crashes_weather_file), new Path("project_final/ana_code/dataframes/crashes_weather.csv"))
println("Saved to project_final/ana_code/dataframes/crashes_weather.csv")
println()

// Some further queries to see the data in interactive mode
// weather_factor = weather_factor.withColumn("weather_count", col("weather_count").cast("integer"))
// weather_factor = weather_factor.withColumn("weather_factor_count", col("weather_factor_count").cast("integer"))
// weather_factor = weather_factor.withColumn("frac", col("weather_factor_count") / col("weather_count"))
// weather_factor.filter(weather_factor("weather") === "clear-day").orderBy($"frac".desc).show
// weather_factor.filter(weather_factor("factor") !== "Unspecified").orderBy($"frac".desc).show