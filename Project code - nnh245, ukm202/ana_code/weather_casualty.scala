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

// Add a new column of casualties
crashes_weather = crashes_weather.withColumn("casualties", col("NUMBER OF ALL INJURED") + col("NUMBER OF ALL KILLED"))
val weather_conditions = crashes_weather.select("weather_summary").distinct.map(f=>f.getString(0)).collect.toList
val schema = StructType(Array(
    StructField("weather", StringType, true),
    StructField("factor", StringType, true),
    StructField("injured", IntegerType, true),
    StructField("killed", IntegerType, true),
    StructField("casualties", IntegerType, true)))
var weather_factor_casualty = spark.createDataFrame(sc.emptyRDD[Row], schema)

// Get sums of different casualty groups by accident factors and weather conditions
for (condition <- weather_conditions) {
    var accidents = crashes_weather.filter(crashes_weather("weather_summary") === condition).select("CONTRIBUTING FACTOR VEHICLE 1", "NUMBER OF ALL INJURED", "NUMBER OF ALL KILLED", "casualties")
    var factors = accidents.select("CONTRIBUTING FACTOR VEHICLE 1").distinct.map(f=>f.getString(0)).collect.toList
    for (factor <- factors) {
        var condition_factor_casualty = accidents.filter(accidents("CONTRIBUTING FACTOR VEHICLE 1") === factor)
        var agg_df = condition_factor_casualty.agg(sum("NUMBER OF ALL INJURED"), sum("NUMBER OF ALL KILLED"), sum("casualties"))
        agg_df = agg_df.withColumn("sum(NUMBER OF ALL INJURED)", col("sum(NUMBER OF ALL INJURED)").cast("integer"))
        agg_df = agg_df.withColumn("sum(NUMBER OF ALL KILLED)", col("sum(NUMBER OF ALL KILLED)").cast("integer"))
        agg_df = agg_df.withColumn("sum(casualties)", col("sum(casualties)").cast("integer"))
        var all_injured = agg_df.first.getInt(0)
        var all_killed = agg_df.first.getInt(1)
        var all_casualties = agg_df.first.getInt(2)
        var new_row = Seq((condition, factor, all_injured, all_killed, all_casualties)).toDF
        weather_factor_casualty = weather_factor_casualty.union(new_row)
        }
    }
println("Finished processing")

// Save to HDFS
weather_factor_casualty.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("project_final/ana_code/dataframes/weather_factor_casualty")
val fs = FileSystem.get(sc.hadoopConfiguration)
val weather_factor_casualty_file = fs.globStatus(new Path("project_final/ana_code/dataframes/weather_factor_casualty/part*"))(0).getPath().getName()
fs.rename(new Path("project_final/ana_code/dataframes/weather_factor_casualty/" + weather_factor_casualty_file), new Path("project_final/ana_code/dataframes/weather_factor_casualty.csv"))
println("Saved to project_final/ana_code/dataframes/weather_factor_casualty.csv")
println()

// Some further queries to see the data in interactive mode
// weather_factor_casualty.filter(weather_factor_casualty("factor") !== "Unspecified").orderBy($"casualties".desc).show(357)