var brooklyn = sqlContext.load("com.databricks.spark.csv", Map("path" -> "project/brooklyn.csv", "header" -> "true"))

brooklyn = brooklyn.withColumn("hour_begin", to_timestamp($"hour_beginning", "MM/dd/yyyy h:m:s a"))

brooklyn.groupBy("location").count.show // 16057
// location: Brooklyn Bridge
// lat: 40.7081639691088
// long: -73.9995087014816
// Location1

brooklyn = brooklyn.drop("hour_beginning", "location", "lat", "long", "Location1", "Towards Manhattan", "Towards Brooklyn")

brooklyn.groupBy("events").count.orderBy($"count".desc).show

brooklyn.filter(brooklyn("hour_begin").isNull).count
// Pedestrians: 0
// Towards Manhattan: 0
// Towards Brooklyn: 0
// weather_summary: 16
// temperature: 16
// precipitation: 16
// events: 14933
// hour_begin: 2

brooklyn.filter(brooklyn("weather_summary").isNull).select("weather_summary", "temperature", "precipitation").show
// The same 16 rows, including 2 rows of hour_begin
brooklyn.na.drop(Seq("weather_summary")) // count = 16041 = 16057 - 16

brooklyn.agg(countDistinct("events")).show()

// Schema
// root
//  |-- Pedestrians: string (nullable = true)
//  |-- Towards Manhattan: string (nullable = true)
//  |-- Towards Brooklyn: string (nullable = true)
//  |-- weather_summary: string (nullable = true)
//  |-- temperature: string (nullable = true)
//  |-- precipitation: string (nullable = true)
//  |-- events: string (nullable = true)
//  |-- hour_begin: timestamp (nullable = true)
// brooklyn = brooklyn.withColumn("Towards Manhattan", col("Towards Manhattan").cast("integer"))
// brooklyn = brooklyn.withColumn("Towards Brooklyn", col("Towards Brooklyn").cast("integer"))
brooklyn = brooklyn.withColumn("Pedestrians", col("Pedestrians").cast("integer"))
brooklyn = brooklyn.withColumn("temperature", col("temperature").cast("double"))
brooklyn = brooklyn.withColumn("precipitation", col("precipitation").cast("double"))

// root
//  |-- Pedestrians: integer (nullable = true)
//  |-- Towards Manhattan: integer (nullable = true)
//  |-- Towards Brooklyn: integer (nullable = true)
//  |-- weather_summary: string (nullable = true)
//  |-- temperature: double (nullable = true)
//  |-- precipitation: double (nullable = true)
//  |-- events: string (nullable = true)
//  |-- hour_begin: timestamp (nullable = true)

brooklyn.agg(min("hour_begin"), max("hour_begin")).show()
// +-------------------+-------------------+
// |    min(hour_begin)|    max(hour_begin)|
// +-------------------+-------------------+
// |2017-10-01 00:00:00|2019-12-31 23:00:00|
// +-------------------+-------------------+

brooklyn.select("hour_begin").orderBy($"hour_begin".desc).show(5)

import java.util.Date
import java.text.SimpleDateFormat

val format = new SimpleDateFormat("yyyy-MM-dd h:m:s")
var some_day = format.parse("2021-11-21 23:35:00")

brooklyn = brooklyn.withColumn("has_event", when(col("events").isNull, "0").otherwise("1"))
brooklyn = brooklyn.withColumn("has_event", col("has_event").cast("integer"))

var w_c = brooklyn.join(b_crashes_small, brooklyn("hour_begin") === b_crashes_small("Time_trunc"), "left")
w_c.groupBy("NUMBER OF ALL KILLED").count.orderBy($"count".desc).show
w_c.filter(w_c("hour_begin").isNull).count
// 0, Time_trunc = 1383
w_c = w_c.withColumn("NUMBER OF ALL INJURED", when(col("NUMBER OF ALL INJURED").isNull, 0).otherwise(col("NUMBER OF ALL INJURED")))
w_c = w_c.withColumn("NUMBER OF ALL KILLED", when(col("NUMBER OF ALL KILLED").isNull, 0).otherwise(col("NUMBER OF ALL KILLED")))
w_c = w_c.withColumn("casualties", col("NUMBER OF ALL INJURED") + col("NUMBER OF ALL KILLED"))
w_c = w_c.withColumn("hour", hour(col("hour_begin")))

w_c.select("weather_summary").distinct.collect.foreach(println)
import org.apache.spark.sql.functions.{coalesce, lit, typedLit}
import org.apache.spark.sql.Column
// val weatherMap: Column = typedLit(Map(
//   "fog" -> "0",
//   "partly-cloudy-day" -> "1",
//   "clear-day" -> "2",
//   "rain" -> "3",
//   "cloudy" -> "4",
//   "sleet" -> "5",
//   "clear-night" -> "6",
//   "wind" -> "7",
//   "partly-cloudy-night" -> "8",
//   "snow" -> "9"
// ))
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
w_c = w_c.withColumn("weather", coalesce(weatherMap($"weather_summary"), lit("")))
w_c = w_c.withColumn("weather", col("weather").cast("integer"))

var w_c_ml = w_c.select("temperature", "precipitation", "has_event", "casualties", "hour", "day", "weather")
w_c_ml = w_c_ml.withColumn("has_accident", when(col("casualties") === 0, 0).otherwise(1))
w_c_ml.groupBy("has_accident").count.orderBy($"count".desc).show
// +------------+-----+                                                            
// |has_accident|count|
// +------------+-----+
// |           0|66720|
// |           1|17618|
// +------------+-----+

// https://medium.com/rahasak/logistic-regression-with-apache-spark-b7ec4c98cfcd

// columns that need to added to feature column
import org.apache.spark.ml.feature.VectorAssembler
val cols = Array("temperature", "precipitation", "has_event", "hour", "weather")
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val feature_df = assembler.transform(w_c_ml)
// root
//  |-- temperature: double (nullable = true)
//  |-- precipitation: double (nullable = true)
//  |-- has_event: integer (nullable = true)
//  |-- casualties: integer (nullable = true)
//  |-- hour: integer (nullable = true)
//  |-- weather: integer (nullable = true)
//  |-- has_accident: integer (nullable = false)
//  |-- features: vector (nullable = true)

// StringIndexer define new 'label' column with 'result' column
import org.apache.spark.ml.feature.StringIndexer
val indexer = new StringIndexer().setInputCol("has_accident").setOutputCol("label")
val label_df = indexer.fit(feature_df).transform(feature_df)
// root
//  |-- temperature: double (nullable = true)
//  |-- precipitation: double (nullable = true)
//  |-- has_event: integer (nullable = true)
//  |-- casualties: integer (nullable = true)
//  |-- hour: integer (nullable = true)
//  |-- weather: integer (nullable = true)
//  |-- has_accident: integer (nullable = false)
//  |-- features: vector (nullable = true)
//  |-- label: double (nullable = false)

// split data set training and test
// training data set - 70%
// test data set - 30%
val seed = 5043
val Array(train, test) = label_df.randomSplit(Array(0.7, 0.3), seed)
import org.apache.spark.ml.classification.LogisticRegression
val logisticRegression = new LogisticRegression().setMaxIter(100).setRegParam(0.02).setElasticNetParam(0.8)
val logisticRegressionModel = logisticRegression.fit(train)
// logisticRegressionModel: org.apache.spark.ml.classification.LogisticRegressionModel = LogisticRegressionModel: uid = logreg_fb03c661a5cd, numClasses = 2, numFeatures = 5
// run model with test data set to get predictions
// this will add new columns rawPrediction, probability and prediction
val pred_df = logisticRegressionModel.transform(test)

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction").setMetricName("areaUnderROC")
val accuracy = evaluator.evaluate(pred_df)
// 0.5
// scala> pred_df.groupBy("prediction").count.show
// +----------+-----+                                                              
// |prediction|count|
// +----------+-----+
// |       0.0|25223|
// +----------+-----+

// scala> w_c_ml.groupBy("has_accident").count.show
// +------------+-----+                                                            
// |has_accident|count|
// +------------+-----+
// |           1|17618|
// |           0|66720|
// +------------+-----+

// w_c_ml.drop(w_c_ml.query("has_accident === 0").sample(frac=.3).index) // DOES NOT WORK
// var to_drop = w_c.filter(w_c_ml("casualties") === 0).sample(0.7, 123) // count = 46878
// var w_c_small = w_c.join(to_drop, w_c("hour_begin") === to_drop("hour_begin"), "leftanti")
// Count = 29947

var with_acc = w_c.filter(w_c("casualties") > 0) // count = 17618
var no_acc_keep = w_c.filter(w_c("casualties") === 0).sample(0.3, 123) // count = 20075
var w_c_small = with_acc.union(no_acc_keep) // count = 37693
var w_c_small_ml = w_c_small.select("temperature", "precipitation", "has_event", "casualties", "hour", "weather")
w_c_small_ml = w_c_small_ml.withColumn("has_accident", when(col("casualties") === 0, 0).otherwise(1))
w_c_small_ml.groupBy("has_accident").count.orderBy($"count".desc).show
// +------------+-----+                                                            
// |has_accident|count|
// +------------+-----+
// |           0|20075|
// |           1|17618|
// +------------+-----+
val feature_df2 = assembler.transform(w_c_small_ml)
val label_df2 = indexer.fit(feature_df2).transform(feature_df2)
val Array(train2, test2) = label_df2.randomSplit(Array(0.7, 0.3), seed)

val logisticRegression = new LogisticRegression().setMaxIter(100).setRegParam(0.02).setElasticNetParam(0.8)
val logisticRegressionModel = logisticRegression.fit(train2)
// logisticRegressionModel: org.apache.spark.ml.classification.LogisticRegressionModel = LogisticRegressionModel: uid = logreg_fb03c661a5cd, numClasses = 2, numFeatures = 5
// run model with test data set to get predictions
// this will add new columns rawPrediction, probability and prediction
val pred_df2 = logisticRegressionModel.transform(test2)

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction").setMetricName("areaUnderROC")
val accuracy = evaluator.evaluate(pred_df2)
// 0.5029180771510242
// +----------+-----+                                                              
// |prediction|count|
// +----------+-----+
// |       0.0|11306|
// |       1.0|  193|
// +----------+-----+
// On train2: accuracy = 0.503575119461819
println(s"Coefficients: ${logisticRegressionModel.coefficients} Intercept: ${logisticRegressionModel.intercept}")

w_c.filter(w_c("casualties") > 0).groupBy("hour").count.orderBy($"count".desc).show
w_c = w_c.withColumn("hour_group", (col("hour") / 4).cast("integer"))
w_c.filter(w_c("casualties") > 0).groupBy("hour_group").count.orderBy($"count".desc).show
// +----------+-----+                                                              
// |hour_group|count|
// +----------+-----+
// |         4| 4909| - 0.23
// |         3| 4017| - 0.18
// |         2| 3115| - 0.17
// |         5| 2950| - 0.27
// |         0| 1327| - 0.21
// |         1| 1300| - 0.20
// +----------+-----+
// Group without filter
// +----------+-----+                                                              
// |hour_group|count|
// +----------+-----+
// |         3|21587|
// |         4|21261|
// |         2|18069|
// |         5|10829|
// |         1| 6345|
// |         0| 6247|
// +----------+-----+

// I TREATED ALL 0 CASUALTY ROWS AS NO ACCIDENT - THERE ARE ACCIDENTS WITHOUT ANY CASUALTY
b_crashes_small = b_crashes_small.withColumn("casualties", col("NUMBER OF ALL INJURED") + col("NUMBER OF ALL KILLED"))
b_crashes_small.groupBy("casualties").count.orderBy($"count".desc).show

var w_c = brooklyn.join(b_crashes_small, brooklyn("hour_begin") === b_crashes_small("Time_trunc"), "left")
// Count = 84338
w_c.filter(w_c("casualties").isNull).count // = 1383

// After narrowing down to Brooklyn Bridge
// Count = 16407
// Null count in casualties = 13355
w_c = w_c.withColumn("weather", coalesce(weatherMap($"weather_summary"), lit("")))
w_c = w_c.withColumn("weather", col("weather").cast("integer"))
w_c = w_c.withColumn("hour", hour(col("hour_begin")))
w_c = w_c.withColumn("day", dayofweek(col("hour_begin")))

var w_c_ml = w_c.select("Pedestrians", "temperature", "precipitation", "has_event", "casualties", "hour", "day", "weather")
w_c_ml = w_c_ml.withColumn("has_accident", when(col("casualties").isNull, 0).otherwise(1))
w_c_ml.groupBy("has_accident").count.show
// +------------+-----+                                                            
// |has_accident|count|
// +------------+-----+
// |           1| 3052|
// |           0|13355|
// +------------+-----+
var with_acc = w_c_ml.filter(w_c_ml("has_accident") === 1) // count = 3052
var no_acc_keep = w_c_ml.filter(w_c_ml("has_accident") === 0).sample(0.23, 123) // count = 3099
var w_c_small_ml = with_acc.union(no_acc_keep)
// +------------+-----+                                                            
// |has_accident|count|
// +------------+-----+
// |           1| 3052|
// |           0| 3099|
// +------------+-----+
w_c_small_ml = w_c_small_ml.withColumn("hour_group", (col("hour") / 4).cast("integer"))
w_c_small_ml.filter(w_c_small_ml("has_accident") === 1).groupBy("hour_group").count.orderBy($"count".desc).show
// +----------+-----+                                                              
// |hour_group|count|
// +----------+-----+
// |         3|  861|
// |         2|  725|
// |         4|  644|
// |         5|  414|
// |         1|  206|
// |         0|  202|
// +----------+-----+

val cols = Array("Pedestrians", "temperature", "precipitation", "has_event", "hour_group", "weather")
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val feature_df = assembler.transform(w_c_small_ml)

val indexer = new StringIndexer().setInputCol("has_accident").setOutputCol("label")
val label_df = indexer.fit(feature_df).transform(feature_df)

val seed = 5043
val Array(train, test) = label_df.randomSplit(Array(0.7, 0.3), seed)
val logisticRegression = new LogisticRegression().setMaxIter(150).setRegParam(0.02).setElasticNetParam(0.8)
val logisticRegressionModel = logisticRegression.fit(train)

val pred_df = logisticRegressionModel.transform(test)

val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction").setMetricName("areaUnderROC")
val accuracy = evaluator.evaluate(pred_df)
// 0.5934197723868767
// scala> pred_df.groupBy("prediction").count.show
// +----------+-----+                                                              
// |prediction|count|
// +----------+-----+
// |       0.0|  944|
// |       1.0|  969|
// +----------+-----+
// On train set: accuracy = 0.60527295731617
println(s"Coefficients: ${logisticRegressionModel.coefficients} Intercept: ${logisticRegressionModel.intercept}")
// Coefficients: [1.8358676324187022E-4,0.0,0.752108844478169,-0.1110754639439769,0.0977312347808858,-0.06573679406439706] Intercept: -0.17779374860098224

val mlr = new LogisticRegression().setMaxIter(150).setRegParam(0.02).setElasticNetParam(0.8).setFamily("multinomial")
val mlrModel = mlr.fit(train)
val pred_df = mlrModel.transform(test)
// On test set: accuracy = 0.5934197723868767
println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

w_c_small_ml.groupBy("Pedestrians").count.orderBy($"Pedestrians".desc).show
w_c_small_ml.filter(w_c_small_ml("has_accident") === 1).groupBy("weather").count.orderBy($"count".desc).show
// +-------+-----+                                                                 
// |weather|count|
// +-------+-----+
// |      2|  853| - clear-day
// |      1|  809| - partly-cloudy-day
// |      4|  433| - cloudy
// |      6|  422| - clear-night
// |      8|  313| - partly-cloudy-night
// |      3|  190| - rain
// |      0|   16| - fog
// |      9|   11| - snow
// |      7|    4| - wind
// |      5|    1| - sleet
// +-------+-----+
w_c.filter(w_c("casualties") >= 0).groupBy("weather_summary").count.orderBy($"count".desc).show
// +-------------------+-----+                                                     
// |    weather_summary|count|
// +-------------------+-----+
// |          clear-day|  853|
// |  partly-cloudy-day|  809|
// |             cloudy|  433|
// |        clear-night|  422|
// |partly-cloudy-night|  313|
// |               rain|  190|
// |                fog|   16|
// |               snow|   11|
// |               wind|    4|
// |              sleet|    1|
// +-------------------+-----+
w_c.filter(w_c("casualties") >= 0).groupBy("hour").count.orderBy($"count".desc).show

w_c = w_c.withColumn("day", dayofweek(col("hour_begin")))
w_c.groupBy("day").count.orderBy($"count".desc).show

var w_c_ml = w_c.select("Pedestrians", "temperature", "precipitation", "has_event", "casualties", "hour", "day", "weather")
w_c_ml = w_c_ml.withColumn("has_accident", when(col("casualties").isNull, 0).otherwise(1))
w_c_ml.groupBy("has_accident").count.show
// +------------+-----+                                                            
// |has_accident|count|
// +------------+-----+
// |           1| 3052|
// |           0|13355|
// +------------+-----+
w_c_ml.filter(w_c_ml("has_accident") === 1).groupBy("day").count.orderBy($"count".desc).show
val dayMap: Column = typedLit(Map(
  1 -> 7,
  2 -> 1,
  3 -> 2,
  4 -> 3,
  5 -> 4,
  6 -> 5,
  7 -> 6
))
w_c_ml = w_c_ml.withColumn("day", coalesce(dayMap($"day"), lit("")))
w_c_ml = w_c_ml.withColumn("day", col("day").cast("integer"))

var with_acc = w_c_ml.filter(w_c_ml("has_accident") === 1) // count = 3052
var no_acc_keep = w_c_ml.filter(w_c_ml("has_accident") === 0).sample(0.23, 123) // count = 3099
var w_c_small_ml = with_acc.union(no_acc_keep)
w_c_small_ml.groupBy("has_accident").count.show
// +------------+-----+                                                            
// |has_accident|count|
// +------------+-----+
// |           1| 3052|
// |           0| 3099|
// +------------+-----+
w_c_small_ml = w_c_small_ml.withColumn("hour_group", (col("hour") / 4).cast("integer"))

import org.apache.spark.ml.classification.NaiveBayes
val NBmodel = new NaiveBayes().fit(train)
val pred = NBmodel.transform(test)
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
val eval = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
// Accuracy = 0.5875588081547308

w_c_small_ml.describe("Pedestrians").show()
// +-------+-----------------+                                                     
// |summary|      Pedestrians|
// +-------+-----------------+
// |  count|             6151|
// |   mean|823.8475044708177|
// | stddev| 886.849696338565|
// |    min|                0|
// |    max|             4330|
// +-------+-----------------+
// Normalization: x' = (x' - x_min) / (x_max - x_min)
w_c_small_ml = w_c_small_ml.withColumn("ped_norm", (col("Pedestrians") / 4330))

val cols = Array("Pedestrians", "temperature", "precipitation", "has_event", "hour_group", "day", "weather")
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val feature_df = assembler.transform(w_c_small_ml)

val indexer = new StringIndexer().setInputCol("has_accident").setOutputCol("label")
val label_df = indexer.fit(feature_df).transform(feature_df)

val seed = 5043
val Array(train, test) = label_df.randomSplit(Array(0.7, 0.3), seed)
val logisticRegression = new LogisticRegression().setMaxIter(150).setRegParam(0.02).setElasticNetParam(0.8)
val logisticRegressionModel = logisticRegression.fit(train)

val pred_df = logisticRegressionModel.transform(test)

val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction").setMetricName("areaUnderROC")
val accuracy = evaluator.evaluate(pred_df)
// Accuracy = 0.5934197723868767 (with pred_norm)
// Accuracy = 0.5919652130183337 (without pred_norm)
// Accuracy = 0.5929190672453565 (with Pedestrian & day)
// Coefficients: [1.8247252113894857E-4,0.0,0.7456965176701134,-0.11090835437763123,0.09784445441463899,0.0,-0.06641060350649787] Intercept: -0.1747535542162888

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row

val f_cols = Array("Pedestrians", "temperature", "precipitation", "has_event", "hour_group", "day", "weather", "has_accident")
val assembler = new VectorAssembler().setInputCols(f_cols).setOutputCol("features")
val features = assembler.transform(w_c_small_ml)

val Row(coeff1: Matrix) = Correlation.corr(features, "features").head
println("Pearson correlation matrix:\n" + coeff1.toString)
println(coeff1.toString(8,Int.MaxValue))
// 1.0                    0.396479901022956      -0.1198996561407998    -0.028912779324748082  0.26196829968816054    0.07676899100276592    0.5981974724372969     0.18069577978880258    
// 0.396479901022956      1.0                    -0.009242605680079272  -0.08626887793171485   0.10412937782291769    -0.019916187830890615  0.2114811915046592     0.04984696016574045    
// -0.1198996561407998    -0.009242605680079272  1.0                    0.004835153997229062   0.02503639627497488    0.005528260982336796   -0.3209328129874895    0.03301308372077867    
// -0.028912779324748082  -0.08626887793171485   0.004835153997229062   1.0                    -0.009753286171065231  -0.012653166479660248  -0.028277758618003083  -0.032347793316907264  
// 0.26196829968816054    0.10412937782291769    0.02503639627497488    -0.009753286171065231  1.0                    -0.004647555381403589  0.0540987198496203     0.14098005717584788    
// 0.07676899100276592    -0.019916187830890615  0.005528260982336796   -0.012653166479660248  -0.004647555381403589  1.0                    -0.03479311640767717   -0.06581792232743477   
// 0.5981974724372969     0.2114811915046592     -0.3209328129874895    -0.028277758618003083  0.0540987198496203     -0.03479311640767717   1.0                    0.1441462635359445     
// 0.18069577978880258    0.04984696016574045    0.03301308372077867    -0.032347793316907264  0.14098005717584788    -0.06581792232743477   0.1441462635359445     1.0

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
val cols = Array("Pedestrians", "temperature", "precipitation", "has_event", "hour_group", "day", "weather")
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val feature_df = assembler.transform(w_c_small_ml)

val indexer = new StringIndexer().setInputCol("has_accident").setOutputCol("label")
val label_df = indexer.fit(feature_df).transform(feature_df)
val chi = ChiSquareTest.test(label_df, "features", "label").head

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

val labelIndexer = new StringIndexer().setInputCol("has_accident").setOutputCol("indexedLabel").fit(feature_df)
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(10).fit(feature_df)
val Array(trainingData, testData) = feature_df.randomSplit(Array(0.7, 0.3))
val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(10)
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labelsArray(0))

// import org.apache.spark.ml.linalg.{Matrix, Vectors}
// import org.apache.spark.ml.stat.Correlation
// import org.apache.spark.sql.Row

// val f_cols = Array("temperature_norm", "precipitation_norm", "weather_norm", "day_norm", "hour_norm", "pedestrians_norm", "has_accident")
// val assembler = new VectorAssembler().setInputCols(f_cols).setOutputCol("features")
// val features = assembler.transform(lr_data)

// val Row(coeff1: Matrix) = Correlation.corr(features, "features").head
// println("Pearson correlation matrix:\n" + coeff1.toString)
// println(coeff1.toString(7,Int.MaxValue))
// 1.0                    -0.006050602161210233  0.222989658487361    0.021353670072886934  0.17294596360333328  0.3668148910644744    0.20145167318937823  
// -0.006050602161210233  1.0                    -0.2905293270970055  0.03430303991995825   0.0623246113882177   -0.07591001319620934  0.03681932409565573  
// 0.222989658487361      -0.2905293270970055    1.0                  0.07894869053255617   0.2928345087198152   0.6358077624680863    0.46707985850083855  
// 0.021353670072886934   0.03430303991995825    0.07894869053255617  1.0                   0.10115596158556392  0.15877273219706786   0.09997142562833429  
// 0.17294596360333328    0.0623246113882177     0.2928345087198152   0.10115596158556392   1.0                  0.49697364626795354   0.6523656472693898   
// 0.3668148910644744     -0.07591001319620934   0.6358077624680863   0.15877273219706786   0.49697364626795354  1.0                   0.6107248923458832   
// 0.20145167318937823    0.03681932409565573    0.46707985850083855  0.09997142562833429   0.6523656472693898   0.6107248923458832    1.0  