# New York City Vehicle Collisions: A Study of Brooklyn Bridge
---

Group 20: Ngoc Hoang (nnh245), Uttam Mishra (ukm202)

---

## Directories and files
The project directory is structured as followed:
* README (this file)
* ana_code
    * dataframes
        * brooklyn_crashes.csv
        * brooklyn.csv
        * crashes_weather.csv
        * norm_df.csv
        * weather_factor_casualty.csv
    * logistic_regression.scala
    * crashes_weather.scala
    * weather_casualty.scala
* data_clean (on Peel)
    * clean.csv
    * crashes_cleaned.csv
* data_raw (on Peel)
    * brooklyn.csv
    * crashes.csv
* etl_code
    * nnh245
        * Clean.java
        * CleanMapper.java
        * CleanReducer.java
        * Clean.class
        * CleanMapper.class
        * CleanReducer.class
    * ukm202
        * Clean.java
        * CleanMapper.java
        * CleanReducer.java
        * Clean.class
        * CleanMapper.class
        * CleanReducer.class
* profiling_code
    * nnh245
        * CountRecs.java
        * CountRecsMapper.java
        * CountRecsReducer.java
        * CountRecs.class
        * CountRecsMapper.class
        * CountRecsReducer.class
        * CountRecs.class
    * ukm202
        * CountRecs.java
        * CountRecsMapper.java
        * CountRecsReducer.java
        * CountRecs.class
        * CountRecsMapper.class
        * CountRecsReducer.class
        * CountRecs.class
* screenshots
    * Analytics
    * Cleaning and profiling ([netID])
* test_code
    * brooklyn.scala
    * crashes.scala
---
## Code instructions

### Directory structure
For the code to run on Peel, there must be a similar directory structure as above. Specifically, inside the user's directory, there should be a **`project_final`** directory which contains **`ana_code`**, **`data_clean`**, **`data_raw`**, etc. as described.

### Data sources
The two data sources used in this project can be obtained from NYC Open Data:
1. Motor Vehicle Collisions - Crashes: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95
2. Brooklyn Bridge Automated Pedestrian Counts Demonstration Project: https://data.cityofnewyork.us/Transportation/Brooklyn-Bridge-Automated-Pedestrian-Counts-Demons/6fi9-q3ta

The necessary data is available on Peel, under **`project_final/data_raw`**: `brooklyn.csv` contains the Brooklyn Bridge Pedestrian Counts dataset (#2), and `crashes.csv` contains the Motor Vehicle Collisions dataset (#1). It might be advisable to use these files instead of using the links, since dataset #1 is regularly updated and might be different from the data we used by a few rows.

### Data cleaning and profiling
The code for data cleaning and data profiling can be found in **`etl_code`** and **`profiling_code`**, respectively. Files under **`nnh245`** folders are for processing dataset #1 (`crashes.csv`), and files under **`ukm202`** folders are for dataset #2 (`brooklyn.csv`).

The cleaned data files on Peel are under **`project_final/data_clean`** as `crashes_cleaned.csv` (#1 dataset) and `clean.csv` (#2 dataset).

Code for data cleaning:
```shell
javac -classpath `yarn classpath` -d . CleanMapper.java
javac -classpath `yarn classpath` -d . CleanReducer.java
javac -classpath `yarn classpath`:. -d . Clean.java
jar -cvf Clean.jar Clean*.class
hadoop jar Clean.jar Clean project_final/data_raw/[crashes.csv/brooklyn.csv] project_final/data_clean/output

hdfs dfs -mv project_final/data_clean/output/part-r-00000 project_final/data_clean/[crashes_cleaned.csv/clean.csv]
```

The last command line is for changing the MapReduce output file from `part-r-00000` format to a .csv file.

Code for data profiling:
```shell
javac -classpath `yarn classpath` -d . CountRecsMapper.java
javac -classpath `yarn classpath` -d . CountRecsReducer.java
javac -classpath `yarn classpath`:. -d . CountRecs.java
jar -cvf CountRecs.jar CountRecs*.class
hadoop jar CountRecs.jar CountRecs project_final/data_raw/[crashes.csv/brooklyn.csv] project_final/[some location]
```

### Analytics

The three .scala files under **`project_final/ana_code`** should be run in this order, since the later 2 files rely on .csv files produced in the first script file: `logistic_regression.scala` > `crashes_weather.scala` > `weather_casualty.scala`

#### `logistic_regression.scala`
After putting `logistic_regression.scala` in Peel's home directory, start Spark shell using:
```shell
spark-shell --deploy-mode client
```

Within Spark shell, load the script file:
```scala
:load /home/[netid]/logistic_regression.scala
```

This script file will produce the following files and directories:
* In **`project_final/ana_code/dataframes`**:
    * brooklyn_crashes.csv: dataframe obtained after further processing on dataset #1
    * brooklyn.csv: dataframe obtained after further processing on dataset #2
    * norm_df.csv: dataframe including parameters for data normalization, for future use
* **`project_final/lr_model`**
    * Data and metadata of the Logistic Regression model trained and tested, for future use

The script will also prints several data information and Logistic Regression model evaluations on the shell while executing. See **`screenshots`** for sample runs.

#### `crashes_weather.scala`
After putting `crashes_weather.scala` in Peel's home directory, start Spark shell using:
```shell
park-shell --deploy-mode client
```

Within Spark shell, load the script file:
```scala
:load /home/[netid]/crashes_weather.scala
```

This script file will produce **`project_final/ana_code/dataframes/crashes_weather.csv`**.

#### `weather_casualty.scala`
After putting `weather_casualty.scala` in Peel's home directory, start Spark shell using:
```shell
park-shell --deploy-mode client
```

Within Spark shell, load the script file:
```scala
:load /home/[netid]/weather_casualty.scala
```

This script file will produce **`project_final/ana_code/dataframes/weather_casualty.csv`**.