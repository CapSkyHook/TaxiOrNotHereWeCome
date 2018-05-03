# TaxiOrNotHereWeCome


Project Contents:

build.sbt - used for sbt packaging of application
project - folder used as part of sbt packaging to create JAR file
target - folder used as part of sbt packaging to create JAR files

src/main/scala - holds the object oriented scala code related to the application 
    - TaxiProject.scala - core of the application which performs the overall running and management of the application, preprocessing of data for each task and runs the clustering and denotation tasks
    - DistanceCalculator.scala - Utility for determining the distance in kilometers between two latitude/longitude location pairs

prototypes - holds the spark-shell commands needed in the prototyping of the pipeline for each data source including Citibike, Taxi (Yellow and Green) and Uber

visualization - holds the files for creating the visualization of the final processed data


How To Run The Application:
Requirements: At least version Spark 2.0 for the use of SparkSession and need SBT installed.
1) In the root directory, run the command "sbt package" to create a JAR version of the project 
2) Load a version of Spark of at least 2.0 
3) Submit the spark job using the following command "spark-submit taxi-project_2.11-1.0.jar"