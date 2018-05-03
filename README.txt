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

data_downloads - holds the fields related to downloading the raw data from online sources

application_screenshots - holds images of the running application as well the final outputted visualizations which include a heatmap of the travel coordinates over New York City and a few images of the newly determined Kmeans based subway locations in relation to existing subway stops. Due the nature of the application and us running it on a compute node, we have shown the Spark UI in stages of the clustering algorithm as well as the intermidate running state and final output of the visualization code.

taxi-project_2.11-1.0.jar - jar file version of the application.
Note: This jar file has the URIs in the Dumbo HDFS file system hardcoded. To use different data input and output URIs, the constants at the top of TaxiProject.scala should be changed to desired locations and the jar file need to be remade using "sbt package" in the root folder. Information about the URIs used in the jarfile have been noted below.

These are the URIs we have used in the jar file above and what each value corresponds to:
Raw Data URIs:
Yellow Taxi Raw Data Input Location: YELLOW_TAXI_DATA_PATH = "hdfs:///user/tra290/BDAD/finalProject/yellow-taxi-data/*.csv"
Green Taxi Raw Data Input Location: GREEN_TAXI_DATA_PATH = "hdfs:///user/tra290/BDAD/finalProject/green-taxi-data/*.csv"
Uber Raw Data Input Location: UBER_DATA_DATA_PATH = "hdfs:///user/tra290/BDAD/finalProject/uber-data/*.csv"
Citibike Raw Data Input Location: CITIBIKE_DATA_PATH = "hdfs:///user/tra290/BDAD/Citibike_data/*"
MTA Subway Station Information: INPUT_TRAIN_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/station_data/NYC_Transit_Subway_Entrance_And_Exit_Data.csv"

Intermediate Application State URIs:
Processed Data Input for the KMeans Clustering Function Created During Application: OUTPUT_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/to_clustering/"
Processed Data Input for Cluster Location Denotation Created During Application: PRESERVED_START_END_PTS_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/start_end_data/"
Processed Subway Station Data Created During Application For Cluster Location Denotation: TRAIN_OUTPUT_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/station_data/processed"

Final Output Appliction URIs:
Output Location For KMeans Clustering Determined Locations: CLUSTER_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/clusterCenters"
Output Location For New Station Denotations: CLUSTER_DENOTATIONS_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/clusterDenotations"


How To Run The Application:
Requirements: Using Spark 2.1.0 for the use of SparkSession. Also need SBT installed.
1) In the root directory, run the command "sbt package" to create a JAR version of the project 
2) Load a version of Spark of at least 2.1.0 
3) Submit the spark job using the following command "spark-submit taxi-project_2.11-1.0.jar"

