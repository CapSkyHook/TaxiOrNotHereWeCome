# TaxiOrNotHereWeCome


Project Contents:

Note: We are still in the process of refactoring from files holding spark-shell commands to proper class files for our application

build.sbt - used for sbt packaging of application
project - folder used as part of sbt packaging to create JAR file
target - folder used as part of sbt packaging to create JAR files

src/main/scala - holds the object oriented scala code related to the application 
    
    - preprocessing.scala - performs the preprocessing on the data
    - TaxiProject.scala - performs the overall running and management of the application
    - visualize_preprocess - visualizes the preprocessed data

prototypes - holds the spark-shell commands needed in the prototyping of the pipeline for each data source including Citibike, Taxi (Yellow and Green) and Uber

visualization - holds the prototyping files for creating the visualization of the final processed data


