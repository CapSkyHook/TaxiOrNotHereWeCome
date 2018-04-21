import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

// Load file into RDD
val path = "hdfs:///user/tra290/BDAD/finalProject/uber-data/*.csv"
val uber_rdd = sc.textFile(path)


// Since we loaded as a RDD, remove any rows that correspond to the header
val header = uber_rdd.first()
val uber_rdd_wo_header = uber_rdd.filter(line => line != header)

// Split the header row into a sequence to make it easier when converting to data frame
val uber_columns = header.split(",").toSeq

// Convert the longitude and latitude to doubles
val uber_rdd_split_long_lat_converted = uber_rdd_wo_header.map(row => row.split(",")).map(row => (row(0), row(1).toDouble, row(2).toDouble, row(3)))

// Make dense vectors
val loc = uber_rdd_split_long_lat_converted.map(l => Vectors.dense(l._2,l._3))

// Identify the clusters for the data
val clusters = KMeans.train(loc, 5, 10)