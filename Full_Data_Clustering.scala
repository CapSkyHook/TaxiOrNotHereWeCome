// In the command line
//spark-shell --packages com.databricks:spark-csv_2.10:1.5.0 

// Boundaries of NYC
// https://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/nynta_metadata.pdf?r=17c
val MIN_LONGITUDE = -74.257159
val MAX_LONGITUDE =  -73.699215
val MAX_LATITUDE =  40.915568
val MIN_LATITUDE =  40.495992

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._ 
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

val path = "hdfs:///user/tra290/BDAD/finalProject/yellow-taxi-data/*.csv"
val y_t_rdd = sc.textFile(path)

val header = y_t_rdd.first()
val y_t_rdd1 = y_t_rdd.filter { line => 
    if (line.toLowerCase != header.toLowerCase) {
        line.toLowerCase != header.toLowerCase
    } else {
        println(line)
        line.toLowerCase != header.toLowerCase
    }
}
val columns = header.split(",").toSeq

val y_t_rdd_split = y_t_rdd1.map(line => line.split(",")).filter(line => line.size == columns.size && line(7).forall(_.isDigit)).map(line => (line(0), line(1), line(2), line(3), line(4), ( if (line(5) != "") line(5).toDouble else 0.0), ( if (line(6) != "") line(6).toDouble else 0.0), line(7), line(8), ( if (line(9) != "") line(9).toDouble else 0.0), ( if (line(10) != "") line(10).toDouble else 0.0), line(11), line(12), line(13), line(14), line(15), line(16), line(17)))


// Make dense vectors. Order is Latitude then Longitude
val start_locs = y_t_rdd_split.map(l => Vectors.dense(l._7, l._6)).cache()
val end_locs = y_t_rdd_split.map(l => Vectors.dense(l._11, l._10)).cache()

val loc = start_locs.union(end_locs).cache()

val loc_in_NYC = loc.filter(x => (x(0) < MAX_LATITUDE && x(0) > MIN_LATITUDE) && (x(1) < MAX_LONGITUDE && x(1) > MIN_LONGITUDE))

val path = "hdfs:///user/tra290/BDAD/finalProject/green-taxi-data/*.csv"
val g_t_rdd = sc.textFile(path)

val header = g_t_rdd.first()
val g_t_rdd1 = g_t_rdd.filter(line => line != header)

val columns = header.split(",").toSeq

val g_t_rdd_split = g_t_rdd1.map(line => line.split(",")).filter(line => line.size == columns.size).map(line => (line(0), line(1), line(2), line(3), line(4), line(5).toDouble, line(6).toDouble, line(7).toDouble, line(8).toDouble, line(9), line(10), line(11), line(12), line(13), line(14), line(15), line(16), line(17), line(18), line(19)))

// Make dense vectors for pick up and drop off

val start_locs = g_t_rdd1.map(l => Vectors.dense(l._7, l._6)).cache()
val end_locs = g_t_rdd1.map(l => Vectors.dense(l._11, l._10)).cache()

val loc = start_locs.union(end_locs).cache()

val tmp_loc_in_NYC = loc.filter(x => (x(0) < MAX_LATITUDE && x(0) > MIN_LATITUDE) && (x(1) < MAX_LONGITUDE && x(1) > MIN_LONGITUDE)).cache()

val loc_in_NYC = loc_in_NYC.union(tmp_loc_in_NYC).cache()

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
val tmp_loc_in_NYC = loc.filter(x => (x(0) < MAX_LATITUDE && x(0) > MIN_LATITUDE) && (x(1) < MAX_LONGITUDE && x(1) > MIN_LONGITUDE)).cache()
val loc_in_NYC = loc_in_NYC.union(tmp_loc_in_NYC).cache()

// Identify the clusters for the data
val clusters = KMeans.train(loc_in_NYC, 25, 25)