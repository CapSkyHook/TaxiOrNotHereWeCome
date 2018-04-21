// In the command line
//spark-shell --packages com.databricks:spark-csv_2.10:1.5.0 

val MIN_LONGITUDE = -74.257159
val MAX_LONGITUDE =  -73.699215
val MAX_LATITUDE =  40.915568
val MIN_LATITUDE =  40.495992


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
// Identify the clusters for the data
val clusters = KMeans.train(loc_in_NYC, 25, 25)