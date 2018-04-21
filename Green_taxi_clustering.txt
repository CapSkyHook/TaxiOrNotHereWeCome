import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

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

val loc_in_NYC = loc.filter(x => (x(0) < MAX_LATITUDE && x(0) > MIN_LATITUDE) && (x(1) < MAX_LONGITUDE && x(1) > MIN_LONGITUDE)).cache()

// Identify the clusters for the data
val clusters = KMeans.train(loc_in_NYC, 5, 10)