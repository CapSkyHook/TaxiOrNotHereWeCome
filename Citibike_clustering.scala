import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

val path = "hdfs:///user/tra290/BDAD/Citibike_data/*"
val rdd = sc.textFile(path)

val header = rdd.map(_.trim.replace("\"","")).first()
val rdd1 = rdd.map(_.trim.replace("\"","")).filter(line => line != header)

val h = rdd1.filter(l => l.split(",")(0)== "Trip Duration")
val HEADER = h.first()
val rdd2 = rdd1.filter(l => l != HEADER)
val rdd3 = rdd2.filter(l => l.split(",").length == 15)

//clustering
val split = rdd3.map(line => line.split(",")).map(line => (line(0).toInt, line(1), line(2), line(3).toInt, line(4), line(5).toDouble, line(6).toDouble, line(7).toInt, line(8), line(9).toDouble, line(10).toDouble, line(11).toInt, line(12), line(13), line(14).toInt))

val loc = split.map(l => Vectors.dense(l._6,l._7))

val clusters = KMeans.train(loc, 5, 10)



