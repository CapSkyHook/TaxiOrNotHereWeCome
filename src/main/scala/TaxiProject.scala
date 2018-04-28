
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._ 
// import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
// import org.apache.spark.mllib.linalg.Vectors

// object TaxiProject {
//   def main(args: Array[String]) {
//     val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
//     val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
//     val logData = spark.read.textFile(logFile).cache()
//     val numAs = logData.filter(line => line.contains("a")).count()
//     val numBs = logData.filter(line => line.contains("b")).count()
//     println(s"Lines with a: $numAs, Lines with b: $numBs")
//     spark.stop()
//   }
// }

object TaxiProject {

  val MIN_LONGITUDE = -74.257159
  val MAX_LONGITUDE =  -73.699215
  val MAX_LATITUDE =  40.915568
  val MIN_LATITUDE =  40.495992
  val YELLOW_TAXI_DATA_PATH = "hdfs:///user/tra290/BDAD/finalProject/yellow-taxi-data/*.csv"
  val GREEN_TAXI_DATA_PATH = "hdfs:///user/tra290/BDAD/finalProject/green-taxi-data/*.csv"
  val UBER_DATA_DATA_PATH = "hdfs:///user/tra290/BDAD/finalProject/uber-data/*.csv"
  val CITIBIKE_DATA_PATH = "hdfs:///user/tra290/BDAD/Citibike_data/*"
  val OUTPUT_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/to_clustering_tmp/"
  val CLUSTER_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/Clusters_tmp"

  def preprocessDataAndSaveToDataframe(sc: org.apache.spark.SparkContext): Unit = {
    val y_t_rdd = sc.textFile(YELLOW_TAXI_DATA_PATH)
    val g_t_rdd = sc.textFile(GREEN_TAXI_DATA_PATH)
    val uber_rdd = sc.textFile(UBER_DATA_DATA_PATH)
    val citibike_rdd = sc.textFile(CITIBIKE_DATA_PATH)

    val y_t_header = y_t_rdd.first()
    val g_t_header = g_t_rdd.first()
    val uber_header = uber_rdd.first()
    val citibike_header = citibike_rdd.map(_.trim.replace("\"","")).first()

    val y_t_rdd1 = y_t_rdd.filter { line =>
      if (line.toLowerCase != y_t_header.toLowerCase) {
        line.toLowerCase != y_t_header.toLowerCase
      } else {
        println(line)
        line.toLowerCase != y_t_header.toLowerCase
      }
    }
    val g_t_rdd1 = g_t_rdd.filter(line => line != g_t_header)
    val uber_rdd1 = uber_rdd.filter(line => line != uber_header)
    
    val citibike_rdd1 = citibike_rdd.map(_.trim.replace("\"","")).filter(line => line != citibike_header)
    val citibike_header2 = citibike_rdd1.filter(l => l.split(",")(0)== "Trip Duration")
    val HEADER = citibike_header2.first()
    val citibike_rdd2 = citibike_rdd1.filter(l => l != HEADER)
    val citibike_rdd3 = citibike_rdd2.filter(l => l.split(",").length == 15)
    val citibike_split = citibike_rdd3.map(line => line.split(",")).map(line => (line(0).toInt, line(1), line(2), line(3).toInt, line(4), line(5).toDouble, line(6).toDouble, line(7).toInt, line(8), line(9).toDouble, line(10).toDouble, line(11).toInt, line(12), line(13), line(14).toInt))

    

    val y_t_columns = y_t_header.split(",").toSeq
    val g_t_columns = g_t_header.split(",").toSeq
    val uber_columns = uber_header.split(",").toSeq


    val y_t_rdd_split = y_t_rdd1.map(line => line.split(",")).filter(line => line.size == y_t_columns.size && line(7).forall(_.isDigit)).map(line => (line(0), line(1), line(2), line(3), line(4), ( if (line(5) != "") line(5).toDouble else 0.0), ( if (line(6) != "") line(6).toDouble else 0.0), line(7), line(8), ( if (line(9) != "") line(9).toDouble else 0.0), ( if (line(10) != "") line(10).toDouble else 0.0), line(11), line(12), line(13), line(14), line(15), line(16), line(17)))

    val g_t_rdd_split = g_t_rdd1.map(line => line.split(",")).filter(line => line.size == g_t_columns.size).map(line => (line(0), line(1), line(2), line(3), line(4), line(5).toDouble, line(6).toDouble, line(7).toDouble, line(8).toDouble, line(9), line(10), line(11), line(12), line(13), line(14), line(15), line(16), line(17), line(18), line(19)))

    val uber_rdd_split_long_lat_converted = uber_rdd1.map(row => row.split(",")).map(row => (row(0), row(1).toDouble, row(2).toDouble, row(3)))


    // Make dense vectors. Order is Latitude then Longitude
    val y_t_start_locs = y_t_rdd_split.map(l => Vectors.dense(l._7, l._6)).cache()
    val y_t_end_locs = y_t_rdd_split.map(l => Vectors.dense(l._11, l._10)).cache()
    val g_t_start_locs = g_t_rdd_split.map(l => Vectors.dense(l._7, l._6)).cache()
    val g_t_end_locs = g_t_rdd_split.map(l => Vectors.dense(l._9, l._8)).cache()
    val uber_locs = uber_rdd_split_long_lat_converted.map(l => Vectors.dense(l._2,l._3)).cache()
    val citibike_start_locs = citibike_split.map(l => Vectors.dense(l._6,l._7)).cache()
    val citibike_end_locs = citibike_split.map(l => Vectors.dense(l._10),l._11)).cache()


    val loc = y_t_start_locs.union(y_t_end_locs).union(g_t_start_locs).union(g_t_end_locs).union(uber_locs).union(citibike_start_locs).union(citibike_end_locs).cache()

    val all_loc_in_NYC = loc.filter(x => (x(0) < MAX_LATITUDE && x(0) > MIN_LATITUDE) && (x(1) < MAX_LONGITUDE && x(1) > MIN_LONGITUDE)).cache()

    all_loc_in_NYC.saveAsTextFile(OUTPUT_DIR_FILE_PATH)
  }

  def loadDataPerformAnalysis(sc: org.apache.spark.SparkContext): Unit = {

  }

  def testDirExist(path: String, spark: org.apache.spark.sql.SparkSession): Boolean = {
    val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val p = new Path(path)
    hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
  }

  def performKMeansClustering(sc: org.apache.spark.SparkContext): Unit = {
    val loc_in_NYC = sc.textFile(OUTPUT_DIR_FILE_PATH).cache()
    val dense_loc_in_nyc = loc_in_NYC.map { line => 
      val splitLine = line.drop(1).dropRight(1).split(",")
      Vectors.dense(splitLine(0).toDouble, splitLine(1).toDouble)
    }.cache()
    val clusters = KMeans.train(dense_loc_in_nyc, 25, 25)
    val centroids = clusters.clusterCenters
    sc.parallelize(centroids).saveAsTextFile(CLUSTER_DIR_FILE_PATH)
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sparkContext = spark.sparkContext

    if (!(this.testDirExist(OUTPUT_DIR_FILE_PATH, spark) && (sparkContext.wholeTextFiles(OUTPUT_DIR_FILE_PATH).count > 0)))  {
        this.preprocessDataAndSaveToDataframe(sparkContext)
    } else {
      println("Data Already Created!")
    }
    if (!(this.testDirExist(CLUSTER_DIR_FILE_PATH, spark) && (sparkContext.wholeTextFiles(CLUSTER_DIR_FILE_PATH).count > 0)))  {
        this.performKMeansClustering(sparkContext)
    } else {
      println("Cluster Already Performed!")
    }

    this.loadDataPerformAnalysis(sparkContext)
    spark.stop()
  }
}
