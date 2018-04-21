
import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext

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
  val OUTPUT_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/to_clustering/"

  def preprocessDataAndSaveToDataframe(sc: Any): Unit = {
    val y_t_rdd = sc.textFile(YELLOW_TAXI_DATA_PATH)
    val g_t_rdd = sc.textFile(GREEN_TAXI_DATA_PATH)
    val uber_rdd = sc.textFile(UBER_DATA_DATA_PATH)

    val y_t_header = y_t_rdd.first()
    val g_t_header = g_t_rdd.first()
    val uber_header = uber_rdd.first()

    val y_t_rdd1 = y_t_rdd.filter { line =>
      if (line.toLowerCase != header.toLowerCase) {
        line.toLowerCase != header.toLowerCase
      } else {
        println(line)
        line.toLowerCase != header.toLowerCase
      }
    }
    val g_t_rdd1 = g_t_rdd.filter(line => line != header)
    val uber_rdd1 = uber_rdd.filter(line => line != header)

    val y_t_columns = header.split(",").toSeq
    val g_t_columns = header.split(",").toSeq
    val uber_columns = header.split(",").toSeq


    val y_t_rdd_split = y_t_rdd1.map(line => line.split(",")).filter(line => line.size == columns.size && line(7).forall(_.isDigit)).map(line => (line(0), line(1), line(2), line(3), line(4), ( if (line(5) != "") line(5).toDouble else 0.0), ( if (line(6) != "") line(6).toDouble else 0.0), line(7), line(8), ( if (line(9) != "") line(9).toDouble else 0.0), ( if (line(10) != "") line(10).toDouble else 0.0), line(11), line(12), line(13), line(14), line(15), line(16), line(17)))

    val g_t_rdd_split = g_t_rdd1.map(line => line.split(",")).filter(line => line.size == g_t_columns.size).map(line => (line(0), line(1), line(2), line(3), line(4), line(5).toDouble, line(6).toDouble, line(7).toDouble, line(8).toDouble, line(9), line(10), line(11), line(12), line(13), line(14), line(15), line(16), line(17), line(18), line(19)))

    val uber_rdd_split_long_lat_converted = uber_rdd1.map(row => row.split(",")).map(row => (row(0), row(1).toDouble, row(2).toDouble, row(3)))


    // Make dense vectors. Order is Latitude then Longitude
    val y_t_start_locs = y_t_rdd_split.map(l => Vectors.dense(l._7, l._6)).cache()
    val y_t_end_locs = y_t_rdd_split.map(l => Vectors.dense(l._11, l._10)).cache()
    val g_t_start_locs = g_t_rdd1.map(l => Vectors.dense(l._7, l._6)).cache()
    val g_t_end_locs = g_t_rdd1.map(l => Vectors.dense(l._11, l._10)).cache()
    val uber_locs = uber_rdd_split_long_lat_converted.map(l => Vectors.dense(l._2,l._3)).cache()


    val loc = start_locs.union(list(y_t_start_locs, y_t_end_locs, g_t_start_locs, g_t_end_locs, uber_locs)).cache()

    val all_loc_in_NYC = loc.filter(x => (x(0) < MAX_LATITUDE && x(0) > MIN_LATITUDE) && (x(1) < MAX_LONGITUDE && x(1) > MIN_LONGITUDE)).cache()

    all_loc_in_NYC.saveAsTextFile(OUTPUT_DIR_FILE_PATH)
  }

  def loadDataPerformAnalysis(sc: Any): Unit = {

  }

  def testDirExist(path: String): Boolean = {
    val p = new Path(path)
    hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext
    val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val output_dir = new File(OUTPUT_DIR_FILE_PATH)
    if (!(this.testDirExist(output_dir) && (sc.wholeTextFiles(output_dir).count > 0)))  {
        this.preprocessDataAndSaveToDataframe(sc)
    }

    this.loadDataPerformAnalysis(sc)

    spark.stop()
  }
}