package runtime
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
// import src.main.scala.DistanceCalculator.{Location, DistanceCalculator}

// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._ 
// import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
// import org.apache.spark.mllib.linalg.Vectors


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
  val PRESERVED_START_END_PTS_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/start_end_data/"
  val INPUT_TRAIN_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/station_data/NYC_Transit_Subway_Entrance_And_Exit_Data.csv"
  val TRAIN_OUTPUT_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/station_data/processed"
  val CLUSTER_DENOTATIONS_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/clusterDenotations"

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
    val citibike_end_locs = citibike_split.map(l => Vectors.dense(l._10,l._11)).cache()


    val loc = y_t_start_locs.union(y_t_end_locs).union(g_t_start_locs).union(g_t_end_locs).union(uber_locs).union(citibike_start_locs).union(citibike_end_locs).cache()

    val all_loc_in_NYC = loc.filter(x => (x(0) < MAX_LATITUDE && x(0) > MIN_LATITUDE) && (x(1) < MAX_LONGITUDE && x(1) > MIN_LONGITUDE))

    all_loc_in_NYC.saveAsTextFile(OUTPUT_DIR_FILE_PATH)
  }

  def preprocessDataMaintainStartStop(sc: org.apache.spark.SparkContext): Unit = {
    val y_t_rdd = sc.textFile(YELLOW_TAXI_DATA_PATH)
    val g_t_rdd = sc.textFile(GREEN_TAXI_DATA_PATH)

    val y_t_header = y_t_rdd.first()
    val g_t_header = g_t_rdd.first()

    val y_t_rdd1 = y_t_rdd.filter { line =>
      if (line.toLowerCase != y_t_header.toLowerCase) {
        line.toLowerCase != y_t_header.toLowerCase
      } else {
        println(line)
        line.toLowerCase != y_t_header.toLowerCase
      }
    }
    val g_t_rdd1 = g_t_rdd.filter(line => line != g_t_header)

    val y_t_columns = y_t_header.split(",").toSeq
    val g_t_columns = g_t_header.split(",").toSeq


    val y_t_rdd_split = y_t_rdd1.map(line => line.split(",")).filter(line => line.size == y_t_columns.size && line(7).forall(_.isDigit)).map(line => (line(0), line(1), line(2), line(3), line(4), ( if (line(5) != "") line(5).toDouble else 0.0), ( if (line(6) != "") line(6).toDouble else 0.0), line(7), line(8), ( if (line(9) != "") line(9).toDouble else 0.0), ( if (line(10) != "") line(10).toDouble else 0.0), line(11), line(12), line(13), line(14), line(15), line(16), line(17)))

    val g_t_rdd_split = g_t_rdd1.map(line => line.split(",")).filter(line => line.size == g_t_columns.size).map(line => (line(0), line(1), line(2), line(3), line(4), line(5).toDouble, line(6).toDouble, line(7).toDouble, line(8).toDouble, line(9), line(10), line(11), line(12), line(13), line(14), line(15), line(16), line(17), line(18), line(19)))

    val y_t_loc = y_t_rdd_split.map(l => Array(Array(l._7, l._6), Array(l._11, l._10)))
    
    val g_t_loc = g_t_rdd_split.map(l => Array(Array(l._7, l._6), Array(l._9, l._8)))

    val loc = y_t_loc.union(g_t_loc)

    val all_loc_in_NYC = loc.filter(line => ((line(0)(0) < MAX_LATITUDE && line(0)(0) > MIN_LATITUDE) && (line(0)(1) < MAX_LONGITUDE && line(0)(1) > MIN_LONGITUDE)) && ((line(1)(0) < MAX_LATITUDE && line(1)(0) > MIN_LATITUDE) && (line(1)(1) < MAX_LONGITUDE && line(1)(1) > MIN_LONGITUDE)))
  

    val all_loc_in_NYC_csved = all_loc_in_NYC.map(locs => locs.flatten.mkString(","))
    
    all_loc_in_NYC_csved.saveAsTextFile(PRESERVED_START_END_PTS_DIR_FILE_PATH)

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
    val clusters = KMeans.train(dense_loc_in_nyc, 25, 10)
    val centroids = clusters.clusterCenters
    sc.parallelize(centroids).saveAsTextFile(CLUSTER_DIR_FILE_PATH)
  }

  def findCorrespondingTrainStation(sc: org.apache.spark.SparkContext): Unit = {
    val startStopData = sc.textFile(PRESERVED_START_END_PTS_DIR_FILE_PATH)
    val subwayStationsData = sc.textFile(TRAIN_OUTPUT_DIR_FILE_PATH)
    val clustersData = sc.textFile(CLUSTER_DIR_FILE_PATH)

    val clusters = clustersData.map(line => line.drop(1).dropRight(1).split(",")).map(value => Location(value(0).toDouble, value(1).toDouble))

    var trainToCoord: scala.collection.mutable.Map[String, Array[Location]] = scala.collection.mutable.Map()
    var coordToTrain: scala.collection.mutable.Map[Location, Array[String]] = scala.collection.mutable.Map()

    subwayStationsData.foreach { line =>
      val data = line.drop(1).dropRight(1).split(",")

      val coords = data(0).split("=").map(_.toDouble)
      val location = Location(coords(0), coords(1))
      val trains = data(1).split("=")

      for (train <- trains) {
          if (trainToCoord.contains(train)) {
              trainToCoord(train) = trainToCoord(train) ++ Array(location)
          } else {
            trainToCoord += (train -> Array(location))
          }
      }

      if (coordToTrain.contains(location)) {
          coordToTrain(location) = coordToTrain(location) ++ trains
      } else {
        coordToTrain += (location -> trains)
      }

    }

    if (trainToCoord.isEmpty || coordToTrain.isEmpty) {
        print("HOLY SHITTTT HOUSTIN WE HAVE  FCUKINNNG PROBLEMMMMMM-------------------------------")
    }
    // val distCalc = new DistanceCalculatorImpl().calculateDistanceInKilometer(Location(10, 20), Location(40, 20))

    val startStopByLoc = startStopData.map{ line => 
      val lineSplit = line.split(",")

      (Location(lineSplit(0).toDouble, lineSplit(1).toDouble), Location(lineSplit(2).toDouble, lineSplit(3).toDouble))
    }

    var clusterToTrainLine: scala.collection.mutable.Map[Location, String] = scala.collection.mutable.Map()

    for (cluster <- clusters) {
      var closestCoord: Location = Location(0.0, 0.0)
      val distCalc = new DistanceCalculatorImpl()
      var closestDist: Double = distCalc.calculateDistanceInKilometer(cluster, closestCoord)
      for (coord <- coordToTrain.keys) {
          if (distCalc.calculateDistanceInKilometer(coord, cluster) < closestDist) {
            closestCoord = coord
            closestDist = distCalc.calculateDistanceInKilometer(coord, cluster)
          }
      }
      print("--------------CLOSEST COORD: " + closestCoord.lat.toString + " " + closestCoord.lon.toString)
      val trainLines = coordToTrain.getOrElse(closestCoord, Array())

      val closeStartPoints = startStopByLoc.filter(locs => new DistanceCalculatorImpl().calculateDistanceInKilometer(locs._1, cluster) < 0.5).take(50)
      val closeEndPoints = startStopByLoc.filter(locs => new DistanceCalculatorImpl().calculateDistanceInKilometer(locs._2, cluster) < 0.5).take(50)

      var pointToTrainLine: scala.collection.mutable.Map[(Location, Location), String] = scala.collection.mutable.Map()
      var pointShortestDistance: scala.collection.mutable.Map[(Location, Location), Double] = scala.collection.mutable.Map()

      for (trainLine <- trainLines) {
        for (coord <- trainToCoord(trainLine)) {
            for (point <- closeStartPoints) {
                val distance = new DistanceCalculatorImpl().calculateDistanceInKilometer(point._2, coord)
                if (distance < pointShortestDistance.getOrElse(point, 9999.9)) {
                    pointToTrainLine += (point -> trainLine)
                    pointShortestDistance += (point -> distance)
                }
            }
            
            for (point <- closeEndPoints) {
                val distance = new DistanceCalculatorImpl().calculateDistanceInKilometer(point._1, coord)
                if (distance < pointShortestDistance.getOrElse(point, 9999.9)) {
                    pointToTrainLine += (point -> trainLine)
                    pointShortestDistance += (point -> distance)
                }
            }
        }
      }

      var lineUseCount: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map()
      var mostUsedTrainLine = "FOOOOOOO"
      for (point <- pointToTrainLine.keys) {
            val currTestedTrainLine = pointToTrainLine(point)
            lineUseCount(currTestedTrainLine) = lineUseCount.getOrElse(currTestedTrainLine, 0) + 1

            if (lineUseCount.getOrElse(currTestedTrainLine, 0) > lineUseCount.getOrElse(mostUsedTrainLine, 0)) {
                mostUsedTrainLine = currTestedTrainLine
            }
      }

      clusterToTrainLine(cluster) = mostUsedTrainLine

    }
    println("------------------------------------Denotation Made!------------------------------------")
    sc.parallelize(clusterToTrainLine.toSeq).saveAsTextFile(CLUSTER_DENOTATIONS_FILE_PATH)

  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sparkContext = spark.sparkContext
    import spark.implicits._

    if (!(this.testDirExist(OUTPUT_DIR_FILE_PATH, spark) && (sparkContext.wholeTextFiles(OUTPUT_DIR_FILE_PATH).count > 0)))  {
        this.preprocessDataAndSaveToDataframe(sparkContext)
    } else {
      println("------------------------------------Data Already Created!------------------------------------")
    }
    if (!(this.testDirExist(CLUSTER_DIR_FILE_PATH, spark) && (sparkContext.wholeTextFiles(CLUSTER_DIR_FILE_PATH).count > 0)))  {
        this.performKMeansClustering(sparkContext)
    } else {
      println("------------------------------------Cluster Already Performed!------------------------------------")
    }
    if (!(this.testDirExist(PRESERVED_START_END_PTS_DIR_FILE_PATH, spark) && (sparkContext.wholeTextFiles(PRESERVED_START_END_PTS_DIR_FILE_PATH).count > 0)))  {
        this.preprocessDataMaintainStartStop(sparkContext)
    } else {
      println("------------------------------------Start Stop Data Already Created!------------------------------------")
    }

    if (!(this.testDirExist(CLUSTER_DENOTATIONS_FILE_PATH, spark) && (sparkContext.wholeTextFiles(CLUSTER_DENOTATIONS_FILE_PATH).count > 0)))  {
        this.findCorrespondingTrainStation(sparkContext)
    } else {
      println("------------------------------------Clusters Already Found Train Lines!------------------------------------")
    }

    spark.stop()
  }
}
