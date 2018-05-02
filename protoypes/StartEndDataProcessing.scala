   val YELLOW_TAXI_DATA_PATH = "hdfs:///user/tra290/BDAD/finalProject/yellow-taxi-data/*.csv"
  val GREEN_TAXI_DATA_PATH = "hdfs:///user/tra290/BDAD/finalProject/green-taxi-data/*.csv"
  val UBER_DATA_DATA_PATH = "hdfs:///user/tra290/BDAD/finalProject/uber-data/*.csv"
  val CITIBIKE_DATA_PATH = "hdfs:///user/tra290/BDAD/Citibike_data/*"
  val OUTPUT_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/to_clustering_tmp/"
  val CLUSTER_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/Clusters_tmp"
  val PRESERVED_START_END_PTS_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/start_end_data"
    val MIN_LONGITUDE = -74.257159
  val MAX_LONGITUDE =  -73.699215
  val MAX_LATITUDE =  40.915568
  val MIN_LATITUDE =  40.495992
    val y_t_rdd = sc.parallelize(sc.textFile(YELLOW_TAXI_DATA_PATH).take(100))
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

    val all_loc_in_NYC = loc.filter(line => ((line(0)(0) < MAX_LATITUDE && line(0)(0) > MIN_LATITUDE) && (line(0)(1) < MAX_LONGITUDE && line(0)(1) > MIN_LONGITUDE)) && ((line(1)(0) < MAX_LATITUDE && line(1)(0) > MIN_LATITUDE) && (line(1)(1) < MAX_LONGITUDE && line(1)(1) > MIN_LONGITUDE))).cache()
	

	val all_loc_in_NYC_csved = all_loc_in_NYC.map(locs => locs.flatten.mkString(","))
    
    all_loc_in_NYC_csved.saveAsTextFile(PRESERVED_START_END_PTS_DIR_FILE_PATH)