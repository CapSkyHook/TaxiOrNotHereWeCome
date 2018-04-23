// // In the command line
// //spark-shell --packages com.databricks:spark-csv_2.10:1.5.0 

// // Boundaries of NYC
// // https://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/nynta_metadata.pdf?r=17c
// val MIN_LONGITUDE = -74.257159
// val MAX_LONGITUDE =  -73.699215
// val MAX_LATITUDE =  40.915568
// val MIN_LATITUDE =  40.495992

// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._ 
// import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
// import org.apache.spark.mllib.linalg.Vectors
// import java.io.File

// val path = "hdfs:///user/tra290/BDAD/finalProject/yellow-taxi-data/*.csv"
// val y_t_rdd = sc.textFile(path)

// val header = y_t_rdd.first()
// val y_t_rdd1 = y_t_rdd.filter { line => 
//     if (line.toLowerCase != header.toLowerCase) {
//         line.toLowerCase != header.toLowerCase
//     } else {
//         println(line)
//         line.toLowerCase != header.toLowerCase
//     }
// }

// val y_t_rdd_split = y_t_rdd1.map(line => line.split(",")).filter(line => line.size == columns.size && line(7).forall(_.isDigit)).map(line => (line(0), line(1), line(2), line(3), line(4), ( if (line(5) != "") line(5).toDouble else 0.0), ( if (line(6) != "") line(6).toDouble else 0.0), line(7), line(8), ( if (line(9) != "") line(9).toDouble else 0.0), ( if (line(10) != "") line(10).toDouble else 0.0), line(11), line(12), line(13), line(14), line(15), line(16), line(17)))


// // Extract Latitude then Longitude
// val start_locs = y_t_rdd_split.map(l => (l(6), l(5))
// val end_locs = y_t_rdd_split.map(l => (l(10), l(9)))

// val loc = start_locs.union(end_locs)

// val path = "hdfs:///user/tra290/BDAD/finalProject/green-taxi-data/*.csv"
// val g_t_rdd = sc.textFile(path)

// val header = g_t_rdd.first()
// val g_t_rdd1 = g_t_rdd.filter(line => line != header)

// val columns = header.split(",").toSeq

// val g_t_rdd_split = g_t_rdd1.map(line => line.split(",")).filter(line => line.size == columns.size).map(line => (line(0), line(1), line(2), line(3), line(4), line(5).toDouble, line(6).toDouble, line(7).toDouble, line(8).toDouble, line(9), line(10), line(11), line(12), line(13), line(14), line(15), line(16), line(17), line(18), line(19)))

// // Extract pick up and drop off

// val start_locs = g_t_rdd1.map(l => (l(6), l(5)))
// val end_locs = g_t_rdd1.map(l => (l(10), l(9)))

// val loc_g = start_locs.union(end_locs).
// val loc_y_g = loc.union(loc_g)

// val tmp_loc_in_NYC = loc.filter(x => (x(0) < MAX_LATITUDE && x(0) > MIN_LATITUDE) && (x(1) < MAX_LONGITUDE && x(1) > MIN_LONGITUDE)).cache()

// // Load file into RDD
// val path = "hdfs:///user/tra290/BDAD/finalProject/uber-data/*.csv"
// val uber_rdd = sc.textFile(path)


// // Since we loaded as a RDD, remove any rows that correspond to the header
// val header = uber_rdd.first()
// val uber_rdd_wo_header = uber_rdd.filter(line => line != header)

// // Split the header row into a sequence to make it easier when converting to data frame
// val uber_columns = header.split(",").toSeq

// // Convert the longitude and latitude to doubles
// val uber_rdd_split_long_lat_converted = uber_rdd_wo_header.map(row => row.split(",")).map(row => (row(0), row(1).toDouble, row(2).toDouble, row(3)))

// // Extract pick ups
// val loc_u = uber_rdd_split_long_lat_converted.map(l => (l(1),l(2)))

// val loc_y_g_u = loc_y_g.union(loc_u)

// val path = "hdfs:///user/tra290/BDAD/Citibike_data/*"
// val rdd = sc.textFile(path)

// val header = rdd.map(_.trim.replace("\"","")).first()
// val rdd1 = rdd.map(_.trim.replace("\"","")).filter(line => line != header)

// val h = rdd1.filter(l => l.split(",")(0)== "Trip Duration")
// val HEADER = h.first()
// val rdd2 = rdd1.filter(l => l != HEADER)
// val rdd3 = rdd2.filter(l => l.split(",").length == 15)

// //clustering
// val split = rdd3.map(line => line.split(",")).map(line => (line(0).toInt, line(1), line(2), line(3).toInt, line(4), line(5).toDouble, line(6).toDouble, line(7).toInt, line(8), line(9).toDouble, line(10).toDouble, line(11).toInt, line(12), line(13), line(14).toInt))

// val start_loc = split.map(l => (l(5),l(6)))
// val end_loc = split.map(l => (l(9),l(10)))
// val loc_c = start_loc.union(end_loc)
// val final_loc = loc_y_g_u.union(loc_c)

// val loc_in_NYC = final_loc.filter(x => (x(0) < MAX_LATITUDE && x(0) > MIN_LATITUDE) && (x(1) < MAX_LONGITUDE && x(1) > MIN_LONGITUDE))
// val title = "lat,long".split(",").toSeq()
// val df = loc_in_NYC.toDf(title:_*)
// df.save("hdfs:///user/tra290/BDAD/finalProject/to_clustering")
//
