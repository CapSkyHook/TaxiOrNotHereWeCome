// group all location data and its count and save it in a dir 

// import org.apache.spark.sql.SQLContext
// val sqlCtx = new SQLContext(sc)
// import sqlCtx._ 

// import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
// import org.apache.spark.mllib.linalg.Vectors


// //read the saved df containing lats and longs for the whole dataset
// val df = sqlCtx.read.load("hdfs:///user/tra290/BDAD/finalProject/to_clustering/*")
// val rdd = df.rdd

// val rdd_gr_loc = rdd.map(l => ((l(0).asInstanceOf[Double],l(1).asInstanceOf[Double]),1)).reduceByKey(_ + _)
//rdd_gr_loc.saveAsTextFile("hdfs:///user/tra290/BDAD/finalProject/to_visualization")

