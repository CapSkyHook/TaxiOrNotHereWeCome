

import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import sqlCtx._ 

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors


//read the saved df containing lats and longs for the whole dataset
val df = sqlCtx.read.load("hdfs:///user/tra290/BDAD/finalProject/to_clustering/*")
val rdd = df.rdd

//Make dense vectors
val rdd_vec = rdd.map(l => Vectors.dense(l(0).asInstanceOf[Double],l(1).asInstanceOf[Double]))

val clusters = KMeans.train(rdd_vec,5,10)