
val INPUT_TRAIN_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/station_data/NYC_Transit_Subway_Entrance_And_Exit_Data.csv"
val OUTPUT_DIR_FILE_PATH = "hdfs:///user/tra290/BDAD/finalProject/station_data/processed"


val stationRDD = sc.textFile(INPUT_TRAIN_DIR_FILE_PATH)

val stationRDDHeader = stationRDD.first()

val stationRDD1 = stationRDD.filter(line => line != stationRDDHeader)

val stationRDD2 = stationRDD1.map { line =>
      val splitLine = line.split(",")
      Array((splitLine(3) + "=" + splitLine(4))) ++ splitLine.slice(5, 16)
    }

// val stationRDDsdfs = stationRDD1.map { line =>
//       val splitLine = line.split(",")
//       splitLine.slice(5, 16)
//     }


val stationRDD3 = stationRDD2.keyBy(line => line(0))

val stationRDD4 = stationRDD3.groupByKey()

val stationToTrainLine = stationRDD4.mapValues { value =>

    value.flatten.toSet.drop(1).filter(_.nonEmpty)

}

val stationToTrainLineStringed = stationToTrainLine.mapValues { value =>

    value.mkString("=")

}

stationToTrainLineStringed.saveAsTextFile(OUTPUT_DIR_FILE_PATH)




// stationRDD1.take(1)(0).split(",").slice(5, 16)