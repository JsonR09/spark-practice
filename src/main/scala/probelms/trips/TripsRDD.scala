package probelms.trips

import utilities.SparkUtilities

/**
  * solve using plain old RDD way
  * Created by ahazarnis on 4/23/17.
  */

object TripsRDD {


  val format = new java.text.SimpleDateFormat("MM/dd/yyyy")

  def main(args: Array[String]): Unit = {

    val sc = SparkUtilities.getSparkContext(this.getClass.getName)

    val textFile = sc.textFile("TRIPS_MOCK_DATA.txt")

    val cidDatePairs = textFile.map(line => line.split("\t"))
      .map(fields => (fields(0), fields(4)))

    val groupedPairs = cidDatePairs.groupByKey().sortByKey()

    val sortedDatePairs = groupedPairs.map {
      row =>
        (row._1, row._2.toList.sortBy(date => format.parse(date)))
    }

    val result = sortedDatePairs.map {
      pair =>
        val id = pair._1
        var count = 1
        var d1 = pair._2(0)
        for (ele <- pair._2) {
          if (format.parse(ele).getTime() - format.parse(d1).getTime() > 7 * 24 * 60 * 60 * 1000) {
            d1 = ele
            count = count + 1
          }
        }
        (id, count)
    }

    result.collect().foreach(line => println(line))
  }
}