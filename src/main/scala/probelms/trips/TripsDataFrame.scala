package probelms.trips

import utilities.SparkUtilities

/**
  * solve it with help of data frames
  * Created by ahazarnis on 4/23/17.
  */

case class Fields(cid:Int,date:Long)

object TripsDataFrame {

  val spark = SparkUtilities.getSparkSession(this.getClass.getName)
  val format = new java.text.SimpleDateFormat("MM/dd/yyyy")

  import spark.implicits._

  val df = spark.read.textFile("TRIPS_MOCK_DATA.txt")
                      .map(line => line.split("\t"))
                      .map {
                        fields =>
                             Fields(fields(0).toInt, format.parse(fields(4)).getTime)
                           }.toDF()

  df.createOrReplaceTempView("tripsDF")

  val tripsDF = spark.sql("select * from tripsDF order by date")

  val groupByCid = tripsDF.rdd.groupBy(row => row(0))

  val cidDatePairs = groupByCid.map{
                                row=>
                                  val cid = row._1
                                  val dateList = row._2.map(pairs => pairs(1))
                                  (cid,dateList)
                                }

  val result = cidDatePairs.map{
                            row =>
                              val cid = row._1
                              var count=1
                              var d1:Long=row._2.toList(0).asInstanceOf[Long]
                              for(ele <- row._2.toList){
                                if(ele.asInstanceOf[Long] - d1 > 7*24*60*60*1000) {
                                  d1 = ele.asInstanceOf[Long]
                                  count = count + 1
                                }
                              }
                              (cid,count)
  }

  result.collect().foreach(line=> println(line))

}
