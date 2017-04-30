package probelms.trafficSource

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utilities.SparkUtilities

/**
  * Created by ahazarnis on 4/29/17.
  */
case class WebLog(uid: String, url: String, time: Long) extends Ordered[WebLog]{
  override def toString= uid+":"+url+":"+time

  override def compare(that:WebLog):Int={
    this.time.compare(that.time)
  }
}

object TrafficSourceRDD {
  val format = new java.text.SimpleDateFormat("HH:mm:ss")

  def main(args:Array[String]):Unit={
    val sc = SparkUtilities.getSparkContext(this.getClass.getName)
    val path = "src/main/resources/TRAFFIC_SOURCE.txt"
    val targetUrl="http://adobe.com"
    val result = getTrafficSource(sc,path,targetUrl)
    result.foreach(line => println(line))
  }

  def getTrafficSource(sc:SparkContext, path:String, targetUrl:String):RDD[(String,Int)]={

    val input = sc.textFile(path)
    val users = input.map(line => line.split("\t"))
                     .map(fields => (fields(0),WebLog(fields(0),fields(1),format.parse(fields(2)).getTime )))
                     .groupByKey()

    val userSortedByTime = users.mapValues(webLog => webLog.toList.sorted)
    // OR    val userSorted = users.mapValues(webLog => webLog.toList.sortWith(_.time<_.time))

    val partialResult = userSortedByTime.flatMap{
                                case(key,value)=>
                                  var count=0
                                  var prev:WebLog=new WebLog("","",0)
                                  var tfsDistribution = scala.collection.mutable.Map[String,Int]()
                                  for(webLog <- value){
                                    if(targetUrl.equals(webLog.url)){
                                      count=tfsDistribution.getOrElse(prev.url,0)+1
                                      tfsDistribution += ( prev.url ->  count)
                                    }
                                    prev=webLog
                                  }
                                  tfsDistribution.toList
    }

    val result = partialResult.reduceByKey((x,y)=>x+y)
    result
  }

}
