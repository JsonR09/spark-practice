package probelms.customerInsights

import java.sql.Timestamp
import java.time.Month

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.format.DateTimeFormat
import utilities.SparkUtilities


/**
  * Created by Ambarish on 4/30/17.
  * E-commerce data analysis
  */

case class Customer(cId:Int, fName:String, lName:String, phoneNumber:Long)
case class Product(pId:Int, pName:String, pType:String, pVersion:String, pPrice:Double)
case class Refund(rId:Int, tId:Int, cId:Int, pId:Int, timestamp:Timestamp, refundAmount:Double, refundQuantity:Int)
case class Sales(tId:Int, cId:Int, pId:Int, timestamp:Timestamp, totalAmount:Double, totalQuantity:Int)


object CustomerInsightsMain {
  val DELIMITER="\\|"
  val format= new java.text.SimpleDateFormat("MM-dd-YYYY")
  val formatter = DateTimeFormat.forPattern("MM/dd/YYYY HH:mm:ss");
  val PRODUCT_PATH="src/main/resources/data/Product.txt"
  val SALES_PATH="src/main/resources/data/Sales.txt"
  val REFUND_PATH="src/main/resources/data/Refund.txt"
  val CUSTOMER_PATH="src/main/resources/data/Customer.txt"
  val BASE_OUTPUT_DIR="src/main/resources/output/customerInsights/"

  def main(args:Array[String]):Unit={

    /* Initialize */
    val spark = SparkUtilities.getSparkSession(this.getClass.getName)

    /* Q1 */
    val productDF = readProductsDF(spark,PRODUCT_PATH)
    productDF.createOrReplaceTempView("products")

    val salesDF = readSalesDF(spark,SALES_PATH)
    salesDF.createOrReplaceTempView("sales")

    val refundDF = readRefundDF(spark,REFUND_PATH)
    refundDF.createOrReplaceTempView("refunds")

    val customersDF = readCustomerDF(spark,CUSTOMER_PATH)
    customersDF.createOrReplaceTempView("customers")

    /* Q2 */
    getDistribution(spark)

    /* Q3 */
    calcSalesAmountInYear(spark,salesDF,2013)

    /* Q4 */
    calcSecondMostPurchase(spark,salesDF,customersDF,2013,Month.MAY)

    /* Q5 */
    findNotPurchasedProducts(spark,productDF,salesDF)

    /* Q6 */
    val count = countConsecutiveBuyers(spark,salesDF)
    println("Total number of users who purchased the same product consecutively at least 2 times on a given day: "+count)

  }

  def convertCurrencyToDouble(currency:String):Double={
    currency.stripPrefix("$").trim.toDouble
  }

  def getDate(date:String):Timestamp={
      new java.sql.Timestamp(formatter.parseDateTime(date).getMillis)
  }

  def readProductsDF(spark:SparkSession,path:String):DataFrame={
    import spark.implicits._
    spark.read.textFile(path)
      .map( line => line.split(DELIMITER))
      .map(fields => new Product(fields(0).toInt, fields(1), fields(2), fields(3),convertCurrencyToDouble(fields(4))))
      .toDF()
  }

  def readSalesDF(spark:SparkSession,path:String):DataFrame= {
    import spark.implicits._
    spark.read.textFile(SALES_PATH)
      .map(line => line.split(DELIMITER))
      .map(fields => new Sales(fields(0).toInt,fields(1).toInt,fields(2).toInt, getDate(fields(3)), convertCurrencyToDouble(fields(4)),fields(5).toInt))
      .toDF()
  }

  def readRefundDF(spark:SparkSession,path:String)={
    import spark.implicits._
    spark.read.textFile(path)
      .map(line => line.split(DELIMITER))
      .map(fields => new Refund(fields(0).toInt,fields(1).toInt,fields(2).toInt,fields(3).toInt, getDate(fields(4)), convertCurrencyToDouble(fields(5)),fields(6).toInt))
      .toDF()
  }
  def readCustomerDF(spark:SparkSession,path:String)= {
    import spark.implicits._
    spark.read.textFile(path)
      .map( line => line.split(DELIMITER))
      .map(fields => new Customer(fields(0).toInt, fields(1), fields(2), fields(3).toLong))
      .toDF()
  }


  def writeDF(df:DataFrame,path:String):Unit={
    df.repartition(1)
      .write
      .format("csv")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .save(path)

  }

  def getDistribution(spark:SparkSession):Unit={
      import spark.sql
      import org.apache.spark.sql.functions._

      val joinPS = sql("SELECT * FROM products JOIN sales ON products.pId=sales.pId ")
      val groupedPS = joinPS.groupBy("pName","pType")

      val sumGroupedPP = groupedPS.agg(sum("totalAmount").as("TotalPurchase"),sum("totalQuantity").as("TotalQuantity"))
      writeDF(sumGroupedPP,BASE_OUTPUT_DIR+"SumAgg")

      val meanGroupedPP = groupedPS.agg(mean("totalAmount").as("AvgPurchase"),mean("totalQuantity").as("AvgQuantity"))
      writeDF(meanGroupedPP,BASE_OUTPUT_DIR+"MeanAgg")

      val minGroupedPP = groupedPS.agg(min("totalAmount").as("MinPurchase"),min("totalQuantity").as("MinQuantity"))
      writeDF(minGroupedPP,BASE_OUTPUT_DIR+"MinAgg")

      val maxGroupedPP = groupedPS.agg(max("totalAmount").as("MaxPurchase"),max("totalQuantity").as("MaxQuantity"))
      writeDF(maxGroupedPP,BASE_OUTPUT_DIR+"MaxAgg")
  }

  def calcSalesAmountInYear(spark:SparkSession,salesDF:DataFrame,yearF:Int):Unit={
    import spark.sql
    import org.apache.spark.sql.functions._

    val salesInYear = salesDF.filter(sale => sale.getAs[Timestamp]("timestamp").toLocalDateTime.getYear == yearF)
    salesInYear.createOrReplaceTempView("salesInYear")

    val salesInYearUnRefunded = sql("select * from salesInYear where tId NOT IN (select tId from refunds)")
    val salesInYearUnRefundedSum = salesInYearUnRefunded.agg(sum("totalAmount").as("Total Unrefunded Amount"))

    writeDF(salesInYearUnRefundedSum,BASE_OUTPUT_DIR+"UnRefundedSum")
  }

  def calcSecondMostPurchase(spark:SparkSession,salesDF:DataFrame,customersDF:DataFrame,yearF:Int,monthF:Month):Unit={
    import spark.sql
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val salesYearMonthFilter = salesDF.filter(sale =>  sale.getAs[Timestamp]("timestamp").toLocalDateTime.getMonth == monthF && sale.getAs[Timestamp]("timestamp").toLocalDateTime.getYear == yearF)
    salesYearMonthFilter.createOrReplaceTempView("salesYearMonthFilter")

    val purchases = salesYearMonthFilter.groupBy($"cId").agg(sum($"totalAmount").as("totalPurchases")).orderBy($"totalPurchases".desc)
    val cid = purchases.takeAsList(2).get(1).get(0)

    val result = customersDF.filter(customer => customer.getAs[Int]("cId") == cid)
                            .select($"fName",$"lName")

    writeDF(result,BASE_OUTPUT_DIR+"SecondMostPurchase")
  }

  def findNotPurchasedProducts(spark:SparkSession,productDF:DataFrame,salesDF:DataFrame):Unit={
    import spark.sql
    val productNotPurchased = sql("select * from products where pId NOT IN (select pId from sales)")
    writeDF(productNotPurchased,BASE_OUTPUT_DIR+"ProductNotPurchased")
  }

  def countConsecutiveBuyers(spark:SparkSession,salesDF:DataFrame):Long={
    import spark.implicits._
    val salesPair=salesDF.map(sale => ((sale.getAs[Int]("cId"),sale.getAs[Int]("pId")),sale.getAs[Timestamp]("timestamp")))
                        .rdd
                        .groupByKey()
                        .filter(x => x._2.size>1)
                        .mapValues(dates => dates.map(date => format.format(date.getTime)))

    val sameDayCount=salesPair.map {
            x =>
              val cid = x._1._1
              val times = x._2.asInstanceOf[List[String]]
              val lsize = times.size
              val setSize = times.distinct.size
              (cid, lsize-setSize)
        }
        .reduceByKey((x,y)=>x+y)
        .filter(x => x._2>0)
        .count()

    sameDayCount
  }
}
