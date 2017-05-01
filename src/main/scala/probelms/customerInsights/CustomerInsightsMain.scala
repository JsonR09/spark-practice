package probelms.customerInsights

import java.sql.Timestamp
import java.time.Month
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


  def main(args:Array[String]):Unit={

    /* Initialize */
    val spark = SparkUtilities.getSparkSession(this.getClass.getName)

    /* Q1 */
    val productDF = IOUtilities.readProductsDF(spark,CIConstants.PRODUCT_PATH)
    productDF.createOrReplaceTempView("products")

    val salesDF = IOUtilities.readSalesDF(spark,CIConstants.SALES_PATH)
    salesDF.createOrReplaceTempView("sales")

    val refundDF = IOUtilities.readRefundDF(spark,CIConstants.REFUND_PATH)
    refundDF.createOrReplaceTempView("refunds")

    val customersDF = IOUtilities.readCustomerDF(spark,CIConstants.CUSTOMER_PATH)
    customersDF.createOrReplaceTempView("customers")

    /* Q2 */
    CIFunctions.getDistribution(spark)

    /* Q3 */
    CIFunctions.calcSalesAmountInYear(spark,salesDF,2013)

    /* Q4 */
    CIFunctions.calcSecondMostPurchase(spark,salesDF,customersDF,2013,Month.MAY)

    /* Q5 */
    CIFunctions.findNotPurchasedProducts(spark,productDF,salesDF)

    /* Q6 */
    val count = CIFunctions.countConsecutiveBuyers(spark,salesDF)
    println("Total number of users who purchased the same product consecutively at least 2 times on a given day: "+count)

  }


}
