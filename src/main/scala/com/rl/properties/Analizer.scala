package com.rl.properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}



/**
  *
  * @author Richard Li
  */
object Analizer extends DemoApp{
  val vatRate: Double = 0.135
  def main(args:Array[String]):Unit = {
    setup()
    val conf = new SparkConf().setAppName("Hello World").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().config(conf).getOrCreate()
    val infoDF = sqlContext.read.format("com.databricks.spark.csv").option("header","true").load("D:\\datasets\\pdata\\*.csv")
    infoDF.printSchema()
    val schema = new StructType()
      .add(StructField("dateOfSell", DateType, true))
      .add(StructField("address", StringType, true))
      .add(StructField("postalCode", StringType, true))
      .add(StructField("county", StringType, true))
      .add(StructField("price", DoubleType, true))
      .add(StructField("notFullMarketPrice", StringType, true))
      .add(StructField("vatExclusive", StringType, true))
      .add(StructField("description", StringType, true))
      .add(StructField("sizeDescription", StringType, true))
      .add(StructField("location", StringType, true))
    //val dummy = new SimpleDateFormat("dd/MM/yyyy")
    val filtered = infoDF.filter(row => row.getAs[String]("Address").toUpperCase.contains("SHACKLETON") && row.getAs[String]("Address").toUpperCase.contains("LUCAN"))
    val finalRDD = filtered.rdd.map(row => Row(
      {
        //println(s"${Thread.currentThread().getId()}  $dummy")
        val format = new SimpleDateFormat("dd/MM/yyyy")
        new java.sql.Date(format.parse(row.getAs[String]("Date of Sale")).getTime())
      },
      row.getAs[String]("Address").split(",")(0).trim().toLowerCase(),//row.getAs[String]("Address"),
      row.getAs[String]("Postal Code"),
      row.getAs[String]("County"),
      if (row.getAs[String]("VAT Exclusive") != null && row.getAs[String]("VAT Exclusive").equalsIgnoreCase("yes")) {
        (row.getAs[String]("Price").substring(1).replace(",", "").toDouble) * ( 1 + vatRate)
      } else {

        row.getAs[String]("Price").substring(1).replace(",", "").toDouble
      },
      row.getAs[String]("Not Full Market Price"),
      row.getAs[String]("VAT Exclusive"),
      row.getAs[String]("Description of Property"),
      row.getAs[String]("Property Size Description"),
      row.getAs[String]("Address").split(",")(0).replaceAll("[0-9]", "").trim().toLowerCase.replace("no ", "").replace("unit", "").replace("shackelton", "shackleton")
    ))

    val finalDF = sqlContext.createDataFrame(finalRDD, schema)
    finalDF.printSchema()
    finalDF.createOrReplaceTempView("infoDF")

    val priceHiLow = sqlContext.sql("SELECT dateOfSell, address, price, location FROM infoDF order by price DESC")
    priceHiLow.repartition(1).write.format("com.databricks.spark.csv").save("D:\\intellij-workspace\\learning-spark\\src\\test\\resources\\price_hi_low")

    val priceHiLowByRegion = sqlContext.sql("SELECT dateOfSell, location, address, price FROM infoDF order by location, price DESC")
    priceHiLowByRegion.repartition(1).write.format("com.databricks.spark.csv").save("D:\\intellij-workspace\\learning-spark\\src\\test\\resources\\price_hi_low_region")

    val regionTopPrice = sqlContext.sql("SELECT location, max(price) as max_price, min(price) as min_price FROM infoDF group by location")
    regionTopPrice.repartition(1).write.format("com.databricks.spark.csv").save("D:\\intellij-workspace\\learning-spark\\src\\test\\resources\\region_top_price")

    val countPerRegion = sqlContext.sql("SELECT location, count(1) as cnt FROM infoDF group by location")
    countPerRegion.repartition(1).write.format("com.databricks.spark.csv").save("D:\\intellij-workspace\\learning-spark\\src\\test\\resources\\count_per_region")

    val dateOrder = sqlContext.sql("SELECT dateOfSell, location, address, price FROM infoDF order by dateOfSell DESC, price DESC")
    dateOrder.repartition(1).write.format("com.databricks.spark.csv").save("D:\\intellij-workspace\\learning-spark\\src\\test\\resources\\date_order")

    sc.stop()
  }
}
