package com.retaildb.usecase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RetailDb extends App{

  val thriftServer = args(1)
  val outputPath = args(0)

  args.foreach(println(_))
  //println("thriftServer: " + thriftServer)
  //println("outputPath: " + outputPath)

  val spark = SparkSession.builder()
    .appName("retail db use case")
    .config("hive.metastore.uris", thriftServer)
    .enableHiveSupport()
    .getOrCreate()

import spark.implicits._
  val categoriesDf = readHive(spark, "dpl.categories_parqt")
  val customersDf = readHive(spark,"dpl.customers_parqt")
  val departmentsDf = readHive(spark, "dpl.departments_parqt")
  val orderItemsDf = readHive(spark, "dpl.order_Items_parqt")
  val ordersDf = readHive(spark, "dpl.orders_parqt")
  val productsDf = readHive(spark, "dpl.products_parqt")

  val resultDf = customersDf.join(ordersDf, customersDf("customer_id") === ordersDf("order_customer_id"))
    .join(orderItemsDf, ordersDf("order_id") === orderItemsDf("order_item_order_id"))
    .join(productsDf, orderItemsDf("order_item_product_id") === productsDf("product_id"))
    .join(categoriesDf, productsDf("product_category_id") === categoriesDf("category_id"))
    .join(departmentsDf, categoriesDf("category_department_id") === departmentsDf("department_id")).drop("order_customer_id","order_item_product_id","product_category_id","category_department_id")
    .withColumn("order_year", year(substring($"order_date", 0, 10)))
    .withColumn("order_month", month(substring($"order_date", 0, 10)))
    .withColumn("order_date_Date", to_date(substring($"order_date", 0, 10))).cache()

  //getCustMonPurchases.write.parquet(s"$outputPath/CustMonthlyPurchases.parquet")
  //getMostPurchProdByCust.write.parquet(s"$outputPath/MostPurchasingProdByCust.parquet")
  //getCustPrevTenDaySales.write.parquet(s"$outputPath/CustPreviousTenDaySales.parquet")

  //getCustMonPurchases.select(concat_ws(";", getCustMonPurchases.columns.map(c => col(c)):_*)).write.mode("overwrite").text(s"$outputPath/CustMonthlyPurchases")
  //getMostPurchProdByCust.select(concat_ws(";", getMostPurchProdByCust.columns.map(c => col(c)):_*)).write.mode("overwrite").text(s"$outputPath/MostPurchasingProdByCust")
  //getCustPrevTenDaySales.select(concat_ws(";", getCustPrevTenDaySales.columns.map(c => col(c)):_*)).write.mode("overwrite").text(s"$outputPath/CustPreviousTenDaySales")

  getCustMonPurchases.write.mode("overwrite").option("header",true).csv(s"$outputPath/CustMonthlyPurchases")
  getMostPurchProdByCust.write.mode("overwrite").option("header",true).csv(s"$outputPath/MostPurchasingProdByCust")
  getCustPrevTenDaySales.write.mode("overwrite").option("header",true).csv(s"$outputPath/CustPreviousTenDaySales")

  def readHive(session: SparkSession, tableName: String) = spark.read.table(tableName)
// First Use Case
  def getCustMonPurchases = {
    //def convertArrayToStringFunc(arrayType: Seq[String]) =  arrayType.mkString(",")

    val groupedDf = resultDf.select($"customer_id", $"order_year", $"order_month", $"product_price".cast("double").as("product_price_double"))
      .groupBy($"customer_id", $"order_year", $"order_month")
      .sum("product_price_double").withColumnRenamed("sum(product_price_double)", "sumofmonthlyprodpercust")

    val departmentName =  resultDf.select("customer_id", "department_name").distinct()
    //val convertArrayToString = udf((arr: Seq[String]) => arr.mkString(","))

    groupedDf.join(departmentName, Seq("customer_id"), "left_outer")
      .withColumn("departmentList", collect_set("department_name").over(Window.partitionBy("customer_id")))
      //.withColumn("departmentListString", convertArrayToString(col("departmentList")))
      .withColumn("departmentListString", concat_ws(",", col("departmentList")))
      .drop("department_name", "departmentList").dropDuplicates(Seq("customer_id", "order_year", "order_month")).orderBy("customer_id")

  }
// Second Use Case
  def getMostPurchProdByCust = {
    val windowSpec = Window.partitionBy("order_year", "order_month").orderBy($"highest_product_sold_in_a_month".desc)
    val rank = row_number().over(windowSpec)


    val groupedDf1 = resultDf.select($"order_year", $"order_month", $"product_name", $"product_price".cast("double").as("product_price_double"))
      .groupBy($"order_year", $"order_month", $"product_name").agg(count("product_name").as("highest_product_sold_in_a_month"))

    //val tempTable = groupedDf1.createOrReplaceGlobalTempView("temp")
    //spark.sql("select * from temp")
    groupedDf1.select($"order_year", $"order_month", $"product_name", $"highest_product_sold_in_a_month", rank as "rank")
      .orderBy($"order_year", $"order_month").where($"rank" === 1).drop($"rank")
  }

  //Third Use Case
  def getCustPrevTenDaySales = {
    val useCaseWindowsSpec = Window.partitionBy("customer_id").orderBy($"order_date_Date".desc)

    resultDf.select($"customer_id", $"customer_fname", $"customer_lname", $"product_name", $"product_price", $"department_name", $"order_date_Date", row_number().over(useCaseWindowsSpec).as("rank"))
      .where($"rank" <= 10).drop("rank")

  }
}

