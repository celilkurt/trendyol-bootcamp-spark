package com.trendyol.bootcamp.homework

import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

object ProductMergerJob {

  def main(args: Array[String]): Unit = {

    /**
    * Find the latest version of each product in every run, and save it as snapshot.
    *
    * Product data stored under the data/homework folder.
    * Read data/homework/initial_data.json for the first run.
    * Read data/homework/cdc_data.json for the nex runs.
    *
    * Save results as json, parquet or etc.
    *
    * Note: You can use SQL, dataframe or dataset APIs, but type safe implementation is recommended.
    */

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Homework")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val productSchema = Encoders.product[Product].schema
    val products = spark.read
      .schema(productSchema)
      .json("data/homework/initial_data.json")
      .as[Product]

    val changedProducts = spark.read
      .schema(productSchema)
      .json("data/homework/cdc_data.json")
      .as[Product]

    val allProducts = products.union(changedProducts)

    def getLastCreatedRecord(products:Iterator[Product]) =
      products.foldLeft(products.next())(
        (curProduct, product) => if (product.timestamp > curProduct.timestamp) product else curProduct)

   val curRecords = allProducts.groupByKey(_.id)
     .mapGroups((_, productGroup) => getLastCreatedRecord(productGroup))




    curRecords
      .repartition(1)
      .write
      .partitionBy("brand")
      .mode(SaveMode.Overwrite)
      .json("output/homework/products")

  }

}

case class Product(id: Int, name: String, category: String, brand: String, color: String, price: Double, timestamp: Long)
