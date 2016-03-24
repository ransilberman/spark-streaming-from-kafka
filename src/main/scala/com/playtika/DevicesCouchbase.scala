package com.playtika

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.spark._
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import com.couchbase.spark.sql._

/**
  * Created by rans on 23/03/16.
  */
object DevicesCouchbase {
  def main(args: Array[String]): Unit = {
    val cfg = new SparkConf()
      .setAppName("DevicesCouchbase") // give your app a name
      .setMaster("local[*]") // set the master to local for easy experimenting
      .set("com.couchbase.bucket.devices", "") // open the devices bucket

    // Generate The Context
    val sc = new SparkContext(cfg)
    val sqlContext = new SQLContext(sc)
    // read file
    val logFile = "appsflyer.txt" // Should be some file on your system
    val logData = sc.textFile(logFile, 2).cache()
    val df = sqlContext.read.json(logData)
    df.registerTempTable("devices")
    df.write.couchbase(Map("idField" -> "advertising_id"))

  }
}