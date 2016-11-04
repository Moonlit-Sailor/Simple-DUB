package org.distributeddub.sparkdub

import java.io.PrintWriter

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by root on 11/3/16.
  */
object DistributedDUB {
  def main(args: Array[String]): Unit = {
    val filePathAmazon = "file:///root/software/RecordMatching/data/Amazon.csv"
    val filePathGoogle = "file:///root/software/RecordMatching/data/GoogleProducts.csv"
    val fileResultMapping = "file:///root/software/RecordMatching/Amzon_GoogleProducts_perfectMapping.csv"
    val warehousePath = "file:///root/software/RecordMatching/data/"
    val out = new PrintWriter("/root/software/RecordMatching/data/Output.txt")

    val ss = SparkSession.builder().appName("DUB").getOrCreate()
    val dfAmazon = ss.read.option("header","true").csv(filePathAmazon).repartition(12)
    val dfGoogle = ss.read.option("header","true").csv(filePathGoogle).repartition(12)

    val dim = 4 // 4 features

    // Generate record pair
    val dfRecPair = (dfAmazon join dfGoogle).persist(StorageLevel.MEMORY_AND_DISK)
  }

}
