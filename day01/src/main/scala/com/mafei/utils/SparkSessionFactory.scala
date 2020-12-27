package com.mafei.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  val spark: Option[SparkSession] = None

  def getSpark: SparkSession = spark match {
    case None => {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().setMaster("local[*]").setAppName("test")
      SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    }
    case Some(x) => x
  }

  def main(args: Array[String]): Unit = {

  }
}
