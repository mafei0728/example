package com.mafei.rdd

import com.mafei.utils.SparkSessionFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.Test

import scala.collection.mutable.ListBuffer
import scala.util.Random

class GetRddPartitionNum {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc: SparkContext = SparkSessionFactory.getSpark.sparkContext

  @Test
  def fun01(): Unit = {
    val l = List((1, "ma"), (2, "te"))
    val rdd: RDD[(Int, String)] = sc.parallelize(l, 12)
    println(rdd.partitions.toList)
    rdd.repartition(12)
    rdd.partitionBy(new HashPartitioner(3))
    rdd.map(x => (x._2, x._1)).reduceByKey(_ + _).foreach(print)
  }

  @Test
  def fun02: Unit = {
    val a: String = "sdsdasdadasdasdasdasd";
    val b = ListBuffer[String]()
    for (i <- 0 to 20) {
      b += a(Random.nextInt(a.length - 1)).toString
    }
    val rdd = sc.parallelize(b, 2).map((_, 1))
    println(rdd.collect().toList)
    println(rdd.aggregateByKey(0)(_ + _, _ + _).collect().toList)
  }

}