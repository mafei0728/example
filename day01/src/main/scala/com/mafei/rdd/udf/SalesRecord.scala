package com.mafei.rdd.udf

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class SalesRecord(val transactionId: String,
                       val customerId: String,
                       val itemId: String,
                       val itemValue: Double) extends Serializable

object SparkMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val dataRDD = sc.textFile(this.getClass.getClassLoader.getResource("sales.txt").getPath)
    val salesRecordRDD = dataRDD.map(row => {
      val colValues = row.split(",")
      new SalesRecord(colValues(0), colValues(1), colValues(2), colValues(3).toDouble)
    })

    import com.mafei.rdd.udf.CustomFunctions._

    println("Spark RDD API : " + salesRecordRDD.map(_.itemValue).sum) //Spark RDD API : 892.0

    //通过隐式转换的方法，增加rdd的transformation算子
    val moneyRDD: RDD[Double] = salesRecordRDD.changeDatas
    println("customer RDD  API:" + moneyRDD.collect().toBuffer)
    //customer RDD  API:ArrayBuffer(128.0, 135.0, 147.0, 196.0, 178.0, 108.0)

    //给rdd增加action算子
    val totalResult: Double = salesRecordRDD.getTotalValue
    println("total_result" + totalResult) //total_result892.0

    //自定义RDD，将RDD转换成为新的RDD
    val resultCountRDD: CustomerRDD = salesRecordRDD.discount(0.8)

    println(resultCountRDD.collect().toBuffer)

    sc.stop()
  }
}