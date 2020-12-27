package com.mafei.hbase.demo

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

object SparkOnHBase {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local").setAppName("readHbase")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val sc = spark.sparkContext
    //--创建Hbase的环境变量参数
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "hadoop03,hadoop04,hadoop05")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "spark_hbase")

    val hbaseContext = new HBaseContext(sc, hbaseConf)

    val scan = new Scan()

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = hbaseContext.hbaseRDD(TableName.valueOf("user"), scan)

    val hbaseRow = hbaseRDD.map(eachResult => {

      val rowkey = Bytes.toString(eachResult._1.get())
      val result: Result = eachResult._2
      //--查询出来的结果集存在 (ImmutableBytesWritable, Result)第二个元素
      //--获取行键
      val rowKey = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
      Row(rowKey, name)
    })
    val ddl: String = "rowkey string, name string"
    val schema = StructType.fromDDL(ddl)
    println(schema)
    val df = spark.createDataFrame(hbaseRow, schema)
    df.groupBy("name").count.show
    df.show()
    spark.stop()
  }


}