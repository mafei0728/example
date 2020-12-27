/*
 * @Author mafei0728
 * @Description  $end$
 * @Date $time$ $date$
 * @Param $param$
 * @return $return$
 */

package com.mafei.dataset

import com.mafei.utils.{ConfigUtils, SparkSessionFactory}
import org.junit.Test
import org.apache.spark.TaskContext

case class Info(name: String, age: Long)

class Demo01 {
  lazy val spark = SparkSessionFactory.getSpark
  lazy val fileJson = ClassLoader.getSystemResource("people.json").getPath

  import spark.implicits._

  lazy val df = spark.read.json(fileJson)
  lazy val dt = df.as[Info]


  @Test
  def fun01() = {
    // 按照指定的列来分区
    val ds = dt.repartition($"name")
    ds.foreachPartition(x => {
      x.foreach(t => println(t + TaskContext.getPartitionId().toString))
    })
  }

  @Test
  def fun02(): Unit ={
    dt.select($"age", $"age".as("h")).describe("age", "h").show()
  }

  @Test
  def fun03():Unit={
    println(df.dtypes)
  }

  @Test
  def fun04(): Unit ={
    //
    df.as("ss").show()
  }
}

