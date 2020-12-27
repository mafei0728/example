/*
 * @Author mafei0728
 * @Description  $end$
 * @Date $time$ $date$
 * @Param $param$
 * @return $return$
 */

package com.mafei.func


import java.util.Properties

import com.mafei.utils.{ConfigUtils, SparkSessionFactory}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, functions => f}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

final case class Exm01(id: String, arr: Array[Int])

final case class Exm02(id: String, arr: Map[String, Int])

final case class Exm03(id: String, book: String)

class FuncTest {
  lazy val spark = SparkSessionFactory.getSpark

  @Test
  def fun01: Unit = {
    import spark.implicits._
    val ds = spark.createDataFrame(Seq(Exm01("a", Array(1, 2, 3)), Exm01("b", Array(1, 2, 3)))).as[Exm01]
    val ds03 = spark.createDataFrame(Seq(Exm03("a", "1 2 3 4"), Exm03("b", "1 2 3 4 5"))).as[Exm03]
    ds.show()
    ds.select(f.col("id"), f.explode(f.col("arr")).alias("tt")).show()
    ds03.withColumn("gg", f.explode(f.split(f.col("book"), " "))).show()
  }

  @Test
  def fun02: Unit = {
    import spark.implicits._
    val ds = spark.createDataFrame(Seq(Exm02("a", Map("t" -> 1, "g" -> 2)),
      Exm02("b", Map("t" -> 2, "j" -> 4)))).as[Exm02]
    ds.show()
    ds.select(f.explode_outer(f.col("arr")).as(Seq("g", "t"))).show()
    ds.select(f.col("id"), f.explode_outer(f.col("arr")).as(Seq("g", "t"))).show()
  }

  @Test
  def fun03: Unit = {
    import java.util
    val arr = new util.ArrayList[Row]()
    arr.add(Row("""{"a":1,"b":32}"""))
    arr.add(Row("""{"a":33,"b":332}"""))
    var df = spark.createDataFrame(arr, StructType.fromDDL("t String"))
    df.show()
    df = df.select(f.from_json(f.col("t"), StructType.fromDDL("a string, b int"))
      .alias("k"))
    df.printSchema()
    df.withColumn("g", f.col("k").getField("a")).show()
  }

  /*
   * @Description:
   * 移动平均线计算算法
   * 5行为一个点, 2个点为窗口,不足一个点下次计算,不足一个窗口下次计算
   *
   * @Author: mafei0728
   * @Date: 2020/12/26 23:07
   * @return: void
   **/
  @Test
  def fun04: Unit = {
    val ips = ClassLoader.getSystemClassLoader.getResourceAsStream("mysql.jdbc.properties")
    val properties: Properties = new Properties()
    properties.load(ips)
    val conf = ConfigUtils.getSourceConfig(Some("mysql.jdbc.properties"))
    var df = spark.read.jdbc(conf.getString("url"), "v1_test", properties)
    val w = Window.partitionBy("id").orderBy("test_date")
    df = df.withColumn("num", f.row_number().over(w))
    df = df.withColumn("modulus", (((f.col("num") - 1) / 5) + 1).cast(IntegerType))
    df.orderBy("id", "test_date").show()
    // 提前合并
    df = df.groupBy("id", "modulus").agg(
      f.max("test_date").alias("max_date"),
      f.min("test_date").alias("min_date"),
      f.collect_list("test_value").alias("value_arr")
    )
    df.show()
    // 去掉长度小于5的,不足一个点,留到下一次计算,
    // 第一次过滤
    df = df.filter(f.size(f.col("value_arr")) === 5)
    df.show()
    // 根据步长继续合并
    val t = f.udf((a: Array[Int], b: Array[Int]) => a)
    val w02 = Window.partitionBy("id")
      .orderBy("modulus")
      .rowsBetween(0, 1)
    df = df.withColumn("value_arr_step", f.collect_list(f.col("value_arr")).over(w02))
    df.orderBy("id", "max_date").show(truncate = false)
    // 窗口为2 不满足2的留到下次计算
    df = df.filter(f.size(f.col("value_arr_step")) === 2)
    df.show(truncate = false)
    // 压平 后面就可以计算指标了,这里就是压平这个方法2.1不支持
    df = df.withColumn("value_arr_step", f.flatten(f.col("value_arr_step")))
    df.show(truncate = false)
  }

  @Test
  def fun05: Unit = {
    val ips = ClassLoader.getSystemClassLoader.getResourceAsStream("mysql.jdbc.properties")
    val properties: Properties = new Properties()
    properties.load(ips)
    val conf = ConfigUtils.getSourceConfig(Some("mysql.jdbc.properties"))
    var df = spark.read.jdbc(conf.getString("url"), "v1_test", properties)
    df = df.withColumn("flag", f.when(f.col("test_value") < 4, 1).otherwise(0))
    df.printSchema()
    import java.sql.Timestamp
    implicit val ord = new Ordering[Timestamp] {
      override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
    }
    df.orderBy("id", "test_date").show(200)
    import scala.collection.mutable
    df.repartition(f.col("id")) // id代表维度,分区,来分布式并行处理,
      .foreachPartition { f => {
        var fg = 0 // 变化标志位1.预警到不预警, 不预警到预警
        var ct = 0 // 计数
        val res: mutable.Buffer[Row] = ArrayBuffer() // 存值容器
        val t: Stream[Row] = f.toList.sortBy(x => x.getTimestamp(1))(ord).toStream // 避免oom
        for (elem <- t) {
          elem match {
            case x: Row if x.get(3) == 1 => { // 无非预警和不预警
              fg = 1  // 预警的逻辑很简单 计数和存储
              ct += 1
              res += x
            }
            case x: Row if x.get(3) == 0 => {
              fg match {  // 不预警,判断上次是否预警状态
                case x: Int if x == 1 => {
                  if (ct >= 2) {  // 如果上次预警,去判断 预警累加是否满足阀值
                    // 处理预警,调用接口,发消息,或者保到数据库
                    println(res)
                    println(ct)
                  }
                  ct = 0
                  res.clear()  // 处理完毕清空 计数和阀值,这里要注意 离线计算 有效数据应该是到最后一个不预警的数据
                               // 如果最后一次是预警 则不要处理,留到下次接上新数据在处理 避免预警不一致
                }              // 这里造df的时候要进行判断
                case _ => // 上次也是正常,不做处理
              }
            }
          }
        }

      }
      }
  }

  //    df = df.groupBy("id", "flag").agg(
  //      f.sum("flag").alias("报警次数"),
  //      f.max("test_date").alias("最大测试时间"),
  //      f.min("test_date").alias("最小测试时间")
  //    )
  //    df.show()
}