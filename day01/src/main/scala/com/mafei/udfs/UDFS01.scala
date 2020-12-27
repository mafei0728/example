/*
 * @Author mafei0728
 * @Description  $end$
 * @Date $time$ $date$
 * @Param $param$
 * @return $return$
 */

package com.mafei.udfs

import java.sql.{Date, Timestamp}

import com.mafei.utils.SparkSessionFactory
import org.apache.spark.sql.{functions => f}
import org.junit.Test

class UDFS01 {

  case class User(id: Int, name: String, create_date: Date, modify_date: Timestamp)

  lazy val spark = SparkSessionFactory.getSpark

  import spark.implicits._

  lazy val df = spark.createDataFrame(List(User(1, "q", Date.valueOf("2020-11-23"),
    Timestamp.valueOf("2020-12-12 11:22:33"))))

  def showSql(sql: String) = {
    spark.sql(sql).show()
  }

  @Test
  def fun01: Unit = {
    showSql("select date_format(date '1970-09-01', 'MM')")
  }

  @Test
  def udf01 = {
    // 最简单的udf,通过api调用
    val s = f.udf((x: String) => x.toUpperCase())
    df.show()
    df.withColumn("name", s($"name")).show()
    // 注册成sql可以用的
    spark.udf.register("s", s)
    df.createTempView("user")
    spark.sql("select name ,s(name) from user").show()
  }
}
