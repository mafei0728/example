/*
 * @Author mafei0728
 * @Description  $end$
 * @Date $time$ $date$
 * @Param $param$
 * @return $return$
 */

package com.mafei.mysqltohive

import java.util.Properties

import com.mafei.utils.{ConfigUtils, SparkSessionFactory}
import org.apache.log4j.{Level, Logger}
import org.junit.Test

class Demo01 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSessionFactory.getSpark

  @Test
  def fun01: Unit = {
    // 隐式转换把conf转化为properties并不被这个方法识别
    import ConfigUtils.propsFromConfig
    val conf = ConfigUtils.getSourceConfig(Some("mysql.jdbc.properties"))
    val ips = ClassLoader.getSystemClassLoader.getResourceAsStream("mysql.jdbc.properties")
    val properties: Properties = new Properties()
    properties.load(ips)
    val df = spark.read.jdbc(conf.getString("url"), "nh_illegal_occ_flight_rank", properties)
    df.write.saveAsTable("temp.rank_info")

  }

  @Test
  def fun02:Unit={
    val conf = ConfigUtils.getSourceConfig(Some("mysql.jdbc.properties"))
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", conf.getString("url"))
      .option("dbtable", "nh_illegal_occ_flight_rank")
      .option("user", "root")
      .option("password", "mafei0728")
      .load()
    jdbcDF.show()
  }
}
