package com.mafei.access

import java.sql.DriverManager

import com.mafei.utils.SparkSessionFactory
import org.apache.commons.dbutils.handlers.BeanListHandler
import org.apache.commons.dbutils.{DbUtils, QueryRunner}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.junit.Test

class ReadTest {
  val ACCESS_DRIVER: String = "net.ucanaccess.jdbc.UcanaccessDriver"
  val URL: String = """jdbc:ucanaccess://C:\Users\Administrator\Documents\Database2.accdb"""
  lazy val spark: SparkSession = SparkSessionFactory.getSpark

  @Test
  def jdbcTest(): Unit = {
    // 初始化
    val queryRunner: QueryRunner = new QueryRunner()
    // 加载驱动
    DbUtils.loadDriver(ACCESS_DRIVER)
    // 创建连接
    val conn = DriverManager.getConnection(URL)
    // 创建对象
    val bm = new BeanListHandler[UserInfo](classOf[UserInfo])
    val res: java.util.List[UserInfo] = queryRunner.query(conn, "select * from test", bm)
    import scala.collection.JavaConverters._
    val users = res.asScala
    users.foreach(println)
  }

  @Test
  def testSpark(): Unit = {
    // 测试spark连通hive的可用性
    spark.catalog.listDatabases().show()
  }

  @Test
  def ReadMddToDF(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val dialect:JdbcDialect = JdbcDialects.get(url = URL)
    JdbcDialects.unregisterDialect(dialect)
    // 加载自定义的单利
    JdbcDialects.registerDialect(AccessJdbcDialects)

    var df = spark.read.format("jdbc")
      .option("driver", ACCESS_DRIVER)
      .option("url", URL)
      .option("dbtable", "test")
      .load()
    df = df.cache()
    df.show()
    spark.catalog.setCurrentDatabase("test_db")
    df.write.mode(SaveMode.Overwrite).saveAsTable("test")
  }


}
