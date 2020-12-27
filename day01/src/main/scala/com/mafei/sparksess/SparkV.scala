/*
 * @Author mafei0728
 * @Description  $end$
 * @Date $time$ $date$
 * @Param $param$
 * @return $return$
 */

package com.mafei.sparksess

import java.util

import com.mafei.dataset.User
import com.mafei.utils.SparkSessionFactory
import org.apache.avro.io.Encoder
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.catalyst.expressions.Encode
import org.apache.spark.sql.types.StructType
import org.junit.Test

case class UserInfo(name: String, age: Int)

class SparkV {
  val spark = SparkSessionFactory.getSpark

  @Test
  def fun01(): Unit = {
    Encoders.bean(classOf[User])
    // javaBean来创建ds
    // createDataFrame(data: List[_], beanClass: Class[_]): DataFrame
    import scala.collection.JavaConverters._
    val l1 = new util.ArrayList[User]()
    l1.add(new User("a", 1))
    l1.add(new User("b", 4))
    l1.add(new User("c", 2))
    val l2 = List(new User("c", 1)).asJava
    println(l1)
    println(l2)
    // 实例获取类用getClass
    //  类获取用classOf
    spark.createDataFrame(l1, classOf[User]).show()
    spark.createDataFrame(l2, classOf[User]).show()
  }

  @Test
  def fun02(): Unit = {
    // case class 不会有值
    import scala.collection.JavaConverters._
    val l1 = new util.ArrayList[UserInfo]()
    l1.add(UserInfo("a", 1))
    l1.add(UserInfo("b", 4))
    val l2 = List(UserInfo("c", 1)).asJava
    println(l1)
    println(l2)
    spark.createDataFrame(l1, classOf[UserInfo]).show()
    spark.createDataFrame(l2, classOf[UserInfo]).show()
  }

  @Test
  def fun03(): Unit = {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq[User](new User("a", 1), new User("b", 1)))
    spark.createDataFrame(rdd, classOf[User]).show()
  }

  @Test
  def fun04(): Unit = {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq[Row](Row("a", 2), Row("b", 3)))
    spark.createDataFrame(rdd, StructType.fromDDL("a string,b int")).show()
  }

  @Test
  def fun05(): Unit = {
    val s = spark.createDataFrame(Seq(UserInfo("a", 223), UserInfo("b", 232323)))
    s.printSchema()
    s.show()
  }

  @Test
  def fun06: Unit = {
    import spark.implicits._
    val t = spark.emptyDataFrame
    val h = spark.emptyDataset[UserInfo]
    t.show()
    t.printSchema()
    h.show()
    h.printSchema()
  }

}
