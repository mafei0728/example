/*
 * @Author mafei0728
 * @Description  $end$
 * @Date $time$ $date$
 * @Param $param$
 * @return $return$
 */

package com.mafei.bulit

import com.mafei.utils.SparkSessionFactory
import org.apache.spark.sql.functions.lit
import org.junit.Test
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => f, ColumnName}

class BuiltInFun {
  lazy val spark = SparkSessionFactory.getSpark

  @Test
  def fun01(): Unit ={
    case class Arr(t1:Map[String,Int], t2:Array[Int])
    val res01 = StructType(StructField("map", MapType(StringType, IntegerType))::Nil)
    val res02 = StructType(StructField("arr", ArrayType(IntegerType))::Nil)
    val map01 = Map("a"->12, "b"->13)
    val arr01 = Array(1,2,3,4)
    val arr = List((map01, arr01))
    var df = spark.createDataFrame(arr)
    df.show()
    df.select(df("_1").getField("a")).show()
    df.select(df("_2").getItem(1)).show()

    df.select(df("_1").getItem("a")).show()
    // df.select(df("_2").getField(1)).show()
  }

  @Test
  def fun02: Unit ={
    val dd:ColumnName = new ColumnName("te")
    val ff = dd.timestamp
    println(ff.dataType)
  }

}
