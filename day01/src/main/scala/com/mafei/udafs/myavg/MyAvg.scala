/*
 * @Author mafei0728
 * @Description  $end$
 * @Date $time$ $date$
 * @Param $param$
 * @return $return$
 */

package com.mafei.udafs.myavg

import java.util

import com.google.common.collect.Lists
import com.mafei.utils.SparkSessionFactory
import org.apache.spark.sql.{Row, types, functions => f}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}


object MyAvg extends UserDefinedAggregateFunction {

  // 输入的类型,名字随便写
  override def inputSchema: StructType = types.StructType.fromDDL("t int")
  // 中间值类型,名字随便写 这个看Row那个类的对应关系
  override def bufferSchema: StructType = types.StructType(types.StructField("r",
    types.ArrayType(types.IntegerType)) :: Nil)
  // 返回的数据类型
  override def dataType: DataType = types.IntegerType
  // 同样的输入,是否一直返回同样的输出
  override def deterministic: Boolean = true
 // 初始化中间值
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = Seq[Int]()
 // 分区类聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val muList: java.util.List[Int] = new util.ArrayList(buffer.getList(0))
    muList.add(input.getInt(0))
    buffer(0) = muList
  }
 // 分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val b1 = new util.ArrayList[Int](buffer1.getList(0))
    b1.addAll(buffer2.getList(0))
    buffer1(0) = b1
  }

  override def evaluate(buffer: Row): Int = {
    // 聚合的时候一定要交代好数据类型
    import scala.collection.JavaConverters._
    val l1: util.List[Int] = buffer.getList(0)
    val s1: Seq[Int] = l1.asScala
    s1.reduce(_ + _)
  }
}


object ClassTest02 extends App {
  val spark = SparkSessionFactory.getSpark
  val df = spark.sql("select * from temp.rank_info")
  df.show()
  // 直接api就可以使用
  df.groupBy(f.col("id")).agg(MyAvg(f.col("frequency"))).show()
  // 如果是sql用,就必须注册
  spark.udf.register("s", MyAvg)
}