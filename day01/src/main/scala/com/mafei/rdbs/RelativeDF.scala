package com.mafei.rdbs

import com.mafei.utils.SparkSessionFactory
import org.apache.commons.dbutils.QueryRunner
import org.apache.spark.sql.SparkSession
import org.junit.Test

class RelativeDF {
  lazy val queryRunner = new QueryRunner(GetConnFromDurid.getDs)
  lazy val spark = SparkSessionFactory.getSpark
  lazy val fileJson = ClassLoader.getSystemResource("people.json").getPath

  @Test
  def fun01: Unit ={
    import spark.implicits._
    var ds =spark.read.json(fileJson).as[UserInfo]
    ds.show()
  }
}
