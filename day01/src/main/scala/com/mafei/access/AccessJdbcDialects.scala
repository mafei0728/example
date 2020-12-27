package com.mafei.access

import java.sql.Types

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StringType}

object AccessJdbcDialects extends JdbcDialect {
  // 加载这个驱动时候
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:ucanaccess")

  //重写sql type 到 df type的映射,我们这里简单点可以全部转化为字符串
  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
      Some(StringType)
  }

  // 标识符
  override def quoteIdentifier(colName: String): String = colName
}
