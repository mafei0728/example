package com.mafei.utils

import java.nio.file.Paths
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtils {

  /*
   * @Description:记载resource的配置文件
   * @Author: mafei0728
   * @Date: 2020/12/5 23:41
   * @param file:
   * @return: com.typesafe.config.Config
   **/
  def getSourceConfig(file: Option[String]): Config = {
    file match {
      case None => ConfigFactory.load()
      case Some(file) => ConfigFactory.load(file)
    }
  }

  /*
   * @Description:加载任意配置的文件
   * @Author: mafei0728
   * @Date: 2020/12/5 23:47
   * @param file:
   * @return: com.typesafe.config.Config
   **/
  def getConf(file: String): Config = {
    ConfigFactory.parseFile(Paths.get(file).toFile)
  }

  /*
   * @Description:java.utils.Properties和 conf的转化
   * @Author: mafei0728
   * @Date: 2020/12/6 11:31
   * @param config:
   * @return: java.util.Properties
   **/
  implicit def propsFromConfig(config: Config): Properties = {
    import scala.collection.JavaConversions._
    val props = new Properties()
    val map: Map[String, Object] = config.entrySet().map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)
    props.putAll(map)
    props
  }

  /*
   * @Description: 获取全局配置信息
   * @Author: mafei0728
   * @Date: 2020/12/6 12:04
   * @param filename:
   * @return: com.typesafe.config.Config
   **/
  def getConfFromRoot(filename: String): Config = {
    // 项目路径
    val file: String = Paths.get(System.getProperty("user.dir"), "conf", filename)
      .toFile.getAbsolutePath

    getConf(file)
  }

}
