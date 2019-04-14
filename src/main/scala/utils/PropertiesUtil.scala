package utils

import java.util.Properties


/**
  * Properties工具类
  */
object PropertiesUtil {

  /**
    * 获取配置文件Properties对象
    * @return
    */
  def getProperties ():Properties = {
    val properties = new Properties();
    // 读取源码中resource文件夹下的my.properties配置文件,得到一个properties
    val reader = getClass.getResourceAsStream("/application.properties")
    properties.load(reader)
    properties
  }

  /**
    * 获取配置文件中key对应value
    * @param key
    * @return
    */
  def getPropString(key: String): String = {
    getProperties().getProperty(key)
  }

  /**
    * 获取配置文件中的整数值
    * @param key
    * @return
    */
  def getPropInt(key: String): Int = {
    getProperties().getProperty(key).toInt
  }

  /**
    * 获取配置文件中key对应的boolean
    * @return
    */
  def getPropBoolean(key: String): Boolean = {
    getProperties().getProperty(key).toBoolean
  }

}
