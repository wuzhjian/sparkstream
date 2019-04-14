package stream

import java.util.{Date, Properties}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import utils.PropertiesUtil

/**
  * 生产者
  */
object KafkaProducer {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("serializer.class", PropertiesUtil.getPropString("serializer.class"))
    properties.put("metadata.broker.list", PropertiesUtil.getPropString("metadata.broker.list"))
    properties.put("request.required.acks",PropertiesUtil.getPropString("request.required.acks"))

    val producerConfig = new ProducerConfig(properties)
    val producer = new Producer[String, String](producerConfig)
    val topic = PropertiesUtil.getPropString("kafka.topics")

    while (true){
      // 每次产生100条数据
      for (i <- 1 to 10000){
        val runtimes = new Date().toString
        val messages = new KeyedMessage[String, String](topic, i + "", "hlw:"+runtimes)
        producer.send(messages)
      }
    }
    println("数据发送完毕...")
  }
}
