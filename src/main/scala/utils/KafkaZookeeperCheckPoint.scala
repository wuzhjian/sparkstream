package utils

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConversions._
import scala.collection.mutable


object KafkaZookeeperCheckPoint {

  val client = {
    val client = CuratorFrameworkFactory
      .builder()
      .connectString("localhost:2181")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .namespace("mykafka")
      .build()
    client.start()
    client
  }

  val kafka_offset_path = "/kafka/offsets"

  // 路径确认，确认zk中路径存在，不存在则创建改路径
  def ensureZKPathExists(path: String) ={
    if (client.checkExists().forPath(path) == null){
      client.create().creatingParentsIfNeeded().forPath(path)
    }
  }

  // 保存新的offset
  def storeOffsets(offsetRange: Array[OffsetRange], groupName: String) = {
    for (o <- offsetRange){
      val zkPath = s"${kafka_offset_path}/${groupName}/${o.topic}/${o.partition}"

      // 向对应分区第一次写入或者更新offset信息
      println("----offset写入-------\nTopic: " + o.topic +", Partition: " + o.partition + ", Offset: " +o.untilOffset)
      client.setData().forPath(zkPath, o.untilOffset.toString.getBytes())
    }
  }


  def getOffset(topic: Array[String], groupName: String):(Map[TopicPartition, Long], Int) = {
    // Kafka 0.8和0.10的版本差别，0.10 为 TopicPartition   0.8 TopicAndPartition
    val fromOffset:Map[TopicPartition, Long] = Map()
    val topic1 = topic(0).toString

    // 读取zk中保存的offset，作为DStream的起始位置，如果没有则创建该路径，并从0开始DStream
    val zkTopicPath = s"${kafka_offset_path}/${groupName}/${topic1}"

    // 检查改路径是否存在
    ensureZKPathExists(zkTopicPath)

    val childrens = client.getChildren.forPath(zkTopicPath)
    // 遍历分区
    val offsets:mutable.Buffer[(TopicPartition, Long)] = for {
      p <- childrens
    }
      yield {
        // 读取子节点的数据
        val offsetData = client.getData().forPath(s"${zkTopicPath}/$p")
        // offset转为long
        val offset = new String(offsetData).toLong
        (new TopicPartition(topic1, Integer.parseInt(p)), offset)
      }
    println(offsets.toMap)
    if (offsets.isEmpty){
      (offsets.toMap, 0)
    } else {
      (offsets.toMap, 1)
    }
  }

  def createMyZookeeperDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, Object],
                                         topic: Array[String], groupName: String):InputDStream[ConsumerRecord[String, String]] = {
    // get offset  flag = 1  表示基于已有的offset计算  flag = 表示从头开始(最早或者最新，根据Kafka配置)
    val (fromOffsets, flag) = getOffset(topic, groupName)
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    if (flag == 1){
      // 加上消息头
      // val messageHandler = (mmd:MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      println(fromOffsets)
     /* kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))*/
      kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topic, kafkaParams, fromOffsets))

      println(fromOffsets)
      println("中断后 Streaming 成功！")
    } else {
      kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topic, kafkaParams))
    }
    kafkaStream
  }

}
