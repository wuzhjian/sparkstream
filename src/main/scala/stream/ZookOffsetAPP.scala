package stream

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.slf4j.LoggerFactory
import utils.PropertiesUtil

/**
  * 保存偏移量到zookeeper
  * 在初始化 kafka stream 的时候，查看 zookeeper 中是否保存有 offset，
  * 有就从该 offset 进行读取，没有就从最新/旧进行读取。在消费 kafka 数据的同时，
  * 将每个 partition 的 offset 保存到 zookeeper 中进行备份，
  *
  * 如果 kafka 上的 offset 已经过期，那么就会报 OffsetOutOfRange 的异常，
  * 因为之前保存在 zk 的 offset 已经 topic 中找不到了。
  * 所以我们需要在 从 zk 找到 offset 的这种情况下增加一个判断条件，
  * 如果 zk 中保存的 offset 小于当前 kafka topic 中最小的 offset，
  * 则设置为 kafka topic 中最小的 offset。假设我们上次保存在 zk 中的 offset 值为 123（某一个 partition），
  * 然后程序停了一周，现在 kafka topic 的最小 offset 变成了 200，那么用前文的代码，
  * 就会得到 OffsetOutOfRange 的异常，因为 123 对应的数据已经找不到了。
  * 下面我们给出，如何获取 <topic, parition> 的最小 offset，这样我们就可以进行对比了
  */
object ZookOffsetAPP {

  val logger = LoggerFactory.getLogger("ZookOffsetAPP")

  def main(args: Array[String]): Unit = {
    // 创建SparkStreaming入口
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("JDBCOffsetApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topic = "topic_name"
    val topics = Set(topic)
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> PropertiesUtil.getPropString("metadata.broker.list"),
      "auto.offset.reset" -> PropertiesUtil.getPropString("auto.offset.reset"),
      "group.id" -> PropertiesUtil.getPropString("group.id")
    )
    val topicDirs = new ZKGroupTopicDirs("test_spark_streaming_group", topic)
    val zkClient = new ZkClient("192.168.1.26:2181")

    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")

    var kafkaStream: InputDStream[(String, String)] = null
    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffset: Map[TopicAndPartition, Long] = Map()
    if (children > 0) { //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
      for (i <- 0 until (children)) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topic, i)
        //-----------------------------------------------
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)))
        val consumerMin = new SimpleConsumer("broker_host", 9092, 1000, 1000, "getMinoffset") // 注意这里的 broker_host，因为这里会导致查询不到，解决方法在下面
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        var nextOffset = partitionOffset.toLong
        if (curOffsets.length > 0 && nextOffset < curOffsets.head){
          // 通过比较从 kafka 上该 partition 的最小 offset 和 zk 上保存的 offset，进行选择
          nextOffset = curOffsets.head
        }
        //-----------------------------------------------
        fromOffset += (tp -> partitionOffset.toLong) // 将不同 partition 对应的 offset 增加到 fromOffsets 中
        logger.info("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
      }

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      // 这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffset, messageHandler)
    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset    }
    }

    var offsetRanges = Array[OffsetRange]()
    kafkaStream.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // 得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(msg => msg._2).foreachRDD{ rdd =>
      for (o <- offsetRanges){
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString) //将该 partition 的 offset 保存到 zookeeper
        logger.info(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
      }

      rdd.foreachPartition(
        message =>{
          while (message.hasNext){
            logger.info(s"@^_^@   [" + message.next() + "] @^_^@")
          }
        }
      )
    }
  }

  def getLeaders()={
    val topic_name = "topic_name"
    val topic2 = List(topic_name)
    val req = new TopicMetadataRequest(topic2,0)
    val getLeaderConsumer = new SimpleConsumer("broker_host", 9092, 10000, 1000, "offsetLookup")
    val res = getLeaderConsumer.send(req)
    val topicMetaOption = res.topicsMetadata.headOption
    val partitions = topicMetaOption match {
      case Some(tm) =>
        tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String] // 将结果转化为 partition -> leader 的映射关系
      case None =>
        Map[Int, String]()
    }

  }
}