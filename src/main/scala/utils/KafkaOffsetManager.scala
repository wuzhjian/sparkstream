package utils


import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

object KafkaOffsetManager {
  val logger = LoggerFactory.getLogger(KafkaOffsetManager.getClass)
  val ZK_ADDRESS = PropertiesUtil.getPropString("zk_address")

  def readOffsets(zkClient: ZkClient, zkOffsetPath: String, topic: String): Option[Map[TopicAndPartition,Long]] ={
    val(offsetsRangesStrop,_) = ZkUtils.readDataMaybeNull(zkClient, zkOffsetPath)
    offsetsRangesStrop match {
      case Some(offsetsRangesStr) =>
        // 这个topic在zk连最新的分区数量
        val lastest_partitions = ZkUtils.getPartitionsForTopics(zkClient, Seq(topic)).get(topic).get

        var offsets = offsetsRangesStr.split(",") // 按逗号split成数组
          .map(s => s.split(":"))  // 按冒号拆分每隔分区和偏移量
          .map{case Array(partitionStr, offsetStr) =>(TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong)} // 加工成最终的格式
          .toMap

        // 说明有分区扩展了
        if (offsets.size < lastest_partitions.size){
          // 得到旧的所有分区序号
          val old_partitions = offsets.keys.map(p => p.partition).toArray
          // 通过差集得出来多的分区数组
          val add_partitions = lastest_partitions.diff(old_partitions)
          if (add_partitions.size>0){
            logger.warn("发现kafka新增区: " + add_partitions.mkString(","))
            add_partitions.foreach(partitionId =>{
              offsets += (TopicAndPartition(topic, partitionId)->0)
              logger.warn("新增分区id: " +partitionId + "添加完毕")
            })
          }
        } else {
          logger.warn("没有发现新增的kafka分区: " + lastest_partitions.mkString(","))
        }
        Some(offsets)  // 将Map返回
      case None =>
        None  // 如果是null，就返回None
    }
  }

  def saveOffsets(zkClient: ZkClient, zkOffsetPath: String, rdd: RDD[_])={
    // 转换RDD为Array[OffsetRange]
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    // 转换每个OffsetsRange为存储到zk时的字符串格式:  分区序号1:偏移量1,分区序号2:偏移量2,,......
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}").mkString(",")
    logger.debug("保存偏移量: " + offsetsRangesStr)
    // 将最终的字符串结果保存到zk里面
    ZkUtils.updatePersistentPath(zkClient, zkOffsetPath, offsetsRangesStr)
  }


}
