package stream


import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import scalikejdbc._
import scalikejdbc.config._
import utils.PropertiesUtil

import scala.collection.JavaConversions._

object JDBCOffsetApp {
  def main(args: Array[String]): Unit = {
    // 创建SparkStreaming入口
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("JDBCOffsetApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    // kafka参数
    // 这里应用了自定义的ValueUtils工具类，来获取application.conf里的参数，方便后期修改
    val topics = PropertiesUtil.getPropString("kafka.topics").split(",")
      //.toSet
    val kafkaParams = Map[String, Object](
      "metadata.broker.list" -> PropertiesUtil.getPropString("metadata.broker.list"),
      "auto.offset.reset" -> PropertiesUtil.getPropString("auto.offset.reset"),
      "group.id" -> PropertiesUtil.getPropString("group.id")
    )

    //先使用scalikejdbc从MySQL数据库中读取offset信息
    //+------------+------------------+------------+------------+-------------+
    //| topic      | groupid          | partitions | fromoffset | untiloffset |
    //+------------+------------------+------------+------------+-------------+
    //MySQL表结构如上，将“topic”，“partitions”，“untiloffset”列读取出来
    //组成 fromOffsets: Map[TopicAndPartition, Long]，后面createDirectStream用到
    Class.forName("com.mysql.jdbc.Driver")
    DBs.setup()
    val fromOffset = DB.readOnly(implicit session =>{
      SQL("select * from hlw_offset").map(rs => {
        (TopicPartition(rs.string("topic"),rs.int("partitions")),
          rs.long("untiloffset"))
      }).list().apply()
    }).toMap
    // 如果mysql中没有offset信息，就从0开始消费，如果有，就从已经存在的offset开始消费
    val message = if (fromOffset.isEmpty){
      print("从头开始消费........")
      KafkaUtils.createDirectStream(ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams))
    } else {
      println("从已有的记录开始消费....")
      // val messageHandler = (mm:MessageAndMetadata[String, String]) => (mm.key(), mm.message())

      KafkaUtils.createDirectStream(ssc,
        PreferConsistent,
        ConsumerStrategies.Assign[String, String](fromOffset.keys.toList, kafkaParams, fromOffset)
    }
    message.foreachRDD(rdd =>{
      if (!rdd.isEmpty()){
        // 输出RDD的数量
        println("数据统计记录为:  " + rdd.count())
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(x =>{
          // 输出每次消费的主题，分区，开始偏移量和结束偏移量
          println(s"---${x.topic},${x.partition},${x.fromOffset},${x.untilOffset}")
          // 将最新的偏移量信息保存到mysql
          DB.autoCommit(implicit session => {
            SQL("replace into hlw_offset(topic, groupid, partitions, fromoffset, untiloffset) values (?,?,?,?,?)")
              .bind(x.topic, PropertiesUtil.getPropString("group.id"), x.partition, x.fromOffset,x.untilOffset)
              .update().apply()
          })
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
