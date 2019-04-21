package stream.app


import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}
import org.spark_project.jetty.server.{Request, Server}
import org.spark_project.jetty.server.handler.{AbstractHandler, ContextHandler}
import utils.{KafkaOffsetManager, PropertiesUtil}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.tools.nsc.interpreter.InputStream

object CustomDirectKafkaExample {
  val logger = LoggerFactory.getLogger(CustomDirectKafkaExample.getClass)




  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("CustomDirectKafkaExample")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topics = PropertiesUtil.getPropString("kafka.topics").split(",").toSet
    val kafkaParams = Map[String, String] (
      "metadata.broker.list" -> PropertiesUtil.getPropString("metadata.broker.list"),
      "auto.offset.reset" -> PropertiesUtil.getPropString("auto.offset.reset"),
      "group.id" -> PropertiesUtil.getPropString("group.id"),
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )

    val zkClient = new ZkClient(PropertiesUtil.getPropString("zkclients"), 3000, 3000)
    val zkOffsetPath = "/sparkstreaming/20171128"
    val message:InputDStream[ConsumerRecord[String, String]] = creaetDirectStream(ssc, kafkaParams, zkClient, zkOffsetPath, topics)

    message.foreachRDD(rdd =>{
      if (!rdd.isEmpty()){
        rdd.foreachPartition(partition=>{
          //如果没有使用广播变量，连接资源就在这个地方初始化
          //比如数据库连接，hbase，elasticsearch，solr，等等

          partition.foreach(msg=>{
            logger.info("读取的数据: " + msg)
            // process(msg)  处理每条数据
          })
        })
        // 更新每隔批次的偏移量到zk中，这段代码允许在driver上
        KafkaOffsetManager.saveOffsets(zkClient, zkOffsetPath, rdd)
      }
    })
    ssc.start()

    // 启动接收停止请求的守护进程
    daemonHttpServer(5555, ssc)
    

  }


  def creaetDirectStream(ssc: StreamingContext,
                         kafkaParams: Map[String, String],
                         zkClient: ZkClient,
                         zkOffsetPath: String,
                         topics: Set[String]): InputDStream[ConsumerRecord[String, String]] = {
    // 目前仅支持一个topic的偏移量处理，读取zk里面偏移量字符串
    val zkOffsetDate = KafkaOffsetManager.readOffsets(zkClient, zkOffsetPath, topics.last)
    val kafkaStream = zkOffsetDate match {
      case None =>
        logger.info("系统第一次启动，没有读取到偏移量，默认从最新的offset开始消费")
        KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
      case Some(lastStopOffset) =>
        logger.info("从zk中读取到偏移量，从上次的偏移量开始消费数据........")
        val topicPartition = new TopicPartition(topics.last, lastStopOffset.last._1.partition)
        val ss = Map[TopicPartition, Long](
          topicPartition -> lastStopOffset.last._2
        )

        // val messageHandler = (mmd: MessageAndMetadata[String, String]) =>(mmd.key, mmd.message())
        //使用上次停止时候的偏移量创建DirectStream

        KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, ss))
    }
    kafkaStream
  }

  /**
    * 判断是否存在mark file
    * @param hdfs_file_path
    * @return
    */
  def isExistsMarkFile(hdfs_file_path: String): Boolean = {
    val conf = new Configuration()
    val path = new Path(hdfs_file_path)
    val fs = path.getFileSystem(conf)
    fs.exists(path)
  }

  def stopByMarkFile(ssc: StreamingContext)={
    val intervalMills = 10 * 1000 //每隔10秒扫描一次消息是否存在
    var isStop = false
    val hdfs_file_path = "/spark/streaming/stop"
    while (!isStop){
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if (!isStop && isExistsMarkFile(hdfs_file_path)){
        logger.warn("2秒后关闭sparkStreaming程序......")
        Thread.sleep(2000)
        ssc.stop(true, true)
      }
    }
  }

  /**
    * 负责启动守护的jetty服务
    * @param port
    * @param ssc
    */
  def daemonHttpServer(port: Int, ssc: StreamingContext): Unit = {
    val server = new Server(port)
    val context = new ContextHandler()
    context.setContextPath("/close")
    context.setHandler(new CloseStreamHandler(ssc))
    server.setHandler(context)
    server.start()
  }

  class CloseStreamHandler(ssc: StreamingContext) extends AbstractHandler{
    override def handle(s: String, request: Request,
                        httpServletRequest: HttpServletRequest,
                        httpServletResponse: HttpServletResponse): Unit = {
      logger.warn("开始关闭.....")
      ssc.stop(true, true)
      httpServletResponse.setContentType("text/html;charset=utf-8")
      httpServletResponse.setStatus(HttpServletResponse.SC_OK)
      val out = httpServletResponse.getWriter
      println("close success")
      request.setHandled(true)
      logger.info("关闭成功 .......")
    }
  }

}
