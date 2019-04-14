package stream.dao

import com.spark.utils.HBaseUtilsTTO
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import stream.model.CategarySearchClickCount

import scala.collection.mutable.ListBuffer

object CategarySearchClickCountDAO {

  val tableName = "categary_search_cout"
  val cf = "info"
  val qulifer = "click_count"

  def save(list: ListBuffer[CategarySearchClickCount])= {
    for (els <- list){
      HBaseUtilsTTO.incrementColumnValue(tableName,
        els.day_search_categary,
        cf,
        qulifer,
        els.clickCount)
    }
  }

  def count(dat_categary: String): Long={
    val table = 0
    val result = HBaseUtilsTTO.getRow(tableName, dat_categary)
    val value = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qulifer))
    if (value == null){
      0L
    } else {
      Bytes.toLong(value)
    }
  }



}
