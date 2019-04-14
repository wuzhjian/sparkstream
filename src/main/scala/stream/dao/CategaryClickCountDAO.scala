package stream.dao

import com.spark.utils.{HBaseUtils, HBaseUtilsTTO}
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import stream.model.CategaryClickCount

import scala.collection.mutable.ListBuffer

object CategaryClickCountDAO {

  val tableName = "categary_search_cout"
  val cf = "info"
  val qualifier = "click_count"

  def save(list: ListBuffer[CategaryClickCount]) = {
    val table = HBaseUtilsTTO.getTable(tableName)
    for (els <- list){
      table.incrementColumnValue(
        Bytes.toBytes(els.categaryID),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifier),
        els.clickCount
      )
    }
  }

  def count(day_category: String): Long={
    val table = HBaseUtilsTTO.getTable(tableName)
    val get = new Get(Bytes.toBytes(day_category))
    val value = table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifier))
    if (value == null){
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CategaryClickCount]
    list.append(CategaryClickCount("20171122_1",300))
    list.append(CategaryClickCount("20171122_9", 60))
    list.append(CategaryClickCount("20171122_10", 160))
    save(list)
  }
}
