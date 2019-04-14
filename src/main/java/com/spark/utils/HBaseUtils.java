package com.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author 44644
 * Hbase 工具类
 */
public class HBaseUtils {
    HBaseAdmin admin = null;
    Configuration configuration = null;

    private static HBaseUtils instance = null;
    private HBaseUtils(){
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","192.168.1.26");
        configuration.set("hbase.zookeeper.property.clientPort","2181");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static synchronized HBaseUtils getInstance(){
       if (null == instance){
           instance = new HBaseUtils();
       }
       return instance;
    }

    public HTable getHtable(String tableName){
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public void put(String tableName, String rowkey, String cf, String column, String value){
        HTable table = getHtable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String tableName = "category_clickcount";
        String rowkey = "20171122_1";
        String cf = "info";
        String colum = "cagegory_click_count";
        String value = "100";

        HBaseUtils.getInstance().put(tableName, rowkey, cf, colum, value);
    }
}
