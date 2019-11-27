package com.sutao.util

import com.sutao.conf.ConfigurationManager
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * Created by sutao on 2019/11/15.
  */
object HbaseUtil {


  def getHBaseTabel(tableName: String) = {
    // 创建HBase配置
    val config = HBaseConfiguration.create
    // 设置HBase参数
    config.set("hbase.zookeeper.property.clientPort", ConfigurationManager.config.getString("hbase.zookeeper.property.clientPort"))
    config.set("hbase.zookeeper.quorum", ConfigurationManager.config.getString("hbase.zookeeper.quorum"))
    // 创建HBase连接
    val connection = ConnectionFactory.createConnection(config)
    // 获取HBaseTable
    val table = connection.getTable(TableName.valueOf(tableName))
    table
  }

}
