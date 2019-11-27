package com.sutao.util

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

/**
  * Created by sutao on 2019/10/16.
  */
object ZkUtil {

  // 初始化Zookeeper客户端
  val zkClient = {
    val client = CuratorFrameworkFactory.builder.connectString("hadoop151:2181")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      // .namespace("mykafka")
      .build()

    client.start()
    client
  }

  def getZkPartition(topic:String): Int ={

    val partitions =  zkClient.getChildren.forPath("/brokers/topics/"+topic+"/partitions").size()
    zkClient.close()
    partitions
  }


  def main(args: Array[String]): Unit = {

    println(getZkPartition("test1_log"))

  }




}
