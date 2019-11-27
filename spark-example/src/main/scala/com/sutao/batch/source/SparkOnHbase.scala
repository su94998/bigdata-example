package com.sutao.batch.source

import com.sutao.model.Student
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
  * Created by sutao on 2019/11/15.
  *
  * spark
  * 读取hbase数据
  * 保存数据到hbse
  */
object SparkOnHbase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkOnHbase")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    // 初始化hbase配置参数
    val config = HBaseConfiguration.create
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.zookeeper.quorum", "hadoop151,hadoop152,hadoop153")
    // 设置表名
    config.set(TableInputFormat.INPUT_TABLE, "student")

    val hbaseTableRDD = spark.sparkContext.newAPIHadoopRDD(config,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hbaseTableRDD.map(item=>{
      (Bytes.toString(item._2.getRow),Bytes.toString(item._2.getValue("info".getBytes, "age".getBytes)))
    }).foreach(println)


    // 初始化保存hbase参数
    config.set(TableOutputFormat.OUTPUT_TABLE, "student")
    val job = Job.getInstance(config)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    // 生成rdd测试数据
    val studentRDD = spark.sparkContext.makeRDD(
      Array(
        Student(1002,"zhangsan",18),
        Student(1003,"lisi",20),
        Student(1004,"wangwu",18),
        Student(1005,"zs",22)
      )
    )

    // 封装保存hbase rdd数据结构
    val rdd = studentRDD.map(item=>{
      val put = new Put(Bytes.toBytes(item.id.toString))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(item.name))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(item.age.toString))
      (new ImmutableBytesWritable,put)
    })

    // 保存到hbase
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)

    spark.close()

  }

}
