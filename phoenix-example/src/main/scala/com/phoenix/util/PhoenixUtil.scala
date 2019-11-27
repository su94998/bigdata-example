package com.phoenix.util

import java.sql._

/**
  * Created by sutao on 2019/7/8.
  */
object PhoenixUtil {

  try {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
  }catch {
    case e: ClassNotFoundException   => println(e.printStackTrace)
  }


  def getConnection(): Connection ={
    var conn: Connection = null
    try{
      conn = DriverManager.getConnection("jdbc:phoenix:hadoop151:2181")
    }catch {
      case e: SQLException  => println(e.printStackTrace)
    }
    conn
  }


  def close(rs:ResultSet,state: Statement, conn: Connection): Unit ={
    if (rs != null) rs.close()
    if (state != null) state.close()
    if (conn != null) conn.close()
  }








}
