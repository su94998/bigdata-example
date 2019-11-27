package com.sutao.dao

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.sutao.conf.ConfigurationManager
import com.sutao.constant.Constants
import org.apache.log4j.LogManager

/**
  * Mysql连接池类(c3p0)
  *
  */
class MysqlPool extends Serializable {

  @transient lazy val log = LogManager.getLogger(this.getClass)

  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)

  // 1.初始化c3p0
  try {
    cpds.setJdbcUrl(ConfigurationManager.config.getString(Constants.JDBC_URL));
    cpds.setDriverClass("com.mysql.jdbc.Driver");
    cpds.setUser(ConfigurationManager.config.getString(Constants.JDBC_USER));
    cpds.setPassword(ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
    cpds.setInitialPoolSize(3)
    cpds.setMaxPoolSize(5)
    cpds.setMinPoolSize(2)
    cpds.setAcquireIncrement(5)
    cpds.setMaxStatements(180)
    /* 最大空闲时间,25000秒内未使用则连接被丢弃。若为0则永不丢弃。Default: 0 */
    cpds.setMaxIdleTime(25000)
    // 检测连接配置
    cpds.setPreferredTestQuery("select id from user_words limit 1")
    cpds.setIdleConnectionTestPeriod(18000)
  } catch {
    case e: Exception =>
      log.error("[MysqlPoolError]", e)
  }


  // 2.获取连接
   def getConnection: Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case e: Exception =>
        log.error("[MysqlPoolGetConnectionError]", e)
        null
    }
  }


  // 3.关闭连接
  private def closeAll(conn: Connection, statement: PreparedStatement, resultSet: ResultSet) {
    try {
      if (resultSet != null) {
        resultSet.close();
      }
      if (statement != null) {
        statement.close();
      }
      if (conn != null) {
        conn.close();
      }
    } catch {
      case e: SQLException => e.printStackTrace
    }

  }


  /**
    * 执行增删改SQL语句
    *
    * @param sql
    * @param params
    * @return 影响的行数
    */
  def executeUpdate(sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    val conn = getConnection

    try {
      getConnection.setAutoCommit(false)
      pstmt = conn.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }finally {
      closeAll(conn,pstmt,null)
    }
    rtn
  }

  /**
    * 执行查询SQL语句
    *
    * @param sql
    * @param params
    */
  def executeQuery(sql: String, params: Array[Any], queryCallback: QueryCallback) {
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null
    val conn = getConnection
    try {
      pstmt = conn.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }

      rs = pstmt.executeQuery()
      queryCallback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace
    }finally {
      closeAll(conn,pstmt,rs)
    }
  }

  /**
    * 批量执行SQL语句
    *
    * @param sql
    * @param paramsList
    * @return 每条SQL语句影响的行数
    */
  def executeBatch(sql: String, paramsList: Array[Array[Any]]): Array[Int] = {
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null
    val conn = getConnection
    try {
      // 第一步：使用Connection对象，取消自动提交
      conn.setAutoCommit(false)
      pstmt = conn.prepareStatement(sql)

      // 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
      if (paramsList != null && paramsList.length > 0) {
        for (params <- paramsList) {
          for (i <- 0 until params.length) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }
      }

      // 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
      rtn = pstmt.executeBatch()

      // 最后一步：使用Connection对象，提交批量的SQL语句
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }finally {
      closeAll(conn,pstmt,null)
    }
    rtn
  }




}


object MysqlManager {

  var mysqlManager: MysqlPool = _
  def getMysqlManager: MysqlPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }
}




