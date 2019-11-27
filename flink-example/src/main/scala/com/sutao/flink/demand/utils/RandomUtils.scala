package com.sutao.flink.demand.utils

import scala.util.Random

// 随机数生成工具类
class RandomUtils{

}

object RandomUtils {

  // 生成随机数并进行递增排序
  def randomList(n:Int,ranDom:Int): List[Int] ={
    var resultList:List[Int]=Nil
    while(resultList.length<n){
      val randomNum=(new Random).nextInt(ranDom)
      resultList=resultList:::List(randomNum)
    }
    resultList
  }

}
