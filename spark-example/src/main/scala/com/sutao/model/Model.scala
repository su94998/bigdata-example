package com.sutao.model

/**
  * Created by sutao on 2019/10/12.
  */

/**
  *
  * @param timestamp 时间戳
  * @param area      大区
  * @param city      城市
  * @param userid    用户
  * @param adid      广告
  */
case class AdsLog(
                   timestamp: Long,
                   area: String,
                   city: String,
                   userid: String,
                   adid: String)


case class DateAreaCityAdsToCount(
                                   dt: String,
                                   area: String,
                                   city: String,
                                   adid: String,
                                   click_count: Long
                                 )

case class DateAreaTOP3(
                         dt: String,
                         area: String,
                         adid: String,
                         click_count: Long,
                         rank: Int
                       )


/**
  *
  * 城市信息表
  *
  * @param city_id   城市id
  * @param city_name 城市名称
  * @param area      城市所在大区
  */
case class CityInfo(city_id: Long,
                    city_name: String,
                    area: String
                   )

/**
  * 产品表
  *
  * @param product_id   商品的ID
  * @param product_name 商品的名称
  * @param extend_info  商品额外的信息
  */
case class ProductInfo(product_id: Long,
                       product_name: String,
                       extend_info: String
                      )


/**
  * 用户信息表
  *
  * @param user_id      用户的ID
  * @param username     用户的名称
  * @param name         用户的名字
  * @param age          用户的年龄
  * @param professional 用户的职业
  * @param gender       用户的性别
  */
case class UserInfo(user_id: Long,
                    username: String,
                    name: String,
                    age: Int,
                    professional: String,
                    gender: String
                   )


/**
  * 用户访问动作表
  *
  * @param date               用户点击行为的日期
  * @param user_id            用户的ID
  * @param session_id         Session的ID
  * @param page_id            某个页面的ID
  * @param action_time        点击行为的时间点
  * @param search_keyword     用户搜索的关键词
  * @param click_category_id  某一个商品品类的ID
  * @param click_product_id   某一个商品的ID
  * @param order_category_ids 一次订单中所有品类的ID集合
  * @param order_product_ids  一次订单中所有商品的ID集合
  * @param pay_category_ids   一次支付中所有品类的ID集合
  * @param pay_product_ids    一次支付中所有商品的ID集合
  */
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long)


case class UserInfo1(
                      name: String,
                      age: String,
                      record_time: String
                    )

case class Student(
                    id: Long,
                    name: String,
                    age: Integer
                  )

case class WordCount(
                      word: String,
                      count: Long
                    )



