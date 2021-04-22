package com.atguigu.bean

/**
  * @ClassName UserBehavior
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/13 19:58
  * @Version 1.0
  */
case class UserBehavior(userId:Long,
                        itemId:Long,
                        categoryId:Int,
                        behavior:String,
                        timestamp:Long) {
}
