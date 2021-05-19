package com.zx.bean

/**
  * @ClassName UserBehavior
  * @Description TODO
  * @Author Xing
  * 13 19:58
  * @Version 1.0
  */
case class UserBehavior(userId:Long,
                        itemId:Long,
                        categoryId:Int,
                        behavior:String,
                        timestamp:Long) {
}
