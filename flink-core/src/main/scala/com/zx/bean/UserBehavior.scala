package com.zx.bean

/**
  * @ClassName UserBehavior
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
case class UserBehavior(userId:Long,
                        itemId:Long,
                        categoryId:Int,
                        behavior:String,
                        timestamp:Long) {
}
