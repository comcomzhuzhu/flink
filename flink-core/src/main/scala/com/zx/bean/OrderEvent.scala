package com.zx.bean

/**
  * @ClassName OrderEvent
  * @Description TODO
  * @Author Xing
  * @Date 13 20:57
  * @Version 1.0
  */
case class OrderEvent(orderId:Long,
                      eventType:String,
                      txId:String,
                      eventTime:Long) {
}
