package com.zx.bean

/**
  * @ClassName OrderEvent
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
case class OrderEvent(orderId:Long,
                      eventType:String,
                      txId:String,
                      eventTime:Long) {
}
