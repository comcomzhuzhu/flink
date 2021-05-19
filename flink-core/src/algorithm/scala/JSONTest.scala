/**
  * @ObjectName JSONTest
  * @Description TODO
  * @Author Xing
  * 9 20:17
  * @Version 1.0
  */
import com.alibaba.fastjson.JSON

object JSONTest {
  def main(args: Array[String]): Unit = {
    val s = "{xxx}"
    val user: User = JSON.parseObject(s,classOf[User])
  }

}
case class User(name:String)
