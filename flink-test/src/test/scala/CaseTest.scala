/**
  * @ObjectName CaseTest
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/12 8:33
  * @Version 1.0
  */
object CaseTest {
  def main(args: Array[String]): Unit = {
    val q = new CaseTestQ
    val q1 = new CaseTestQ
    println(q.hashCode())
    println(q1.hashCode())
  }

}

class CaseTestQ()
