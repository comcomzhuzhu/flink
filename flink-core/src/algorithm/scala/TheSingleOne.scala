/**
  * @ObjectName TheSingleOne
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
object TheSingleOne {
  def main(args: Array[String]): Unit = {
    val list:Array[Int] = Array(1,2,3,1,2,3,4)
    println(find(list))
  }

  def find(nums:Array[Int]): Unit ={
    nums.reduce(_^_)
  }

}
