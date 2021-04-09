



object TheSingleOne {
  def main(args: Array[String]): Unit = {
    val list:Array[Int] = Array(1,2,3,1,2,3,4)
    println(find(list))
  }


  def find(nums:Array[Int])={
    nums.reduce(_^_)
  }
}
