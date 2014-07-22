package cn.com.gxdgroup.dataplatform.avm.function.test


import java.util.Random

import breeze.linalg.{Vector, DenseVector}
/**
* Created by ThinkPad on 14-6-30.
*/

//参考网址http://spark.apache.org/docs/latest/mllib-linear-methods.html#linear-least-squares-lasso-and-ridge-regression
object LocalLR {
// val N = 10000  // Number of data points
  val N=10
  val D = 10   // Number of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 5
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def generateData = {
    def generatePoint(i: Int) = {
      val y = if(i % 2 == 0) -1 else 1
      val x = DenseVector.fill(D){rand.nextGaussian + y * R}
      DataPoint(x, y)
    }
    Array.tabulate(N)(generatePoint)
  }


  def main(args: Array[String]) {
    val data = generateData
println("data:"+data)
    // Initialize w to a random value
    var w = DenseVector.fill(D){2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      var gradient = DenseVector.zeros[Double](D)
      println("gradient"+i +":"+gradient)
      for (p <- data) {
        println("p@@@@@@@@@:"+p)
        val scale = (1 / (1 + math.exp(-p.y * (w.dot(p.x)))) - 1) * p.y  //为什么不是NG视频中的梯度，因为他的取值范围是{-1，1}而NG的取值范围是{0,1}
        println("scala#########:"+scale)
//因为每一个维度都要求梯度（根据NG的视频），由于我们这里的是向量所以向量的每一个元素就是一个权重对应的梯度，所以直接向量
        gradient +=  p.x * scale
      }
      w -= gradient
      println("middle:"+i+":"+w)
    }

    println("Final w: " + w)

println("%%%%%%%%%%"+ StrictMath.log(10))
    println("&&&&&&&&&&:"+StrictMath.sqrt(2 * StrictMath.log(10) / 2))
  }


}
