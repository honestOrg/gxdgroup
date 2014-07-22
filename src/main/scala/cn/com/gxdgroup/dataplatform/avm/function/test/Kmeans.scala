package cn.com.gxdgroup.dataplatform.avm.function.test
import java.util.Random
import org.apache.spark.util.Vector
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
/**
 * Created by ThinkPad on 14-7-9.
 */
object Kmeans {
  val N = 1000
  val R = 1000 // Scaling factor
  val D = 10
  val K = 10
  val convergeDist = 0.001
  val rand = new Random(42)
  def generateData = {
    def generatePoint(i: Int) = {
      Vector(D, _ => rand.nextDouble * R)
    }
    //返回的为1000个10维的Vector
    Array.tabulate(N)(generatePoint)
  }
  //求出这个向量到底应该属于哪个中心，返回值是Int索引
  def closestPoint(p: Vector, centers: HashMap[Int, Vector]): Int = {
    var index = 0
    var bestIndex = 0
    //指定closest为无穷大值
    var closest = Double.PositiveInfinity
    for (i <- 1 to centers.size) {
      val vCurr = centers.get(i).get
      val tempDist = p.squaredDist(vCurr)
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }
  def main(args: Array[String]) {
    val data = generateData
    var points = new HashSet[Vector]
    var kPoints = new HashMap[Int, Vector]
    var tempDist = 1.0
    //先初始化随机构造K个K-means的中心点，放在points中
    while (points.size < K) {
      points.add(data(rand.nextInt(N)))
    }
    val iter = points.iterator
    for (i <- 1 to points.size) {
      kPoints.put(i, iter.next())
    }
    println("Initial centers: " + kPoints)
    while(tempDist > convergeDist) {
      //遍历data中的数据，将每个数据划分到，到底属于哪个类
      var closest = data.map (p => (closestPoint(p, kPoints), (p, 1)))
      var mappings = closest.groupBy[Int] (x => x._1)
      var pointStats = mappings.map(pair => pair._2.reduceLeft [(Int, (Vector, Int))] {case ((id1, (x1, y1)), (id2, (x2, y2))) => (id1, (x1 + x2, y1+y2))})
      var newPoints = pointStats.map {mapping => (mapping._1, mapping._2._1/mapping._2._2)}
      tempDist = 0.0
      for (mapping <- newPoints) {
        tempDist += kPoints.get(mapping._1).get.squaredDist(mapping._2)
      }
      for (newP <- newPoints) {
        kPoints.put(newP._1, newP._2)
      }
    }
    println("Final centers: " + kPoints)
  }
}
